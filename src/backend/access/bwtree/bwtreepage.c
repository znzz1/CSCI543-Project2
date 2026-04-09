#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/lmgr.h"

#define BWTREE_ALLOC_BATCH_SIZE			8
#define BWTREE_ALLOC_STASH_CAPACITY		128

typedef struct BWTreeAllocStashState
{
	bool			valid;
	Oid				relid;
	RelFileLocator	locator;
	int				nblocks;
	BlockNumber		blknos[BWTREE_ALLOC_STASH_CAPACITY];
} BWTreeAllocStashState;

static BWTreeAllocStashState bwtree_alloc_stash = {0};

static bool _bwt_alloc_stash_matches_rel(Relation rel);
static void _bwt_alloc_stash_reset_for_rel(Relation rel);

static bool
_bwt_alloc_stash_matches_rel(Relation rel)
{
	if (rel == NULL || !bwtree_alloc_stash.valid)
		return false;
	if (bwtree_alloc_stash.relid != RelationGetRelid(rel))
		return false;

	return (bwtree_alloc_stash.locator.spcOid == rel->rd_locator.spcOid &&
			bwtree_alloc_stash.locator.dbOid == rel->rd_locator.dbOid &&
			bwtree_alloc_stash.locator.relNumber == rel->rd_locator.relNumber);
}

static void
_bwt_alloc_stash_reset_for_rel(Relation rel)
{
	if (rel == NULL)
	{
		memset(&bwtree_alloc_stash, 0, sizeof(bwtree_alloc_stash));
		return;
	}

	bwtree_alloc_stash.valid = true;
	bwtree_alloc_stash.relid = RelationGetRelid(rel);
	bwtree_alloc_stash.locator = rel->rd_locator;
	bwtree_alloc_stash.nblocks = 0;
}

Buffer
_bwt_getbuf(Relation rel, BlockNumber blkno, int access)
{
	Buffer	buf;

	buf = ReadBuffer(rel, blkno);

	if (access != BWT_NOLOCK)
		LockBuffer(buf, access);

	return buf;
}

void
_bwt_relbuf(Relation rel, Buffer buf, int access)
{
	(void) rel;

	if (access == BWT_NOLOCK)
		ReleaseBuffer(buf);
	else
		UnlockReleaseBuffer(buf);
}

Buffer
_bwt_allocbuf(Relation rel)
{
	Buffer	buf;
	bool	need_lock;
	bool	use_stash_for_rel;
	int		i;

	use_stash_for_rel = _bwt_alloc_stash_matches_rel(rel);
	if (!use_stash_for_rel)
	{
		/*
		 * Keep correctness and bounded space behavior:
		 * if there are still reserved blocks for another relation, do not
		 * overwrite that stash slot. Fall back to one-by-one allocation for
		 * current relation until stash drains or relation switches back.
		 */
		if (!bwtree_alloc_stash.valid || bwtree_alloc_stash.nblocks == 0)
		{
			_bwt_alloc_stash_reset_for_rel(rel);
			use_stash_for_rel = true;
		}
	}

	if (use_stash_for_rel && bwtree_alloc_stash.nblocks > 0)
	{
		BlockNumber blkno;

		blkno = bwtree_alloc_stash.blknos[--bwtree_alloc_stash.nblocks];
		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
		return buf;
	}

	need_lock = !RELATION_IS_LOCAL(rel);

	if (need_lock)
		LockRelationForExtension(rel, ExclusiveLock);

	buf = ReadBuffer(rel, P_NEW);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	/*
	 * Reduce relation-extension lock contention by reserving a small batch
	 * only when this backend-local stash is currently tracking this relation.
	 */
	for (i = 0; use_stash_for_rel && i < (BWTREE_ALLOC_BATCH_SIZE - 1); i++)
	{
		Buffer		extrabuf;
		Page		extrapage;
		BWTreePageOpaque extraopaque;
		BlockNumber	extra_blkno;

		if (bwtree_alloc_stash.nblocks >= BWTREE_ALLOC_STASH_CAPACITY)
			break;

		extrabuf = ReadBuffer(rel, P_NEW);
		LockBuffer(extrabuf, BUFFER_LOCK_EXCLUSIVE);
		extrapage = BufferGetPage(extrabuf);
		PageInit(extrapage, BLCKSZ, sizeof(BWTreePageOpaqueData));
		extraopaque = BWTreePageGetOpaque(extrapage);
		extraopaque->bwto_prev = InvalidBlockNumber;
		extraopaque->bwto_next = InvalidBlockNumber;
		extraopaque->bwto_pid = InvalidBWTreePid;
		extraopaque->bwto_level = 0;
		extraopaque->bwto_flags = BWT_DELETED;
		extraopaque->bwto_page_id = BWTREE_PAGE_ID;
		MarkBufferDirty(extrabuf);
		extra_blkno = BufferGetBlockNumber(extrabuf);
		bwtree_alloc_stash.blknos[bwtree_alloc_stash.nblocks++] = extra_blkno;
		UnlockReleaseBuffer(extrabuf);
	}

	if (need_lock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	return buf;
}

void
_bwt_initpage(Page page, uint16 flags, BWTreePid pid, uint32 level)
{
	BWTreePageOpaque opaque;

	PageInit(page, BLCKSZ, sizeof(BWTreePageOpaqueData));

	opaque = BWTreePageGetOpaque(page);
	opaque->bwto_prev = InvalidBlockNumber;
	opaque->bwto_next = InvalidBlockNumber;
	opaque->bwto_pid = pid;
	opaque->bwto_level = level;
	opaque->bwto_flags = flags;
	opaque->bwto_page_id = BWTREE_PAGE_ID;
}

void
_bwt_initmetapage(Page page, BWTreePid root_pid, uint32 level)
{
	BWTreeMetaPageData *metad;

	if (sizeof(BWTreeMetaPageData) >
		(BLCKSZ - MAXALIGN(SizeOfPageHeaderData)))
		elog(ERROR, "bwtree: metapage layout exceeds page size");

	PageInit(page, BLCKSZ, 0);

	metad = BWTreeMetaPageGetData(page);
	metad->bwt_magic = BWTREE_MAGIC;
	metad->bwt_version = BWTREE_VERSION;
	metad->bwt_root_pid = root_pid;
	metad->bwt_level = level;
	metad->bwt_next_pid = 0;
	metad->bwt_num_tuples = 0;
	metad->bwt_num_map_pages = 0;
	memset(metad->bwt_map_blknos, 0, sizeof(metad->bwt_map_blknos));

	((PageHeader) page)->pd_lower =
		((char *) metad + sizeof(BWTreeMetaPageData)) - (char *) page;
}
