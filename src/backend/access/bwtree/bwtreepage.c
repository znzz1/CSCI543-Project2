/*-------------------------------------------------------------------------
 *
 * bwtreepage.c
 *    Buffer manager wrappers and page initialization for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"

/*
 * Read an existing page and optionally lock it.
 */
Buffer
_bwt_getbuf(Relation rel, BlockNumber blkno, int access)
{
	Buffer buf;

	buf = ReadBuffer(rel, blkno);

	if (access != BWT_NOLOCK)
		LockBuffer(buf, access);

	return buf;
}

/*
 * Unlock and release (unpin) a buffer.
 */
void
_bwt_relbuf(Relation rel, Buffer buf)
{
	UnlockReleaseBuffer(buf);
}

/*
 * Extend the relation by one page.  Returns the new buffer
 * with an exclusive lock held.
 */
Buffer
_bwt_allocbuf(Relation rel)
{
	Buffer		buf;
	bool		needLock;

	needLock = !RELATION_IS_LOCAL(rel);

	if (needLock)
		LockRelationForExtension(rel, ExclusiveLock);

	buf = ReadBuffer(rel, P_NEW);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	if (needLock)
		UnlockRelationForExtension(rel, ExclusiveLock);

	return buf;
}

/*
 * Initialize a base page (leaf or internal) or delta page.
 *
 * Caller must hold exclusive lock on the buffer.
 */
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

/*
 * Initialize the meta page (block 0).
 *
 * The meta page does not use special space; its payload lives in the
 * content area obtained via PageGetContents().
 */
void
_bwt_initmetapage(Page page, BWTreePid root_pid, uint32 level)
{
	BWTreeMetaPageData *metad;

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
