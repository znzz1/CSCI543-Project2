/*-------------------------------------------------------------------------
 *
 * bwtreeconsolidate.c
 *    Delta-chain consolidation for the Bw-tree.
 *
 *    When a leaf page's delta chain exceeds BWTREE_DELTA_CHAIN_THRESHOLD,
 *    we consolidate: read the base page + all deltas, produce a new base
 *    page with all changes applied, update the mapping table, and free
 *    the old delta pages.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufmgr.h"

/*
 * Count the number of delta pages in a chain.
 */
static int
_bwt_count_delta_pages(Relation rel, BlockNumber delta_blkno)
{
	int		count = 0;

	while (delta_blkno != InvalidBlockNumber)
	{
		Buffer		buf;
		BWTreePageOpaque opaque;

		buf = _bwt_getbuf(rel, delta_blkno, BWT_READ);
		opaque = BWTreePageGetOpaque(BufferGetPage(buf));
		delta_blkno = opaque->bwto_next;
		_bwt_relbuf(rel, buf);
		count++;
	}

	return count;
}

/*
 * Should we consolidate the delta chain for this PID?
 */
bool
_bwt_should_consolidate(Relation rel, BWTreeMetaPageData *metad,
						BWTreePid pid)
{
	BlockNumber base_blkno;
	BlockNumber delta_blkno;

	if (!_bwt_map_lookup(rel, metad, pid, &base_blkno, &delta_blkno))
		return false;

	if (delta_blkno == InvalidBlockNumber)
		return false;

	return _bwt_count_delta_pages(rel, delta_blkno) >=
		   BWTREE_DELTA_CHAIN_THRESHOLD;
}

/*
 * Consolidate: merge all delta records into a fresh base page.
 *
 *   1. Read old base page → copy
 *   2. Apply all delta records to the copy
 *   3. Write a new base page with the merged content
 *   4. Update mapping table: new base_blkno, delta_blkno = Invalid
 */
void
_bwt_consolidate(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid)
{
	BlockNumber old_base_blkno;
	BlockNumber delta_blkno;
	Buffer		old_basebuf;
	Page		merge_page;
	OffsetNumber maxoff;
	Buffer		new_basebuf;
	Page		new_page;
	BWTreePageOpaque old_opaque;
	BWTreePageOpaque new_opaque;
	OffsetNumber off;

	if (!_bwt_map_lookup(rel, metad, pid, &old_base_blkno, &delta_blkno))
		return;

	if (delta_blkno == InvalidBlockNumber)
		return;

	/* make a working copy of the old base page */
	old_basebuf = _bwt_getbuf(rel, old_base_blkno, BWT_READ);
	merge_page = (Page) palloc(BLCKSZ);
	memcpy(merge_page, BufferGetPage(old_basebuf), BLCKSZ);
	old_opaque = BWTreePageGetOpaque(BufferGetPage(old_basebuf));
	_bwt_relbuf(rel, old_basebuf);

	/* apply deltas */
	_bwt_delta_apply(rel, delta_blkno, merge_page, &maxoff);

	/* allocate a new base page */
	new_basebuf = _bwt_allocbuf(rel);
	new_page = BufferGetPage(new_basebuf);

	_bwt_initpage(new_page, old_opaque->bwto_flags & ~BWT_DELTA,
				  pid, old_opaque->bwto_level);

	new_opaque = BWTreePageGetOpaque(new_page);
	new_opaque->bwto_prev = old_opaque->bwto_prev;
	new_opaque->bwto_next = old_opaque->bwto_next;

	/* copy merged items to the new page */
	maxoff = PageGetMaxOffsetNumber(merge_page);
	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(merge_page, off);
		IndexTuple	itup;

		if (!ItemIdIsUsed(iid))
			continue;

		itup = (IndexTuple) PageGetItem(merge_page, iid);

		if (PageAddItem(new_page, (Item) itup, IndexTupleSize(itup),
						InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "bwtree: consolidation failed to add item");
	}

	MarkBufferDirty(new_basebuf);

	/* update mapping: new base, no deltas */
	_bwt_map_update(rel, metad, pid,
					BufferGetBlockNumber(new_basebuf),
					InvalidBlockNumber);

	_bwt_relbuf(rel, new_basebuf);
	pfree(merge_page);
}
