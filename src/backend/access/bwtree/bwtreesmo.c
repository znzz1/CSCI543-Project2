/*-------------------------------------------------------------------------
 *
 * bwtreesmo.c
 *    Structure Modification Operations for the Bw-tree.
 *
 *    Split uses two-phase delta records:
 *      Phase 1: SPLIT delta on the overflowing leaf
 *      Phase 2: SEPARATOR delta on the parent
 *
 *    This is a simplified initial implementation.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufmgr.h"

/*
 * Split a leaf page that has become too full.
 *
 * Steps:
 *   1. Read the leaf base page (with deltas consolidated).
 *   2. Create a new sibling page with the upper half of items.
 *   3. Allocate a PID for the sibling and add to mapping table.
 *   4. Install a SPLIT delta on the original leaf.
 *   5. (TODO) Install a SEPARATOR delta on the parent.
 */
void
_bwt_split(Relation rel, BWTreeMetaPageData *metad,
		   Buffer leafbuf, BWTreePid leaf_pid)
{
	Page		leafpage;
	OffsetNumber maxoff;
	OffsetNumber midoff;
	OffsetNumber off;
	Buffer		newbuf;
	Page		newpage;
	BWTreePageOpaque leaf_opaque;
	BWTreePageOpaque new_opaque;
	BWTreePid	new_pid;
	IndexTuple	mid_itup;

	leafpage = BufferGetPage(leafbuf);
	maxoff = PageGetMaxOffsetNumber(leafpage);

	if (maxoff < 2)
		return;

	midoff = maxoff / 2 + 1;

	/* allocate new sibling page */
	newbuf = _bwt_allocbuf(rel);
	newpage = BufferGetPage(newbuf);
	_bwt_initpage(newpage, BWT_LEAF, InvalidBWTreePid, 0);

	/* copy upper half to new page */
	for (off = midoff; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(leafpage, off);
		IndexTuple	itup;

		if (!ItemIdIsUsed(iid))
			continue;

		itup = (IndexTuple) PageGetItem(leafpage, iid);
		if (PageAddItem(newpage, (Item) itup, IndexTupleSize(itup),
						InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "bwtree: split failed to add item to new page");
	}

	/* set up sibling links */
	leaf_opaque = BWTreePageGetOpaque(leafpage);
	new_opaque = BWTreePageGetOpaque(newpage);

	new_opaque->bwto_prev = BufferGetBlockNumber(leafbuf);
	new_opaque->bwto_next = leaf_opaque->bwto_next;

	MarkBufferDirty(newbuf);

	/* register sibling in mapping table */
	new_pid = _bwt_map_alloc_pid(rel, metad,
								 BufferGetBlockNumber(newbuf),
								 InvalidBlockNumber);
	new_opaque->bwto_pid = new_pid;
	MarkBufferDirty(newbuf);

	/* install SPLIT delta on original leaf */
	mid_itup = (IndexTuple) PageGetItem(leafpage,
										PageGetItemId(leafpage, midoff));
	_bwt_delta_install(rel, metad, leaf_pid,
					   BW_DELTA_SPLIT, mid_itup, new_pid);

	/* update original leaf's right link */
	leaf_opaque->bwto_next = BufferGetBlockNumber(newbuf);
	MarkBufferDirty(leafbuf);

	_bwt_relbuf(rel, newbuf);
}
