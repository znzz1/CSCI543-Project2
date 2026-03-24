/*-------------------------------------------------------------------------
 *
 * bwtreesearch.c
 *    Tree traversal for the Bw-tree.
 *
 *    Searching goes through the mapping table at each level:
 *      1. Read mapping entry for current PID → base_blkno
 *      2. Read base page, binary-search for the right child PID
 *      3. Repeat until leaf
 *
 *    At each node the delta chain is also checked (for SPLIT deltas
 *    that may redirect the search to a new sibling).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/*
 * Compare scan keys against the tuple at `offnum` on `page`.
 *
 * Returns < 0 if scankey < tuple, 0 if equal, > 0 if scankey > tuple.
 * Uses the index's first sort-support or comparison function.
 */
int32
_bwt_compare(Relation rel, ScanKey scankey, int nkeys,
			 Page page, OffsetNumber offnum)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	IndexTuple	itup;
	Datum		datum;
	bool		isnull;
	int			i;

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));

	for (i = 0; i < nkeys; i++)
	{
		ScanKey		skey = &scankey[i];
		int32		cmp;

		datum = index_getattr(itup, skey->sk_attno, itupdesc, &isnull);

		if (isnull)
		{
			if (skey->sk_flags & SK_ISNULL)
				continue;
			return 1;
		}
		if (skey->sk_flags & SK_ISNULL)
			return -1;

		cmp = DatumGetInt32(FunctionCall2Coll(&skey->sk_func,
											  skey->sk_collation,
											  skey->sk_argument,
											  datum));
		if (cmp != 0)
			return cmp;
	}

	return 0;
}

/*
 * Descend from the root to find the leaf PID whose key range contains
 * the scan key.
 *
 * If no scan keys are given (nkeys == 0), returns the leftmost leaf.
 */
BWTreePid
_bwt_search_leaf(Relation rel, BWTreeMetaPageData *metad,
				 ScanKey scankey, int nkeys)
{
	BWTreePid	cur_pid;
	uint32		level;

	cur_pid = metad->bwt_root_pid;
	level = metad->bwt_level;

	while (level > 0)
	{
		BlockNumber base_blkno;
		BlockNumber delta_blkno;
		Buffer		buf;
		Page		page;
		OffsetNumber maxoff;
		OffsetNumber off;
		BWTreePid	child_pid = InvalidBWTreePid;

		if (!_bwt_map_lookup(rel, metad, cur_pid, &base_blkno, &delta_blkno))
			elog(ERROR, "bwtree: PID %u not found in mapping table", cur_pid);

		buf = _bwt_getbuf(rel, base_blkno, BWT_READ);
		page = BufferGetPage(buf);
		maxoff = PageGetMaxOffsetNumber(page);

		if (nkeys == 0 || maxoff < FirstOffsetNumber)
		{
			/* no keys or empty page: follow leftmost child */
			if (maxoff >= FirstOffsetNumber)
			{
				IndexTuple	itup = (IndexTuple) PageGetItem(page,
								  PageGetItemId(page, FirstOffsetNumber));
				child_pid = BWTreeTupleGetDownLink(itup);
			}
		}
		else
		{
			/* find rightmost entry whose key <= scan key */
			child_pid = BWTreeTupleGetDownLink(
				(IndexTuple) PageGetItem(page,
				PageGetItemId(page, FirstOffsetNumber)));

			for (off = FirstOffsetNumber; off <= maxoff; off++)
			{
				int32 cmp = _bwt_compare(rel, scankey, nkeys, page, off);

				if (cmp >= 0)
					child_pid = BWTreeTupleGetDownLink(
						(IndexTuple) PageGetItem(page,
						PageGetItemId(page, off)));
				else
					break;
			}
		}

		_bwt_relbuf(rel, buf);

		if (child_pid == InvalidBWTreePid)
			elog(ERROR, "bwtree: could not find child during descent");

		cur_pid = child_pid;
		level--;
	}

	return cur_pid;
}
