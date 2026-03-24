/*-------------------------------------------------------------------------
 *
 * bwtreescan.c
 *    Scan (SELECT) for the Bw-tree index.
 *
 *    The scan reads a leaf page and its delta chain, builds a merged
 *    list of heap TIDs, and returns them one at a time.  When the
 *    current leaf is exhausted, follow the right-sibling link.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/relscan.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"

static bool _bwt_load_leaf(IndexScanDesc scan, BWTreePid leaf_pid);
static BWTreePid _bwt_next_leaf_pid(Relation rel, BWTreeMetaPageData *metad,
									BWTreePid cur_pid);

IndexScanDesc
bwtreebeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	BWTreeScanOpaque so;

	scan = RelationGetIndexScan(rel, nkeys, norderbys);

	so = (BWTreeScanOpaque) palloc0(sizeof(BWTreeScanOpaqueData));
	so->started = false;
	so->cur_leaf_pid = InvalidBWTreePid;
	so->cur_item = 0;
	so->num_items = 0;
	so->items = NULL;
	so->numberOfKeys = 0;
	so->keyData = NULL;
	scan->opaque = so;

	return scan;
}

void
bwtreerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			 ScanKey orderbys, int norderbys)
{
	BWTreeScanOpaque so = (BWTreeScanOpaque) scan->opaque;

	so->started = false;
	so->cur_leaf_pid = InvalidBWTreePid;
	so->cur_item = 0;
	so->num_items = 0;

	if (so->items)
	{
		pfree(so->items);
		so->items = NULL;
	}

	if (scankey && nscankeys > 0)
	{
		if (so->keyData == NULL)
			so->keyData = (ScanKey) palloc(sizeof(ScanKeyData) * nscankeys);
		memmove(so->keyData, scankey, sizeof(ScanKeyData) * nscankeys);
		so->numberOfKeys = nscankeys;
	}
}

void
bwtreeendscan(IndexScanDesc scan)
{
	BWTreeScanOpaque so = (BWTreeScanOpaque) scan->opaque;

	if (so->items)
		pfree(so->items);
	if (so->keyData)
		pfree(so->keyData);
	pfree(so);
	scan->opaque = NULL;
}

/*
 * Get the PID of the next leaf to the right of cur_pid.
 * Returns InvalidBWTreePid if there is no next leaf.
 */
static BWTreePid
_bwt_next_leaf_pid(Relation rel, BWTreeMetaPageData *metad, BWTreePid cur_pid)
{
	BlockNumber base_blkno, delta_blkno;
	Buffer		basebuf;
	Page		basepage;
	BWTreePageOpaque opaque;
	BlockNumber next_blkno;
	BWTreePid	next_pid;

	if (!_bwt_map_lookup(rel, metad, cur_pid, &base_blkno, &delta_blkno))
		return InvalidBWTreePid;

	basebuf = _bwt_getbuf(rel, base_blkno, BWT_READ);
	basepage = BufferGetPage(basebuf);
	opaque = BWTreePageGetOpaque(basepage);
	next_blkno = opaque->bwto_next;
	_bwt_relbuf(rel, basebuf);

	if (next_blkno == InvalidBlockNumber)
		return InvalidBWTreePid;

	basebuf = _bwt_getbuf(rel, next_blkno, BWT_READ);
	basepage = BufferGetPage(basebuf);
	opaque = BWTreePageGetOpaque(basepage);
	next_pid = opaque->bwto_pid;
	_bwt_relbuf(rel, basebuf);

	return next_pid;
}

/*
 * Check if an IndexTuple satisfies all scan keys.
 *
 * The scan keys from the executor have sk_func set to the OPERATOR
 * function (e.g. int4eq for =, int4lt for <), which returns a bool.
 */
static bool
_bwt_tuple_matches_keys(IndexScanDesc scan, IndexTuple itup)
{
	BWTreeScanOpaque so = (BWTreeScanOpaque) scan->opaque;
	TupleDesc	itupdesc = RelationGetDescr(scan->indexRelation);
	int			i;

	for (i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey skey = &so->keyData[i];
		Datum	datum;
		bool	isnull;
		bool	satisfies;

		datum = index_getattr(itup, skey->sk_attno, itupdesc, &isnull);

		if (isnull || (skey->sk_flags & SK_ISNULL))
			return false;

		satisfies = DatumGetBool(FunctionCall2Coll(&skey->sk_func,
												   skey->sk_collation,
												   datum,
												   skey->sk_argument));
		if (!satisfies)
			return false;
	}

	return true;
}

/*
 * Load the merged view of a leaf page + its delta chain into the
 * scan opaque's items array.  Returns true if any items were found.
 */
static bool
_bwt_load_leaf(IndexScanDesc scan, BWTreePid leaf_pid)
{
	BWTreeScanOpaque so = (BWTreeScanOpaque) scan->opaque;
	Relation	rel = scan->indexRelation;
	Buffer		metabuf;
	BWTreeMetaPageData *metad;
	BlockNumber base_blkno;
	BlockNumber delta_blkno;
	Buffer		basebuf;
	Page		merge_page;
	OffsetNumber maxoff;
	OffsetNumber off;
	int			count;

	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
	metad = BWTreeMetaPageGetData(BufferGetPage(metabuf));

	if (!_bwt_map_lookup(rel, metad, leaf_pid, &base_blkno, &delta_blkno))
	{
		_bwt_relbuf(rel, metabuf);
		return false;
	}

	basebuf = _bwt_getbuf(rel, base_blkno, BWT_READ);
	merge_page = (Page) palloc(BLCKSZ);
	memcpy(merge_page, BufferGetPage(basebuf), BLCKSZ);
	_bwt_relbuf(rel, basebuf);

	if (delta_blkno != InvalidBlockNumber)
		_bwt_delta_apply(rel, delta_blkno, merge_page, &maxoff);
	else
		maxoff = PageGetMaxOffsetNumber(merge_page);

	_bwt_relbuf(rel, metabuf);

	if (so->items)
	{
		pfree(so->items);
		so->items = NULL;
	}
	so->num_items = 0;
	so->cur_item = 0;

	if (maxoff < FirstOffsetNumber)
	{
		pfree(merge_page);
		return false;
	}

	so->items = (BWTreeScanPosItem *) palloc(sizeof(BWTreeScanPosItem) * maxoff);
	count = 0;

	for (off = FirstOffsetNumber; off <= maxoff; off++)
	{
		ItemId		iid = PageGetItemId(merge_page, off);
		IndexTuple	itup;

		if (!ItemIdIsUsed(iid))
			continue;

		itup = (IndexTuple) PageGetItem(merge_page, iid);

		if (so->numberOfKeys > 0 && !_bwt_tuple_matches_keys(scan, itup))
			continue;

		so->items[count].heapTid = itup->t_tid;
		count++;
	}

	so->num_items = count;
	so->cur_leaf_pid = leaf_pid;

	pfree(merge_page);
	return count > 0;
}

/*
 * bwtreegettuple -- AM callback, return next matching tuple.
 */
bool
bwtreegettuple(IndexScanDesc scan, ScanDirection dir)
{
	BWTreeScanOpaque so = (BWTreeScanOpaque) scan->opaque;
	Relation	rel = scan->indexRelation;

	scan->xs_recheck = false;

	if (!so->started)
	{
		Buffer			metabuf;
		BWTreeMetaPageData *metad;
		BWTreePid		leaf_pid;

		metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
		metad = BWTreeMetaPageGetData(BufferGetPage(metabuf));

		/* start from leftmost leaf for simplicity */
		leaf_pid = _bwt_search_leaf(rel, metad, NULL, 0);
		_bwt_relbuf(rel, metabuf);

		if (leaf_pid == InvalidBWTreePid)
			return false;

		so->cur_leaf_pid = leaf_pid;
		_bwt_load_leaf(scan, leaf_pid);
		so->started = true;
	}

	while (true)
	{
		if (so->cur_item < so->num_items)
		{
			scan->xs_heaptid = so->items[so->cur_item].heapTid;
			so->cur_item++;
			return true;
		}

		/* move to next leaf */
		{
			Buffer		metabuf;
			BWTreeMetaPageData *metad;
			BWTreePid	next_pid;

			metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
			metad = BWTreeMetaPageGetData(BufferGetPage(metabuf));

			next_pid = _bwt_next_leaf_pid(rel, metad, so->cur_leaf_pid);
			_bwt_relbuf(rel, metabuf);

			if (next_pid == InvalidBWTreePid)
				return false;

			so->cur_leaf_pid = next_pid;
			_bwt_load_leaf(scan, next_pid);

			/* continue the loop; _bwt_load_leaf may have found 0 items
			 * on this leaf but there could be matches on the next one */
		}
	}
}

int64
bwtreegetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	(void) scan;
	(void) tbm;
	return 0;
}
