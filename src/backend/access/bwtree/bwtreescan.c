#include "postgres.h"

#include "access/bwtree.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "miscadmin.h"
#include "nodes/tidbitmap.h"

/*
 * Safety bound for leaf-link walks, to avoid infinite loops if sibling
 * pointers become cyclic due to corruption.
 */
#define BWTREE_MAX_SCAN_LEAF_STEPS	1048576

static void _bwt_scan_reset_items(BWTreeScanOpaque so);
static void _bwt_scan_set_keys(BWTreeScanOpaque so, ScanKey scankey, int nscankeys);
static bool _bwt_tuple_matches_key(Relation rel, IndexTuple itup, ScanKey key);
static bool _bwt_tuple_matches_all_keys(Relation rel, IndexTuple itup,
										ScanKey scankey, int nkeys);
static BWTreePid _bwt_pid_from_blkno(Relation rel, BlockNumber blkno);
static void _bwt_read_metad_snapshot(Relation rel, BWTreeMetaPageData *snapshot);
static void _bwt_validate_scan_leaf(const BWTMaterializedPage *mpage,
									BlockNumber expected_prev_blkno);
static bool _bwt_pick_scan_bounds(Relation rel, BWTreeScanOpaque so,
								  ScanKeyData *lower_out, bool *has_lower_out,
								  ScanKeyData *upper_out, bool *has_upper_out);
static ScanKey _bwt_build_route_key(Relation rel, const ScanKeyData *bound_key);
static int32 _bwt_compare_first_key_with_bound(Relation rel, IndexTuple itup,
												const ScanKeyData *bound_key);
static bool _bwt_lower_bound_allows_leaf(Relation rel, const BWTMaterializedPage *mpage,
										 const ScanKeyData *lower_bound);
static bool _bwt_stop_before_leaf(Relation rel, const BWTMaterializedPage *mpage,
								  const ScanKeyData *upper_bound);
static bool _bwt_stop_after_leaf(Relation rel, const BWTMaterializedPage *mpage,
								 const ScanKeyData *upper_bound);
static BWTreePid _bwt_adjust_scan_start_left(Relation rel, BWTreePid start_pid,
											 const ScanKeyData *lower_bound);
static bool _bwt_scan_next_match(IndexScanDesc scan, BWTreeScanOpaque so,
								 ItemPointerData *tid_out);

IndexScanDesc
bwtreebeginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc	scan;
	BWTreeScanOpaque so;

	if (norderbys != 0)
		elog(ERROR, "bwtree: ORDER BY scan is not supported yet");

	scan = RelationGetIndexScan(rel, nkeys, norderbys);
	so = (BWTreeScanOpaque) palloc0(sizeof(BWTreeScanOpaqueData));
	so->started = false;
	so->cur_leaf_pid = InvalidBWTreePid;
	so->leaf_walk_steps = 0;
	so->has_lower_bound = false;
	so->has_upper_bound = false;
	so->cur_item = 0;
	so->num_items = 0;
	so->items = NULL;
	so->numberOfKeys = 0;
	so->keyDataCapacity = 0;
	so->keyData = NULL;

	if (nkeys > 0)
	{
		so->keyData = (ScanKey) palloc0(sizeof(ScanKeyData) * nkeys);
		so->keyDataCapacity = nkeys;
	}

	scan->opaque = so;
	return scan;
}

void
bwtreerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			 ScanKey orderbys, int norderbys)
{
	BWTreeScanOpaque so;

	if (scan == NULL || scan->opaque == NULL)
		elog(ERROR, "bwtree: rescan requires a valid scan descriptor");
	if (norderbys != 0)
		elog(ERROR, "bwtree: ORDER BY rescan is not supported yet");
	(void) orderbys;
	if (nscankeys < 0)
		elog(ERROR, "bwtree: invalid number of scan keys %d", nscankeys);

	so = (BWTreeScanOpaque) scan->opaque;
	if (scankey != NULL)
		_bwt_scan_set_keys(so, scankey, nscankeys);

	_bwt_scan_reset_items(so);
	so->started = false;
	so->cur_leaf_pid = InvalidBWTreePid;
	so->leaf_walk_steps = 0;
	so->has_lower_bound = false;
	so->has_upper_bound = false;
	so->cur_item = 0;

	ItemPointerSetInvalid(&scan->xs_heaptid);
	scan->xs_recheck = false;
}

bool
bwtreegettuple(IndexScanDesc scan, ScanDirection dir)
{
	BWTreeScanOpaque so;
	bool				found;
	ItemPointerData	tid;

	_bwt_epoch_enter();
	PG_TRY();
	{
		if (scan == NULL || scan->opaque == NULL)
			elog(ERROR, "bwtree: gettuple requires a valid scan descriptor");
		if (dir != ForwardScanDirection)
			elog(ERROR, "bwtree: backward scan is not supported yet");

		so = (BWTreeScanOpaque) scan->opaque;
		found = _bwt_scan_next_match(scan, so, &tid);
		if (!found)
		{
			ItemPointerSetInvalid(&scan->xs_heaptid);
			scan->xs_recheck = false;
		}
		else
		{
			scan->xs_heaptid = tid;
			scan->xs_recheck = false;
		}
	}
	PG_CATCH();
	{
		_bwt_epoch_exit();
		PG_RE_THROW();
	}
	PG_END_TRY();

	_bwt_epoch_exit();
	return found;
}

int64
bwtreegetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	BWTreeScanOpaque so;
	int64			ntuples;
	ItemPointerData	tid;

	ntuples = 0;
	_bwt_epoch_enter();
	PG_TRY();
	{
		if (scan == NULL || scan->opaque == NULL)
			elog(ERROR, "bwtree: getbitmap requires a valid scan descriptor");
		if (tbm == NULL)
			elog(ERROR, "bwtree: getbitmap requires a valid tidbitmap");

		so = (BWTreeScanOpaque) scan->opaque;

		/*
		 * Start from a clean scan position so bitmap build never depends on
		 * prior gettuple consumption state.
		 */
		_bwt_scan_reset_items(so);
		so->started = false;
		so->cur_leaf_pid = InvalidBWTreePid;
		so->leaf_walk_steps = 0;
		so->has_lower_bound = false;
		so->has_upper_bound = false;
		so->cur_item = 0;

		while (_bwt_scan_next_match(scan, so, &tid))
		{
			tbm_add_tuples(tbm, &tid, 1, false);
			ntuples++;
		}

		_bwt_scan_reset_items(so);
		so->started = true;
		so->cur_leaf_pid = InvalidBWTreePid;
		so->leaf_walk_steps = 0;
		so->has_lower_bound = false;
		so->has_upper_bound = false;
		so->cur_item = 0;

		scan->xs_recheck = false;
		ItemPointerSetInvalid(&scan->xs_heaptid);
	}
	PG_CATCH();
	{
		_bwt_epoch_exit();
		PG_RE_THROW();
	}
	PG_END_TRY();

	_bwt_epoch_exit();
	return ntuples;
}

void
bwtreeendscan(IndexScanDesc scan)
{
	BWTreeScanOpaque so;

	if (scan == NULL || scan->opaque == NULL)
		return;

	so = (BWTreeScanOpaque) scan->opaque;
	_bwt_scan_reset_items(so);
	if (so->keyData != NULL)
		pfree(so->keyData);
	pfree(so);
	scan->opaque = NULL;
}

static void
_bwt_scan_reset_items(BWTreeScanOpaque so)
{
	if (so == NULL)
		return;

	if (so->items != NULL)
		pfree(so->items);
	so->items = NULL;
	so->num_items = 0;
	so->cur_item = 0;
}

static void
_bwt_scan_set_keys(BWTreeScanOpaque so, ScanKey scankey, int nscankeys)
{
	if (so == NULL)
		elog(ERROR, "bwtree: scan-set-keys requires scan opaque");
	if (nscankeys < 0)
		elog(ERROR, "bwtree: scan-set-keys got invalid nscankeys %d", nscankeys);

	if (nscankeys > so->keyDataCapacity)
	{
		if (so->keyData == NULL)
			so->keyData = (ScanKey) palloc(sizeof(ScanKeyData) * nscankeys);
		else
			so->keyData = (ScanKey) repalloc(so->keyData,
											 sizeof(ScanKeyData) * nscankeys);
		so->keyDataCapacity = nscankeys;
	}

	if (nscankeys > 0)
		memcpy(so->keyData, scankey, sizeof(ScanKeyData) * nscankeys);
	so->numberOfKeys = nscankeys;
}

static bool
_bwt_tuple_matches_key(Relation rel, IndexTuple itup, ScanKey key)
{
	TupleDesc	itupdesc;
	Datum		datum;
	bool		isNull;
	bool		matched;

	if (rel == NULL || itup == NULL || key == NULL)
		elog(ERROR, "bwtree: tuple-match-key requires valid inputs");

	if (key->sk_flags & (SK_ROW_HEADER | SK_ROW_MEMBER))
		elog(ERROR, "bwtree: row-comparison scankey is not supported yet");
	if (key->sk_flags & SK_SEARCHARRAY)
		elog(ERROR, "bwtree: array scankey is not supported yet");
	if (key->sk_attno <= 0)
		elog(ERROR, "bwtree: invalid scankey attno %d", key->sk_attno);

	itupdesc = RelationGetDescr(rel);
	if (key->sk_attno > itupdesc->natts)
		elog(ERROR, "bwtree: scankey attno %d exceeds index natts %d",
			 key->sk_attno, itupdesc->natts);

	datum = index_getattr(itup, key->sk_attno, itupdesc, &isNull);

	if (key->sk_flags & SK_SEARCHNULL)
		return isNull;
	if (key->sk_flags & SK_SEARCHNOTNULL)
		return !isNull;
	if (key->sk_flags & SK_ISNULL)
		return false;

	if (isNull)
		return false;

	/*
	 * For ordinary scan keys, sk_func is a boolean operator predicate
	 * (e.g. =, <, <=). We evaluate it directly against tuple datum.
	 */
	matched = DatumGetBool(FunctionCall2Coll(&key->sk_func,
											 key->sk_collation,
											 datum,
											 key->sk_argument));
#ifdef SK_NEGATE
	if (key->sk_flags & SK_NEGATE)
		matched = !matched;
#endif

	return matched;
}

static bool
_bwt_tuple_matches_all_keys(Relation rel, IndexTuple itup,
							ScanKey scankey, int nkeys)
{
	int i;

	if (scankey == NULL || nkeys <= 0)
		return true;

	for (i = 0; i < nkeys; i++)
	{
		if (!_bwt_tuple_matches_key(rel, itup, &scankey[i]))
			return false;
	}

	return true;
}

static BWTreePid
_bwt_pid_from_blkno(Relation rel, BlockNumber blkno)
{
	Buffer			buf;
	Page			page;
	BWTreePageOpaque opaque;
	BWTreePid		pid;

	if (!BlockNumberIsValid(blkno))
		return InvalidBWTreePid;

	buf = _bwt_getbuf(rel, blkno, BWT_READ);
	page = BufferGetPage(buf);
	opaque = BWTreePageGetOpaque(page);
	if (opaque->bwto_page_id != BWTREE_PAGE_ID)
	{
		_bwt_relbuf(rel, buf, BWT_READ);
		elog(ERROR, "bwtree: block %u has invalid page id", (unsigned int) blkno);
	}

	pid = opaque->bwto_pid;
	_bwt_relbuf(rel, buf, BWT_READ);
	return pid;
}

static void
_bwt_read_metad_snapshot(Relation rel, BWTreeMetaPageData *snapshot)
{
	Buffer				metabuf;
	Page				metapage;
	BWTreeMetaPageData *metad;

	if (snapshot == NULL)
		elog(ERROR, "bwtree: metapage snapshot output must not be NULL");

	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
	metapage = BufferGetPage(metabuf);
	metad = BWTreeMetaPageGetData(metapage);

	if (metad->bwt_magic != BWTREE_MAGIC || metad->bwt_version != BWTREE_VERSION)
	{
		_bwt_relbuf(rel, metabuf, BWT_READ);
		elog(ERROR, "bwtree: invalid metapage (magic/version mismatch)");
	}

	*snapshot = *metad;
	_bwt_relbuf(rel, metabuf, BWT_READ);
}

static void
_bwt_validate_scan_leaf(const BWTMaterializedPage *mpage,
						BlockNumber expected_prev_blkno)
{
	if (mpage == NULL)
		elog(ERROR, "bwtree: scan leaf validation requires materialized page");
	if ((mpage->flags & BWT_LEAF) == 0 || mpage->level != 0)
		elog(ERROR, "bwtree: scan reached non-leaf page (flags=%u level=%u)",
			 (unsigned int) mpage->flags,
			 (unsigned int) mpage->level);
	/*
	 * Concurrency-tolerant scan:
	 * sibling links may be transiently inconsistent while split/consolidation
	 * is publishing. Do not fail hard on prev-link mismatch.
	 */
	(void) expected_prev_blkno;
}

static bool
_bwt_pick_scan_bounds(Relation rel, BWTreeScanOpaque so,
					  ScanKeyData *lower_out, bool *has_lower_out,
					  ScanKeyData *upper_out, bool *has_upper_out)
{
	ScanKey		lower = NULL;
	ScanKey		upper = NULL;
	FmgrInfo   *orderproc;
	int			i;

	if (rel == NULL || so == NULL ||
		lower_out == NULL || has_lower_out == NULL ||
		upper_out == NULL || has_upper_out == NULL)
		elog(ERROR, "bwtree: pick-scan-bounds requires valid inputs");

	*has_lower_out = false;
	*has_upper_out = false;

	if (so->numberOfKeys <= 0 || so->keyData == NULL)
		return false;

	/*
	 * Route/early-stop fast path currently supports ASC first key only.
	 * DESC and NULL-search cases stay on conservative full-walk path.
	 */
	if ((rel->rd_indoption[0] & INDOPTION_DESC) != 0)
		return false;

	orderproc = index_getprocinfo(rel, 1, BWTORDER_PROC);

	for (i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey key = &so->keyData[i];
		int32	cmp = 0;

		if (key->sk_attno != 1)
			continue;
		if (key->sk_flags & (SK_ROW_HEADER | SK_ROW_MEMBER |
							 SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL))
			continue;
		if (key->sk_flags & SK_ISNULL)
			continue;

		if (key->sk_strategy == BTGreaterStrategyNumber ||
			key->sk_strategy == BTGreaterEqualStrategyNumber ||
			key->sk_strategy == BTEqualStrategyNumber)
		{
			if (lower == NULL)
				lower = key;
			else
			{
				cmp = DatumGetInt32(FunctionCall2Coll(orderproc,
													  rel->rd_indcollation[0],
													  key->sk_argument,
													  lower->sk_argument));
				if (cmp > 0 ||
					(cmp == 0 &&
					 key->sk_strategy == BTGreaterStrategyNumber &&
					 lower->sk_strategy != BTGreaterStrategyNumber))
					lower = key;
			}
		}

		if (key->sk_strategy == BTLessStrategyNumber ||
			key->sk_strategy == BTLessEqualStrategyNumber ||
			key->sk_strategy == BTEqualStrategyNumber)
		{
			if (upper == NULL)
				upper = key;
			else
			{
				cmp = DatumGetInt32(FunctionCall2Coll(orderproc,
													  rel->rd_indcollation[0],
													  key->sk_argument,
													  upper->sk_argument));
				if (cmp < 0 ||
					(cmp == 0 &&
					 key->sk_strategy == BTLessStrategyNumber &&
					 upper->sk_strategy != BTLessStrategyNumber))
					upper = key;
			}
		}
	}

	if (lower != NULL)
	{
		*lower_out = *lower;
		*has_lower_out = true;
	}
	if (upper != NULL)
	{
		*upper_out = *upper;
		*has_upper_out = true;
	}

	return (*has_lower_out || *has_upper_out);
}

static ScanKey
_bwt_build_route_key(Relation rel, const ScanKeyData *bound_key)
{
	ScanKeyData *route_key;
	FmgrInfo    *procinfo;
	int			flags;

	if (rel == NULL || bound_key == NULL)
		return NULL;

	route_key = (ScanKeyData *) palloc(sizeof(ScanKeyData));
	procinfo = index_getprocinfo(rel, 1, BWTORDER_PROC);
	flags = (rel->rd_indoption[0] << SK_BT_INDOPTION_SHIFT);
	if (bound_key->sk_flags & SK_ISNULL)
		flags |= SK_ISNULL;

	ScanKeyEntryInitializeWithInfo(route_key,
								   flags,
								   1,
								   InvalidStrategy,
								   InvalidOid,
								   rel->rd_indcollation[0],
								   procinfo,
								   bound_key->sk_argument);
	return route_key;
}

static int32
_bwt_compare_first_key_with_bound(Relation rel, IndexTuple itup,
								  const ScanKeyData *bound_key)
{
	TupleDesc	itupdesc;
	Datum		tuple_datum;
	bool		tuple_isnull;
	bool		arg_isnull;
	bool		nulls_first;
	FmgrInfo   *orderproc;
	int32		result;

	if (rel == NULL || itup == NULL || bound_key == NULL)
		elog(ERROR, "bwtree: compare-first-key requires valid inputs");
	if (bound_key->sk_attno != 1)
		elog(ERROR, "bwtree: compare-first-key expects attno=1, got %d",
			 bound_key->sk_attno);

	itupdesc = RelationGetDescr(rel);
	tuple_datum = index_getattr(itup, 1, itupdesc, &tuple_isnull);
	arg_isnull = ((bound_key->sk_flags & SK_ISNULL) != 0);
	nulls_first = ((rel->rd_indoption[0] & INDOPTION_NULLS_FIRST) != 0);

	if (tuple_isnull || arg_isnull)
	{
		if (tuple_isnull && arg_isnull)
			return 0;
		if (tuple_isnull)
			return nulls_first ? -1 : 1;
		return nulls_first ? 1 : -1;
	}

	orderproc = index_getprocinfo(rel, 1, BWTORDER_PROC);
	result = DatumGetInt32(FunctionCall2Coll(orderproc,
											 rel->rd_indcollation[0],
											 tuple_datum,
											 bound_key->sk_argument));
	if (rel->rd_indoption[0] & INDOPTION_DESC)
		result = -result;
	if (result < 0)
		return -1;
	if (result > 0)
		return 1;
	return 0;
}

static bool
_bwt_lower_bound_allows_leaf(Relation rel, const BWTMaterializedPage *mpage,
							 const ScanKeyData *lower_bound)
{
	IndexTuple	max_itup;
	int32		cmp;

	if (rel == NULL || mpage == NULL || lower_bound == NULL)
		elog(ERROR, "bwtree: lower-bound check requires valid inputs");
	if (mpage->nitems <= 0 || mpage->items == NULL)
		return false;

	max_itup = mpage->items[mpage->nitems - 1];
	if (max_itup == NULL)
		return false;

	cmp = _bwt_compare_first_key_with_bound(rel, max_itup, lower_bound);
	switch (lower_bound->sk_strategy)
	{
		case BTGreaterStrategyNumber:
			return (cmp > 0);
		case BTGreaterEqualStrategyNumber:
		case BTEqualStrategyNumber:
			/*
			 * Equality scans backtrack on >= bound to find the first leaf where
			 * the key could appear.
			 */
			return (cmp >= 0);
		default:
			return false;
	}
}

static bool
_bwt_stop_before_leaf(Relation rel, const BWTMaterializedPage *mpage,
					  const ScanKeyData *upper_bound)
{
	IndexTuple	first_itup;
	int32		cmp;

	if (rel == NULL || mpage == NULL || upper_bound == NULL)
		elog(ERROR, "bwtree: stop-before-leaf requires valid inputs");
	if (mpage->nitems <= 0 || mpage->items == NULL)
		return false;

	first_itup = mpage->items[0];
	if (first_itup == NULL)
		return false;

	cmp = _bwt_compare_first_key_with_bound(rel, first_itup, upper_bound);
	switch (upper_bound->sk_strategy)
	{
		case BTLessStrategyNumber:
			return (cmp >= 0);
		case BTLessEqualStrategyNumber:
		case BTEqualStrategyNumber:
			return (cmp > 0);
		default:
			return false;
	}
}

static bool
_bwt_stop_after_leaf(Relation rel, const BWTMaterializedPage *mpage,
					 const ScanKeyData *upper_bound)
{
	IndexTuple	last_itup;
	int32		cmp;

	if (rel == NULL || mpage == NULL || upper_bound == NULL)
		elog(ERROR, "bwtree: stop-after-leaf requires valid inputs");
	if (mpage->nitems <= 0 || mpage->items == NULL)
		return false;

	last_itup = mpage->items[mpage->nitems - 1];
	if (last_itup == NULL)
		return false;

	cmp = _bwt_compare_first_key_with_bound(rel, last_itup, upper_bound);
	switch (upper_bound->sk_strategy)
	{
		case BTLessStrategyNumber:
			return (cmp >= 0);
		case BTLessEqualStrategyNumber:
		case BTEqualStrategyNumber:
			return (cmp > 0);
		default:
			return false;
	}
}

static BWTreePid
_bwt_adjust_scan_start_left(Relation rel, BWTreePid start_pid,
							const ScanKeyData *lower_bound)
{
	BWTreePid	pid;
	uint64		step = 0;

	if (rel == NULL || lower_bound == NULL)
		return start_pid;
	if (start_pid == InvalidBWTreePid)
		return start_pid;

	pid = start_pid;
	while (pid != InvalidBWTreePid)
	{
		BWTreeMetaPageData	metad_snapshot;
		BWTMaterializedPage	cur_page;
		BWTMaterializedPage	prev_page;
		BWTreePid			prev_pid;
		bool				move_left;

		CHECK_FOR_INTERRUPTS();
		step++;
		if (step > BWTREE_MAX_SCAN_LEAF_STEPS)
			elog(ERROR, "bwtree: scan start-left walk exceeded safety bound");

		_bwt_read_metad_snapshot(rel, &metad_snapshot);
		_bwt_materialize_page(rel, &metad_snapshot, pid, NULL, &cur_page);
		_bwt_validate_scan_leaf(&cur_page, InvalidBlockNumber);
		prev_pid = _bwt_pid_from_blkno(rel, cur_page.prev_blkno);
		_bwt_free_materialized_page(&cur_page);
		if (prev_pid == InvalidBWTreePid)
			break;

		_bwt_read_metad_snapshot(rel, &metad_snapshot);
		_bwt_materialize_page(rel, &metad_snapshot, prev_pid, NULL, &prev_page);
		_bwt_validate_scan_leaf(&prev_page, InvalidBlockNumber);
		move_left = _bwt_lower_bound_allows_leaf(rel, &prev_page, lower_bound);
		_bwt_free_materialized_page(&prev_page);
		if (!move_left)
			break;

		pid = prev_pid;
	}

	return pid;
}

static bool
_bwt_scan_next_match(IndexScanDesc scan, BWTreeScanOpaque so,
					 ItemPointerData *tid_out)
{
	Relation			rel;

	if (scan == NULL || so == NULL || tid_out == NULL)
		elog(ERROR, "bwtree: scan-next-match requires valid inputs");

	rel = scan->indexRelation;

	if (!so->started)
	{
		Buffer				metabuf;
		Page				metapage;
		BWTreeMetaPageData *metad;
		ScanKey				route_key = NULL;
		int					route_nkeys = 0;
		BWTreePid			pid;

		metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
		metapage = BufferGetPage(metabuf);
		metad = BWTreeMetaPageGetData(metapage);
		if (metad->bwt_magic != BWTREE_MAGIC || metad->bwt_version != BWTREE_VERSION)
		{
			_bwt_relbuf(rel, metabuf, BWT_READ);
			elog(ERROR, "bwtree: invalid metapage (magic/version mismatch)");
		}

		(void) _bwt_pick_scan_bounds(rel, so,
									 &so->lower_bound, &so->has_lower_bound,
									 &so->upper_bound, &so->has_upper_bound);

		if (so->has_lower_bound)
		{
			route_key = _bwt_build_route_key(rel, &so->lower_bound);
			route_nkeys = 1;
		}

		pid = _bwt_search_leaf(rel, metad, route_key, route_nkeys);
		if (route_key != NULL)
			pfree(route_key);
		_bwt_relbuf(rel, metabuf, BWT_READ);

		if (pid != InvalidBWTreePid && so->has_lower_bound)
			pid = _bwt_adjust_scan_start_left(rel, pid, &so->lower_bound);

		_bwt_scan_reset_items(so);
		so->started = true;
		so->cur_leaf_pid = pid;
		so->leaf_walk_steps = 0;
	}

	for (;;)
	{
		if (so->cur_item < so->num_items)
		{
			*tid_out = so->items[so->cur_item].heapTid;
			so->cur_item++;
			return true;
		}

		_bwt_scan_reset_items(so);
		if (so->cur_leaf_pid == InvalidBWTreePid)
			return false;

		{
			BWTreeMetaPageData	metad_snapshot;
			BWTMaterializedPage mpage;
			BWTreePid			next_pid;
			BWTreeScanPosItem  *items = NULL;
			int					capacity = 0;
			int					nmatches = 0;
			int					i;

			CHECK_FOR_INTERRUPTS();
			so->leaf_walk_steps++;
			if (so->leaf_walk_steps > BWTREE_MAX_SCAN_LEAF_STEPS)
				elog(ERROR, "bwtree: scan leaf walk exceeded safety bound (possible sibling cycle)");

			_bwt_read_metad_snapshot(rel, &metad_snapshot);
			_bwt_materialize_page(rel, &metad_snapshot, so->cur_leaf_pid, NULL, &mpage);
			_bwt_validate_scan_leaf(&mpage, InvalidBlockNumber);

			if (so->has_upper_bound &&
				_bwt_stop_before_leaf(rel, &mpage, &so->upper_bound))
			{
				_bwt_free_materialized_page(&mpage);
				so->cur_leaf_pid = InvalidBWTreePid;
				return false;
			}

			for (i = 0; i < mpage.nitems; i++)
			{
				IndexTuple	itup = mpage.items[i];

				if (itup == NULL)
					continue;
				if (!_bwt_tuple_matches_all_keys(rel, itup, so->keyData, so->numberOfKeys))
					continue;

				if (nmatches == capacity)
				{
					capacity = (capacity == 0) ? 16 : capacity * 2;
					if (items == NULL)
						items = (BWTreeScanPosItem *)
							palloc(sizeof(BWTreeScanPosItem) * capacity);
					else
						items = (BWTreeScanPosItem *)
							repalloc(items, sizeof(BWTreeScanPosItem) * capacity);
				}

				items[nmatches].heapTid = itup->t_tid;
				nmatches++;
			}

			next_pid = _bwt_pid_from_blkno(rel, mpage.next_blkno);
			if (so->has_upper_bound &&
				_bwt_stop_after_leaf(rel, &mpage, &so->upper_bound))
				next_pid = InvalidBWTreePid;
			_bwt_free_materialized_page(&mpage);

			so->items = items;
			so->num_items = nmatches;
			so->cur_item = 0;
			so->cur_leaf_pid = next_pid;
		}
	}
}
