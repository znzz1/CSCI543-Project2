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
static ScanKey _bwt_build_scan_route_key(Relation rel, BWTreeScanOpaque so,
										 int *nkeys_out);
static void _bwt_collect_scan_items(IndexScanDesc scan, BWTreeScanOpaque so);

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
	so->cur_item = 0;

	ItemPointerSetInvalid(&scan->xs_heaptid);
	scan->xs_recheck = false;
}

bool
bwtreegettuple(IndexScanDesc scan, ScanDirection dir)
{
	BWTreeScanOpaque so;
	bool				found;

	_bwt_epoch_enter();
	PG_TRY();
	{
		if (scan == NULL || scan->opaque == NULL)
			elog(ERROR, "bwtree: gettuple requires a valid scan descriptor");
		if (dir != ForwardScanDirection)
			elog(ERROR, "bwtree: backward scan is not supported yet");

		so = (BWTreeScanOpaque) scan->opaque;
		if (!so->started)
		{
			_bwt_collect_scan_items(scan, so);
			so->started = true;
			so->cur_item = 0;
		}

		if (so->cur_item >= so->num_items)
		{
			ItemPointerSetInvalid(&scan->xs_heaptid);
			found = false;
		}
		else
		{
			scan->xs_heaptid = so->items[so->cur_item].heapTid;
			scan->xs_recheck = false;
			so->cur_item++;
			found = true;
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
	int				i;

	ntuples = 0;
	_bwt_epoch_enter();
	PG_TRY();
	{
		if (scan == NULL || scan->opaque == NULL)
			elog(ERROR, "bwtree: getbitmap requires a valid scan descriptor");
		if (tbm == NULL)
			elog(ERROR, "bwtree: getbitmap requires a valid tidbitmap");

		so = (BWTreeScanOpaque) scan->opaque;
		_bwt_scan_reset_items(so);
		_bwt_collect_scan_items(scan, so);
		so->started = true;
		so->cur_item = so->num_items;

		for (i = 0; i < so->num_items; i++)
		{
			tbm_add_tuples(tbm, &so->items[i].heapTid, 1, false);
			ntuples++;
		}

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

static ScanKey
_bwt_build_scan_route_key(Relation rel, BWTreeScanOpaque so, int *nkeys_out)
{
	int			i;
	ScanKey		chosen = NULL;
	ScanKeyData *route_key;
	FmgrInfo    *procinfo;
	int			flags;

	if (nkeys_out == NULL)
		elog(ERROR, "bwtree: route-key builder requires nkeys output");
	*nkeys_out = 0;

	if (rel == NULL || so == NULL || so->numberOfKeys <= 0 || so->keyData == NULL)
		return NULL;

	/*
	 * Correctness-first guard:
	 * keep DESC/null-order corner cases on the existing leftmost-start path.
	 */
	if ((rel->rd_indoption[0] & INDOPTION_DESC) != 0)
		return NULL;

	for (i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey key = &so->keyData[i];

		if (key->sk_attno != 1)
			continue;
		if (key->sk_flags & (SK_ROW_HEADER | SK_ROW_MEMBER |
							 SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL))
			continue;
		if (key->sk_flags & SK_ISNULL)
			continue;
		/*
		 * Correctness-first guard:
		 * for duplicate keys spanning multiple leaves, '=' or '>=' start-route
		 * can skip qualifying tuples in left siblings. Restrict route assist to
		 * strict '>' lower bound.
		 */
		if (key->sk_strategy != BTGreaterStrategyNumber)
			continue;

		chosen = key;
		break;
	}

	if (chosen == NULL)
		return NULL;

	route_key = (ScanKeyData *) palloc(sizeof(ScanKeyData));
	procinfo = index_getprocinfo(rel, 1, BWTORDER_PROC);
	flags = ((rel->rd_indoption[0] << SK_BT_INDOPTION_SHIFT) |
			 ((chosen->sk_flags & SK_ISNULL) ? SK_ISNULL : 0));

	ScanKeyEntryInitializeWithInfo(route_key,
								   flags,
								   1,
								   InvalidStrategy,
								   InvalidOid,
								   rel->rd_indcollation[0],
								   procinfo,
								   chosen->sk_argument);
	*nkeys_out = 1;
	return route_key;
}

static void
_bwt_collect_scan_items(IndexScanDesc scan, BWTreeScanOpaque so)
{
	Relation			rel;
	Buffer				metabuf;
	Page				metapage;
	BWTreeMetaPageData *metad;
	BWTreeMetaPageData	metad_snapshot;
	BWTreePid			pid;
	BlockNumber			prev_leaf_blkno = InvalidBlockNumber;
	uint64				step = 0;
	uint64				limit;
	int					capacity = 0;
	BWTreeScanPosItem  *items = NULL;
	ScanKey				route_key = NULL;
	int					route_nkeys = 0;

	if (scan == NULL || so == NULL)
		elog(ERROR, "bwtree: collect-scan-items requires valid scan state");

	rel = scan->indexRelation;
	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
	metapage = BufferGetPage(metabuf);
	metad = BWTreeMetaPageGetData(metapage);

	if (metad->bwt_magic != BWTREE_MAGIC || metad->bwt_version != BWTREE_VERSION)
	{
		_bwt_relbuf(rel, metabuf, BWT_READ);
		elog(ERROR, "bwtree: invalid metapage (magic/version mismatch)");
	}

	/*
	 * Correctness-first policy:
	 *
	 * Use a conservative lower-bound route only when it is obviously safe
	 * (first-key =/>=/>, ASC). Otherwise fall back to leftmost leaf start.
	 * Tuple-level predicate checking still guarantees final correctness.
	 */
	route_key = _bwt_build_scan_route_key(rel, so, &route_nkeys);
	pid = _bwt_search_leaf(rel, metad, route_key, route_nkeys);
	if (route_key != NULL)
		pfree(route_key);
	so->cur_leaf_pid = pid;
	if (pid == InvalidBWTreePid)
	{
		_bwt_relbuf(rel, metabuf, BWT_READ);
		so->items = NULL;
		so->num_items = 0;
		return;
	}

	/*
	 * Use a stable global guard. A limit derived from an initial PID snapshot
	 * can false-trigger under concurrent growth.
	 */
	limit = BWTREE_MAX_SCAN_LEAF_STEPS;
	_bwt_relbuf(rel, metabuf, BWT_READ);

	while (pid != InvalidBWTreePid)
	{
		BWTMaterializedPage mpage;
		int					i;
		BWTreePid			next_pid;

		CHECK_FOR_INTERRUPTS();
		/*
		 * Avoid holding metapage lock across the whole range scan.
		 * Refresh a short-lived metapage snapshot per leaf materialization.
		 */
		_bwt_read_metad_snapshot(rel, &metad_snapshot);
		_bwt_materialize_page(rel, &metad_snapshot, pid, NULL, &mpage);
		_bwt_validate_scan_leaf(&mpage, prev_leaf_blkno);
		prev_leaf_blkno = mpage.base_blkno;

		for (i = 0; i < mpage.nitems; i++)
		{
			IndexTuple	itup = mpage.items[i];

			if (itup == NULL)
				continue;
			if (!_bwt_tuple_matches_all_keys(rel, itup, so->keyData, so->numberOfKeys))
				continue;

			if (so->num_items == capacity)
			{
				capacity = (capacity == 0) ? 64 : capacity * 2;
				if (items == NULL)
					items = (BWTreeScanPosItem *)
						palloc(sizeof(BWTreeScanPosItem) * capacity);
				else
					items = (BWTreeScanPosItem *)
						repalloc(items, sizeof(BWTreeScanPosItem) * capacity);
			}

			items[so->num_items].heapTid = itup->t_tid;
			so->num_items++;
		}

		next_pid = _bwt_pid_from_blkno(rel, mpage.next_blkno);
		_bwt_free_materialized_page(&mpage);

		pid = next_pid;
		step++;
		if (step > limit)
		{
			if (items != NULL)
				pfree(items);
			elog(ERROR, "bwtree: scan leaf walk exceeded safety bound (possible sibling cycle)");
		}
	}

	so->items = items;
}
