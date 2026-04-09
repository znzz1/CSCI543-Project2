#include "postgres.h"

#include "access/bwtree.h"
#include "access/nbtree.h"
#include "miscadmin.h"

/*
 * Safety bound that prevents infinite loops when metadata is corrupt
 * (e.g., internal-node downlink cycles).
 */
#define BWTREE_MAX_DESCEND_STEPS	1024
static int32 _bwt_compare_tuple(Relation rel, ScanKey scankey, int nkeys,
								IndexTuple itup);
static BWTreePid _bwt_choose_child_pid(Relation rel, ScanKey scankey, int nkeys,
									   const BWTreeNodeView *view);
static bool _bwt_leaf_should_move_right_fast(Relation rel,
											 const BWTreeNodeSnapshot *snapshot,
											 ScanKey scankey, int nkeys,
											 BWTreePid *right_pid_out);
static bool _bwt_should_move_right(Relation rel, ScanKey scankey, int nkeys,
								   const BWTreeNodeView *view);
static void _bwt_push_pid(BWTreeContext *ctx, BWTreePid pid);

void
_bwt_begin_traverse(BWTreeContext *ctx, MemoryContext memcxt)
{
	if (ctx == NULL)
		elog(ERROR, "bwtree: begin-traverse requires a valid context");

	memset(ctx->stack_pid, 0xFF, sizeof(ctx->stack_pid));
	ctx->depth = 0;
	ctx->memcxt = (memcxt != NULL) ? memcxt : CurrentMemoryContext;
}

void
_bwt_finish_traverse(BWTreeContext *ctx)
{
	if (ctx == NULL)
		return;

	memset(ctx->stack_pid, 0xFF, sizeof(ctx->stack_pid));
	ctx->depth = 0;
	ctx->memcxt = NULL;
}

BWTreePid
_bwt_search_leaf(Relation rel, BWTreeMetaPageData *metad,
				  ScanKey scankey, int nkeys)
{
	BWTreeContext		ctx;
	BWTreeNodeSnapshot	leaf_snapshot;
	bool				found;

	if (metad == NULL)
		elog(ERROR, "bwtree: search-leaf requires valid metapage data");

	_bwt_begin_traverse(&ctx, CurrentMemoryContext);
	found = _bwt_descend_to_leaf(rel, metad, scankey, nkeys, &ctx,
								 &leaf_snapshot);
	_bwt_finish_traverse(&ctx);

	if (!found)
		return InvalidBWTreePid;

	return leaf_snapshot.pid;
}

BWTreePid
_bwt_search_leaf_with_parent(Relation rel, BWTreeMetaPageData *metad,
							 ScanKey scankey, int nkeys,
							 BWTreePid *parent_pid)
{
	BWTreeContext		ctx;
	BWTreeNodeSnapshot	leaf_snapshot;
	bool				found;

	if (metad == NULL)
		elog(ERROR, "bwtree: search-leaf-with-parent requires valid metapage data");

	if (parent_pid != NULL)
		*parent_pid = InvalidBWTreePid;

	_bwt_begin_traverse(&ctx, CurrentMemoryContext);
	found = _bwt_descend_to_leaf(rel, metad, scankey, nkeys, &ctx,
								 &leaf_snapshot);
	if (found && parent_pid != NULL && ctx.depth >= 2)
		*parent_pid = ctx.stack_pid[ctx.depth - 2];
	_bwt_finish_traverse(&ctx);

	if (!found)
		return InvalidBWTreePid;

	return leaf_snapshot.pid;
}

bool
_bwt_descend_to_leaf(Relation rel, BWTreeMetaPageData *metad,
					 ScanKey scankey, int nkeys,
					 BWTreeContext *ctx,
					 BWTreeNodeSnapshot *leaf_snapshot)
{
	BWTreePid	cur_pid;
	int			step;

	if (metad == NULL)
		elog(ERROR, "bwtree: descend-to-leaf requires valid metapage data");
	if (ctx == NULL)
		elog(ERROR, "bwtree: descend-to-leaf requires valid traversal context");
	if (leaf_snapshot == NULL)
		elog(ERROR, "bwtree: descend-to-leaf requires output snapshot");

	/*
	 * Always start from a clean traversal stack so callers never observe
	 * stale path entries from a previous descent.
	 */
	_bwt_begin_traverse(ctx,
						(ctx->memcxt != NULL) ? ctx->memcxt : CurrentMemoryContext);

	cur_pid = metad->bwt_root_pid;
	if (cur_pid == InvalidBWTreePid)
		return false;

	for (step = 0; step < BWTREE_MAX_DESCEND_STEPS; step++)
	{
		BWTreeNodeSnapshot	cur_snapshot;

		CHECK_FOR_INTERRUPTS();
		if (!_bwt_capture_node_snapshot(rel, metad, cur_pid, &cur_snapshot))
			elog(ERROR, "bwtree: descend cannot find PID %u",
				 (unsigned int) cur_pid);

		/*
		 * Insert/search hot path optimization:
		 *
		 * For leaf nodes, avoid full node materialization. We only inspect
		 * split-control deltas and can return leaf snapshot directly.
		 */
		if (cur_snapshot.is_leaf)
		{
			BWTreePid	split_right_pid = InvalidBWTreePid;

			if (_bwt_leaf_should_move_right_fast(rel, &cur_snapshot,
												 scankey, nkeys,
												 &split_right_pid))
			{
				cur_pid = split_right_pid;
				continue;
			}

			_bwt_push_pid(ctx, cur_pid);
			*leaf_snapshot = cur_snapshot;
			return true;
		}

		{
			BWTreeNodeView	view;
			BWTreePid		next_pid;

			_bwt_materialize_node(rel, metad, cur_pid, &view);

			/*
			 * Correctness-first split routing:
			 *
			 * If a split delta exists and key belongs to the right half, move to
			 * split_right_pid before recording this level in traversal stack.
			 * This approximates "move-right" semantics even while full SMO
			 * publication protocol is still under construction.
			 */
			if (_bwt_should_move_right(rel, scankey, nkeys, &view))
			{
				cur_pid = view.split_right_pid;
				_bwt_free_node_view(&view);
				continue;
			}

			_bwt_push_pid(ctx, cur_pid);
			next_pid = _bwt_choose_child_pid(rel, scankey, nkeys, &view);
			cur_pid = next_pid;
			_bwt_free_node_view(&view);
		}
	}

	elog(ERROR, "bwtree: descend-to-leaf exceeded max steps (possible routing cycle)");
	return false;
}

int32
_bwt_compare(Relation rel, ScanKey scankey, int nkeys,
			 Page page, OffsetNumber offnum)
{
	OffsetNumber	maxoff;
	ItemId			itemid;
	IndexTuple		itup;
	BWTreePageOpaque opaque;

	if (page == NULL)
		elog(ERROR, "bwtree: compare requires a valid page");
	if (offnum < FirstOffsetNumber)
		elog(ERROR, "bwtree: compare offset %u is invalid",
			 (unsigned int) offnum);

	opaque = BWTreePageGetOpaque(page);
	if (opaque->bwto_page_id != BWTREE_PAGE_ID)
		elog(ERROR, "bwtree: compare target page has invalid page id");

	maxoff = PageGetMaxOffsetNumber(page);
	if (offnum > maxoff)
		elog(ERROR, "bwtree: compare offset %u exceeds max offset %u",
			 (unsigned int) offnum, (unsigned int) maxoff);

	itemid = PageGetItemId(page, offnum);
	if (!ItemIdIsUsed(itemid))
		elog(ERROR, "bwtree: compare target offset %u is unused",
			 (unsigned int) offnum);

	itup = (IndexTuple) PageGetItem(page, itemid);
	return _bwt_compare_tuple(rel, scankey, nkeys, itup);
}

static int32
_bwt_compare_tuple(Relation rel, ScanKey scankey, int nkeys, IndexTuple itup)
{
	TupleDesc	itupdesc;
	int			i;

	if (rel == NULL)
		elog(ERROR, "bwtree: compare requires a valid relation");
	if (itup == NULL)
		elog(ERROR, "bwtree: compare requires a valid index tuple");

	if (scankey == NULL || nkeys <= 0)
		return 0;

	itupdesc = RelationGetDescr(rel);

	for (i = 0; i < nkeys; i++)
	{
		ScanKey		key = &scankey[i];
		Datum		datum;
		bool		isNull;
		int32		result;

		if (key->sk_attno <= 0)
			elog(ERROR, "bwtree: scankey[%d] has invalid attno %d",
				 i, key->sk_attno);
		if (key->sk_attno > itupdesc->natts)
			elog(ERROR, "bwtree: scankey[%d] attno %d exceeds index natts %d",
				 i, key->sk_attno, itupdesc->natts);

		datum = index_getattr(itup, key->sk_attno, itupdesc, &isNull);

		if (key->sk_flags & SK_ISNULL)
		{
			if (isNull)
				result = 0;
			else if (key->sk_flags & SK_BT_NULLS_FIRST)
				result = -1;
			else
				result = 1;
		}
		else if (isNull)
		{
			if (key->sk_flags & SK_BT_NULLS_FIRST)
				result = 1;
			else
				result = -1;
		}
		else
		{
			/*
			 * sk_func is called as (tuple_key, scan_argument).  We invert the
			 * sign in ASC order so return value semantics stay:
			 *   <0 => scankey < tuple
			 *    0 => equal
			 *   >0 => scankey > tuple
			 */
			result = DatumGetInt32(FunctionCall2Coll(&key->sk_func,
													 key->sk_collation,
													 datum,
													 key->sk_argument));
			if (!(key->sk_flags & SK_BT_DESC))
			{
				if (result > 0)
					result = -1;
				else if (result < 0)
					result = 1;
			}
		}

		if (result != 0)
			return result;
	}

	return 0;
}

static bool
_bwt_leaf_should_move_right_fast(Relation rel,
								 const BWTreeNodeSnapshot *snapshot,
								 ScanKey scankey, int nkeys,
								 BWTreePid *right_pid_out)
{
	BlockNumber	cur_blkno;
	BlockNumber	rel_nblocks;
	BlockNumber	hops = 0;
	BlockNumber	hops_limit;
	bool		has_split = false;
	bool		has_separator = false;
	int32		separator_cmp = 0;
	BWTreePid	split_right_pid = InvalidBWTreePid;

	if (right_pid_out != NULL)
		*right_pid_out = InvalidBWTreePid;

	if (rel == NULL || snapshot == NULL)
		return false;
	if (!snapshot->is_leaf)
		return false;
	if ((snapshot->flags & BWT_SPLIT_PENDING) == 0)
		return false;
	if (!BlockNumberIsValid(snapshot->delta_blkno))
		return false;
	if (scankey == NULL || nkeys <= 0)
		return false;

	cur_blkno = snapshot->delta_blkno;
	rel_nblocks = RelationGetNumberOfBlocks(rel);
	hops_limit = rel_nblocks + 1;

	while (BlockNumberIsValid(cur_blkno))
	{
		Buffer					buf;
		Page					page;
		BWTreePageOpaque		opaque;
		OffsetNumber			maxoff;
		BlockNumber				next_blkno;

		if (cur_blkno >= rel_nblocks)
			elog(ERROR, "bwtree: leaf move-right chain points outside relation (blk=%u nblocks=%u)",
				 (unsigned int) cur_blkno,
				 (unsigned int) rel_nblocks);

		buf = _bwt_getbuf(rel, cur_blkno, BWT_READ);
		page = BufferGetPage(buf);
		opaque = BWTreePageGetOpaque(page);
		if (opaque->bwto_page_id != BWTREE_PAGE_ID || !BWTreePageIsDelta(opaque))
		{
			_bwt_relbuf(rel, buf, BWT_READ);
			elog(ERROR, "bwtree: leaf move-right encountered non-delta page in chain (blk=%u)",
				 (unsigned int) cur_blkno);
		}

		maxoff = PageGetMaxOffsetNumber(page);
		if (maxoff >= FirstOffsetNumber)
		{
			ItemId					itemid;
			BWTreeDeltaRecordData  *drec;

			itemid = PageGetItemId(page, FirstOffsetNumber);
			if (ItemIdIsUsed(itemid))
			{
				drec = (BWTreeDeltaRecordData *) PageGetItem(page, itemid);
				if (!has_split && drec->type == BW_DELTA_SPLIT)
				{
					has_split = true;
					split_right_pid = drec->related_pid;
				}
				else if (!has_separator &&
						 drec->type == BW_DELTA_SEPARATOR &&
						 drec->data_len > 0)
				{
					IndexTuple sep_itup;

					sep_itup = BWTreeDeltaRecordGetTuple(drec);
					if (sep_itup != NULL)
					{
						has_separator = true;
						separator_cmp = _bwt_compare_tuple(rel, scankey, nkeys, sep_itup);
					}
				}
			}
		}

		next_blkno = opaque->bwto_next;
		_bwt_relbuf(rel, buf, BWT_READ);
		cur_blkno = next_blkno;

		if (has_split && has_separator)
			break;

		hops++;
		if (hops > hops_limit)
			elog(ERROR, "bwtree: leaf move-right check exceeded safety bound");
	}

	if (has_split && has_separator &&
		split_right_pid != InvalidBWTreePid &&
		separator_cmp >= 0)
	{
		if (right_pid_out != NULL)
			*right_pid_out = split_right_pid;
		return true;
	}

	return false;
}

static bool
_bwt_should_move_right(Relation rel, ScanKey scankey, int nkeys,
					   const BWTreeNodeView *view)
{
	if (view == NULL)
		return false;
	if (view->state != BWT_STATE_SPLIT_PENDING)
		return false;
	if (view->split_separator == NULL || view->split_right_pid == InvalidBWTreePid)
		return false;
	if (scankey == NULL || nkeys <= 0)
		return false;

	return (_bwt_compare_tuple(rel, scankey, nkeys, view->split_separator) >= 0);
}

static BWTreePid
_bwt_choose_child_pid(Relation rel, ScanKey scankey, int nkeys,
					  const BWTreeNodeView *view)
{
	BWTreePid	child_pid = InvalidBWTreePid;
	int			i;

	if (view == NULL)
		elog(ERROR, "bwtree: choose-child requires a valid node view");
	if (view->snapshot.is_leaf)
		elog(ERROR, "bwtree: choose-child called on leaf PID %u",
			 (unsigned int) view->snapshot.pid);
	if (view->page.nitems <= 0 || view->page.items == NULL)
		elog(ERROR, "bwtree: internal PID %u has no downlink tuples",
			 (unsigned int) view->snapshot.pid);

	/* No key means leftmost descent. */
	if (scankey == NULL || nkeys <= 0)
	{
		child_pid = BWTreeTupleGetDownLink(view->page.items[0]);
		if (child_pid == InvalidBWTreePid)
			elog(ERROR, "bwtree: internal PID %u has invalid leftmost downlink",
				 (unsigned int) view->snapshot.pid);
		return child_pid;
	}

	for (i = 0; i < view->page.nitems; i++)
	{
		IndexTuple	itup = view->page.items[i];
		BWTreePid	downlink;
		int32		cmp;

		if (itup == NULL)
			elog(ERROR, "bwtree: internal PID %u has NULL tuple at slot %d",
				 (unsigned int) view->snapshot.pid, i);

		downlink = BWTreeTupleGetDownLink(itup);
		if (downlink == InvalidBWTreePid)
			elog(ERROR, "bwtree: internal PID %u has invalid downlink at slot %d",
				 (unsigned int) view->snapshot.pid, i);

		cmp = _bwt_compare_tuple(rel, scankey, nkeys, itup);
		if (cmp < 0)
			break;

		child_pid = downlink;
	}

	if (child_pid == InvalidBWTreePid)
	{
		child_pid = BWTreeTupleGetDownLink(view->page.items[0]);
		if (child_pid == InvalidBWTreePid)
			elog(ERROR, "bwtree: internal PID %u has invalid fallback downlink",
				 (unsigned int) view->snapshot.pid);
	}

	return child_pid;
}

static void
_bwt_push_pid(BWTreeContext *ctx, BWTreePid pid)
{
	int max_depth;

	if (ctx == NULL)
		elog(ERROR, "bwtree: push-pid requires a valid traversal context");

	max_depth = (int) lengthof(ctx->stack_pid);
	if (ctx->depth < 0 || ctx->depth >= max_depth)
		elog(ERROR, "bwtree: traversal stack overflow at depth %d", ctx->depth);

	ctx->stack_pid[ctx->depth++] = pid;
}
