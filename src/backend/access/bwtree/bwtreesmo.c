/*-------------------------------------------------------------------------
 *
 * bwtreesmo.c
 *    Structure-modification skeleton for the Bw-tree index.
 *
 *------------------------------------------------------------------------- */
#include "postgres.h"

#include <limits.h>

#include "access/bwtree.h"
#include "access/genam.h"
#include "catalog/pg_index.h"
#include "miscadmin.h"

/*
 * Correctness-first split trigger:
 * use tuple-count threshold (not byte-accurate free-space accounting).
 */
#define BWTREE_LEAF_SPLIT_THRESHOLD	128

static IndexTuple _bwt_copy_itup(IndexTuple src);
static int	_bwt_tuple_keycmp(Relation rel, IndexTuple a, IndexTuple b);
static void _bwt_sort_tuples(Relation rel, IndexTuple *items, int nitems);
static void _bwt_fill_page_with_items(Page page, IndexTuple *items, int nitems);
static bool _bwt_try_fill_page_with_items(Page page, IndexTuple *items, int nitems);
static bool _bwt_page_build_fits(BWTreePid pid, uint16 flags, uint32 level,
								 BlockNumber prev_blkno, BlockNumber next_blkno,
								 IndexTuple *items, int nitems);
static bool _bwt_parent_has_child(Relation rel, BWTreeMetaPageData *metad,
								  BWTreePid parent_pid, BWTreePid child_pid);
static bool _bwt_find_parent_pid(Relation rel, BWTreeMetaPageData *metad,
								 BWTreePid child_pid, BWTreePid *parent_pid_out);
static BWTreePid _bwt_resolve_parent_pid(Relation rel, BWTreeMetaPageData *metad,
										 BWTreePid child_pid, BWTreePid parent_hint);
static void _bwt_split_internal_from_items(Relation rel, BWTreeMetaPageData *metad,
										   const BWTreeNodeSnapshot *snapshot,
										   const BWTMaterializedPage *mpage,
										   IndexTuple *items, int nitems,
										   BWTreePid parent_pid_hint);
static void _bwt_insert_into_internal(Relation rel, BWTreeMetaPageData *metad,
									  BWTreePid pid, BWTreePid split_left_child_pid,
									  IndexTuple new_itup);
static void _bwt_publish_split_metadata(Relation rel, BWTreeMetaPageData *metad,
										BWTreePid split_pid, BWTreePid right_pid,
										IndexTuple sep_itup);

static IndexTuple
_bwt_copy_itup(IndexTuple src)
{
	Size		sz;
	IndexTuple	dst;

	if (src == NULL)
		elog(ERROR, "bwtree: cannot copy NULL index tuple");

	sz = IndexTupleSize(src);
	dst = (IndexTuple) palloc(sz);
	memcpy(dst, src, sz);
	return dst;
}

static int
_bwt_tuple_keycmp(Relation rel, IndexTuple a, IndexTuple b)
{
	TupleDesc	itupdesc;
	int			nkeys;
	int			i;

	if (rel == NULL || a == NULL || b == NULL)
		elog(ERROR, "bwtree: tuple comparison requires valid inputs");

	itupdesc = RelationGetDescr(rel);
	nkeys = IndexRelationGetNumberOfKeyAttributes(rel);

	for (i = 0; i < nkeys; i++)
	{
		Datum		da;
		Datum		db;
		bool		na;
		bool		nb;
		int32		res;
		bool		nulls_first;
		bool		desc;
		FmgrInfo   *orderproc;

		da = index_getattr(a, i + 1, itupdesc, &na);
		db = index_getattr(b, i + 1, itupdesc, &nb);
		nulls_first = (rel->rd_indoption[i] & INDOPTION_NULLS_FIRST) != 0;
		desc = (rel->rd_indoption[i] & INDOPTION_DESC) != 0;

		if (na || nb)
		{
			if (na && nb)
				continue;
			if (na)
				return nulls_first ? -1 : 1;
			return nulls_first ? 1 : -1;
		}

		orderproc = index_getprocinfo(rel, i + 1, BWTORDER_PROC);
		res = DatumGetInt32(FunctionCall2Coll(orderproc,
											  rel->rd_indcollation[i],
											  da, db));
		if (res != 0)
		{
			if (desc)
				res = -res;
			return (res < 0) ? -1 : 1;
		}
	}

	return 0;
}

static void
_bwt_sort_tuples(Relation rel, IndexTuple *items, int nitems)
{
	int	i;

	if (items == NULL || nitems <= 1)
		return;

	/*
	 * Correctness-first trade-off:
	 *
	 * Use stable insertion sort instead of qsort. Internal pages can contain
	 * repeated separator keys; preserving equal-key relative order keeps
	 * child-routing deterministic under the current downlink encoding.
	 */
	for (i = 1; i < nitems; i++)
	{
		IndexTuple	cur = items[i];
		int			j = i - 1;

		while (j >= 0 && _bwt_tuple_keycmp(rel, items[j], cur) > 0)
		{
			items[j + 1] = items[j];
			j--;
		}
		items[j + 1] = cur;
	}
}

static void
_bwt_fill_page_with_items(Page page, IndexTuple *items, int nitems)
{
	int i;

	for (i = 0; i < nitems; i++)
	{
		OffsetNumber off;
		Size		 sz;

		if (items[i] == NULL)
			continue;

		sz = IndexTupleSize(items[i]);
		off = PageAddItem(page, (Item) items[i], sz,
						  InvalidOffsetNumber, false, false);
		if (off == InvalidOffsetNumber)
			elog(ERROR, "bwtree: page is full while writing split/rewrite result");
	}
}

static bool
_bwt_try_fill_page_with_items(Page page, IndexTuple *items, int nitems)
{
	int i;

	for (i = 0; i < nitems; i++)
	{
		OffsetNumber off;
		Size		 sz;

		if (items[i] == NULL)
			continue;

		sz = IndexTupleSize(items[i]);
		off = PageAddItem(page, (Item) items[i], sz,
						  InvalidOffsetNumber, false, false);
		if (off == InvalidOffsetNumber)
			return false;
	}

	return true;
}

static bool
_bwt_page_build_fits(BWTreePid pid, uint16 flags, uint32 level,
					 BlockNumber prev_blkno, BlockNumber next_blkno,
					 IndexTuple *items, int nitems)
{
	Page			workpage;
	BWTreePageOpaque opaque;
	bool			ok;

	workpage = (Page) palloc(BLCKSZ);
	_bwt_initpage(workpage, flags, pid, level);
	opaque = BWTreePageGetOpaque(workpage);
	opaque->bwto_prev = prev_blkno;
	opaque->bwto_next = next_blkno;
	ok = _bwt_try_fill_page_with_items(workpage, items, nitems);
	pfree(workpage);

	return ok;
}

static bool
_bwt_parent_has_child(Relation rel, BWTreeMetaPageData *metad,
					  BWTreePid parent_pid, BWTreePid child_pid)
{
	BWTreeNodeSnapshot	snapshot;
	BWTMaterializedPage mpage;
	bool				found = false;
	int					i;

	if (parent_pid == InvalidBWTreePid || child_pid == InvalidBWTreePid)
		return false;

	if (!_bwt_capture_node_snapshot(rel, metad, parent_pid, &snapshot))
		return false;
	if (snapshot.is_leaf)
		return false;

	_bwt_materialize_page(rel, metad, parent_pid, NULL, &mpage);
	for (i = 0; i < mpage.nitems; i++)
	{
		IndexTuple	itup = mpage.items[i];
		BWTreePid	downlink;

		if (itup == NULL)
			continue;
		downlink = BWTreeTupleGetDownLink(itup);
		if (downlink == child_pid)
		{
			found = true;
			break;
		}
	}
	_bwt_free_materialized_page(&mpage);
	return found;
}

static bool
_bwt_find_parent_pid(Relation rel, BWTreeMetaPageData *metad,
					 BWTreePid child_pid, BWTreePid *parent_pid_out)
{
	int			nslots;
	bool	   *visited;
	bool	   *queued;
	BWTreePid  *stack;
	int			top = 0;
	uint64		steps = 0;
	uint64		steps_limit;

	if (parent_pid_out == NULL)
		elog(ERROR, "bwtree: parent lookup requires output pointer");

	*parent_pid_out = InvalidBWTreePid;
	if (child_pid == InvalidBWTreePid)
		return false;
	if (metad->bwt_root_pid == InvalidBWTreePid)
		return false;
	if (child_pid == metad->bwt_root_pid)
		return true;
	if (child_pid >= metad->bwt_next_pid)
		return false;
	if (metad->bwt_next_pid == 0 ||
		metad->bwt_next_pid > (BWTreePid) INT_MAX)
		elog(ERROR, "bwtree: invalid PID space while searching parent");

	nslots = (int) metad->bwt_next_pid;
	visited = (bool *) palloc0(sizeof(bool) * nslots);
	queued = (bool *) palloc0(sizeof(bool) * nslots);
	stack = (BWTreePid *) palloc(sizeof(BWTreePid) * nslots);

	if (metad->bwt_root_pid >= metad->bwt_next_pid)
	{
		pfree(stack);
		pfree(queued);
		pfree(visited);
		elog(ERROR, "bwtree: root PID %u is out of range",
			 (unsigned int) metad->bwt_root_pid);
	}

	stack[top++] = metad->bwt_root_pid;
	queued[metad->bwt_root_pid] = true;
	steps_limit = (uint64) nslots * 8 + 64;

	while (top > 0)
	{
		BWTreePid		cur_pid;
		BWTreeNodeView	view;
		int				i;

		CHECK_FOR_INTERRUPTS();
		if (steps++ > steps_limit)
		{
			pfree(stack);
			pfree(queued);
			pfree(visited);
			elog(ERROR, "bwtree: parent lookup exceeded safety bound");
		}

		cur_pid = stack[--top];
		if (cur_pid >= metad->bwt_next_pid)
			continue;
		if (visited[cur_pid])
			continue;
		visited[cur_pid] = true;

		_bwt_materialize_node(rel, metad, cur_pid, &view);
		if (view.snapshot.is_leaf)
		{
			_bwt_free_node_view(&view);
			continue;
		}

		for (i = 0; i < view.page.nitems; i++)
		{
			IndexTuple	itup = view.page.items[i];
			BWTreePid	downlink;

			if (itup == NULL)
				continue;
			downlink = BWTreeTupleGetDownLink(itup);
			if (downlink == InvalidBWTreePid || downlink >= metad->bwt_next_pid)
				continue;

			if (downlink == child_pid)
			{
				*parent_pid_out = cur_pid;
				_bwt_free_node_view(&view);
				pfree(stack);
				pfree(queued);
				pfree(visited);
				return true;
			}

			if (!queued[downlink])
			{
				if (top >= nslots)
				{
					_bwt_free_node_view(&view);
					pfree(stack);
					pfree(queued);
					pfree(visited);
					elog(ERROR, "bwtree: parent lookup stack overflow");
				}
				stack[top++] = downlink;
				queued[downlink] = true;
			}
		}

		_bwt_free_node_view(&view);
	}

	pfree(stack);
	pfree(queued);
	pfree(visited);
	return false;
}

static BWTreePid
_bwt_resolve_parent_pid(Relation rel, BWTreeMetaPageData *metad,
						BWTreePid child_pid, BWTreePid parent_hint)
{
	BWTreePid	parent_pid;
	bool		found;

	if (child_pid == metad->bwt_root_pid)
		return InvalidBWTreePid;

	if (parent_hint != InvalidBWTreePid &&
		_bwt_parent_has_child(rel, metad, parent_hint, child_pid))
		return parent_hint;

	found = _bwt_find_parent_pid(rel, metad, child_pid, &parent_pid);
	if (!found || parent_pid == InvalidBWTreePid)
		elog(ERROR, "bwtree: cannot resolve parent for PID %u",
			 (unsigned int) child_pid);

	return parent_pid;
}

static void
_bwt_publish_split_metadata(Relation rel, BWTreeMetaPageData *metad,
							BWTreePid split_pid, BWTreePid right_pid,
							IndexTuple sep_itup)
{
	if (sep_itup == NULL)
		elog(ERROR, "bwtree: split metadata publish requires separator tuple");
	if (split_pid == InvalidBWTreePid || right_pid == InvalidBWTreePid)
		elog(ERROR, "bwtree: split metadata publish requires valid PIDs");

	/*
	 * Publish split metadata as two control deltas on split source PID:
	 *   1) SPLIT     : right sibling PID
	 *   2) SEPARATOR : split key
	 *
	 * Search path consumes these to perform move-right safely if it reaches
	 * the old left node before/without perfect parent routing.
	 */
	_bwt_delta_install(rel, metad, split_pid, BW_DELTA_SPLIT, NULL, right_pid);
	_bwt_delta_install(rel, metad, split_pid, BW_DELTA_SEPARATOR, sep_itup, right_pid);
}

static void
_bwt_split_internal_from_items(Relation rel, BWTreeMetaPageData *metad,
							   const BWTreeNodeSnapshot *snapshot,
							   const BWTMaterializedPage *mpage,
							   IndexTuple *items, int nitems,
							   BWTreePid parent_pid_hint)
{
	int					split_at;
	Buffer				metabuf;
	Buffer				leftbuf;
	Buffer				rightbuf;
	Page				leftpage;
	Page				rightpage;
	BWTreePageOpaque	left_opaque;
	BWTreePageOpaque	right_opaque;
	BWTreePid			right_pid;
	BlockNumber			right_blkno;
	IndexTuple			sep_itup;

	if (snapshot == NULL || mpage == NULL || items == NULL)
		elog(ERROR, "bwtree: internal split requires valid inputs");
	if (snapshot->is_leaf)
		elog(ERROR, "bwtree: internal split called on leaf PID %u",
			 (unsigned int) snapshot->pid);
	if (nitems < 2)
		elog(ERROR, "bwtree: cannot split internal PID %u with %d items",
			 (unsigned int) snapshot->pid, nitems);

	split_at = nitems / 2;
	if (split_at <= 0)
		split_at = 1;
	if (split_at >= nitems)
		split_at = nitems - 1;

	/*
	 * Caller holds metapage write lock; take only a pin for map helpers.
	 */
	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_NOLOCK);

	leftbuf = _bwt_getbuf(rel, snapshot->base_blkno, BWT_WRITE);
	rightbuf = _bwt_allocbuf(rel);
	right_blkno = BufferGetBlockNumber(rightbuf);
	right_pid = _bwt_map_alloc_pid(rel, metad, metabuf,
								   right_blkno, InvalidBlockNumber);

	leftpage = BufferGetPage(leftbuf);
	rightpage = BufferGetPage(rightbuf);
	_bwt_initpage(leftpage, 0, snapshot->pid, snapshot->level);
	_bwt_initpage(rightpage, 0, right_pid, snapshot->level);

	left_opaque = BWTreePageGetOpaque(leftpage);
	right_opaque = BWTreePageGetOpaque(rightpage);
	left_opaque->bwto_prev = mpage->prev_blkno;
	left_opaque->bwto_next = right_blkno;
	right_opaque->bwto_prev = snapshot->base_blkno;
	right_opaque->bwto_next = mpage->next_blkno;

	if (!_bwt_try_fill_page_with_items(leftpage, items, split_at) ||
		!_bwt_try_fill_page_with_items(rightpage, &items[split_at], nitems - split_at))
	{
		_bwt_relbuf(rel, leftbuf, BWT_WRITE);
		_bwt_relbuf(rel, rightbuf, BWT_WRITE);
		_bwt_relbuf(rel, metabuf, BWT_NOLOCK);
		elog(ERROR, "bwtree: internal split output page overflow");
	}

	MarkBufferDirty(leftbuf);
	MarkBufferDirty(rightbuf);
	_bwt_relbuf(rel, leftbuf, BWT_WRITE);
	_bwt_relbuf(rel, rightbuf, BWT_WRITE);

	_bwt_map_update(rel, metad, snapshot->pid,
					snapshot->base_blkno, InvalidBlockNumber);

	if (BlockNumberIsValid(mpage->next_blkno))
	{
		Buffer			nextbuf;
		Page			nextpage;
		BWTreePageOpaque next_opaque;

		nextbuf = _bwt_getbuf(rel, mpage->next_blkno, BWT_WRITE);
		nextpage = BufferGetPage(nextbuf);
		next_opaque = BWTreePageGetOpaque(nextpage);
		if (next_opaque->bwto_page_id == BWTREE_PAGE_ID)
		{
			next_opaque->bwto_prev = right_blkno;
			MarkBufferDirty(nextbuf);
		}
		_bwt_relbuf(rel, nextbuf, BWT_WRITE);
	}

	sep_itup = _bwt_copy_itup(items[split_at]);
	BWTreeTupleSetDownLink(sep_itup, right_pid);
	_bwt_publish_split_metadata(rel, metad, snapshot->pid, right_pid, sep_itup);

	if (snapshot->is_root)
	{
		_bwt_install_new_root(rel, metad, snapshot->pid, right_pid,
							  sep_itup, snapshot->level);
	}
	else
	{
		BWTreePid	parent_pid;

		parent_pid = _bwt_resolve_parent_pid(rel, metad,
											 snapshot->pid, parent_pid_hint);
		_bwt_insert_item(rel, metad, parent_pid, snapshot->pid,
						 sep_itup, false);
	}

	pfree(sep_itup);
	_bwt_relbuf(rel, metabuf, BWT_NOLOCK);
}

static void
_bwt_insert_into_internal(Relation rel, BWTreeMetaPageData *metad,
						  BWTreePid pid, BWTreePid split_left_child_pid,
						  IndexTuple new_itup)
{
	BWTreeNodeSnapshot	snapshot;
	BWTMaterializedPage mpage;
	IndexTuple		   *items;
	IndexTuple			inserted_right;
	IndexTuple			inserted_left;
	int					total;
	int					i;
	Buffer				basebuf;
	Page				basepage;
	BWTreePageOpaque	base_opaque;
	uint16				flags;

	if (new_itup == NULL)
		elog(ERROR, "bwtree: internal insert requires separator tuple");
	if (split_left_child_pid == InvalidBWTreePid)
		elog(ERROR, "bwtree: internal insert requires split-left child PID");

	if (!_bwt_capture_node_snapshot(rel, metad, pid, &snapshot))
		elog(ERROR, "bwtree: internal insert cannot find PID %u",
			 (unsigned int) pid);
	if (snapshot.is_leaf)
		elog(ERROR, "bwtree: internal insert called on leaf PID %u",
			 (unsigned int) pid);

	_bwt_materialize_page(rel, metad, pid, NULL, &mpage);
	total = mpage.nitems + 2;
	items = (IndexTuple *) palloc(sizeof(IndexTuple) * total);
	for (i = 0; i < mpage.nitems; i++)
		items[i] = mpage.items[i];

	inserted_right = _bwt_copy_itup(new_itup);
	inserted_left = _bwt_copy_itup(new_itup);
	BWTreeTupleSetDownLink(inserted_left, split_left_child_pid);

	/*
	 * Correctness-first representation:
	 * for each split boundary key K, insert two tuples:
	 *   (K -> left_child), (K -> right_child)
	 * This matches current search rule (pick last tuple <= key, fallback to
	 * first tuple for key < min key) without needing a separate leftmost
	 * downlink field.
	 */
	items[total - 2] = inserted_left;
	items[total - 1] = inserted_right;
	_bwt_sort_tuples(rel, items, total);

	flags = snapshot.is_root ? BWT_ROOT : 0;

	if (!_bwt_page_build_fits(pid, flags, snapshot.level,
							  mpage.prev_blkno, mpage.next_blkno,
							  items, total))
	{
		/*
		 * Recursive split propagation:
		 * if parent page overflows, split it and push separator upward.
		 */
		_bwt_split_internal_from_items(rel, metad, &snapshot, &mpage,
									   items, total, InvalidBWTreePid);
		pfree(inserted_left);
		pfree(inserted_right);
		pfree(items);
		_bwt_free_materialized_page(&mpage);
		return;
	}

	basebuf = _bwt_getbuf(rel, snapshot.base_blkno, BWT_WRITE);
	basepage = BufferGetPage(basebuf);

	/*
	 * Correctness-first trade-off:
	 *
	 * We rewrite the parent base page in-place and clear its delta head
	 * afterwards. This is simpler than delta-publishing separators and avoids
	 * relying on incomplete separator-delta replay logic.
	 */
	_bwt_initpage(basepage, flags, pid, snapshot.level);
	base_opaque = BWTreePageGetOpaque(basepage);
	base_opaque->bwto_prev = mpage.prev_blkno;
	base_opaque->bwto_next = mpage.next_blkno;
	_bwt_fill_page_with_items(basepage, items, total);
	MarkBufferDirty(basebuf);
	_bwt_relbuf(rel, basebuf, BWT_WRITE);

	_bwt_map_update(rel, metad, pid, snapshot.base_blkno, InvalidBlockNumber);

	pfree(inserted_left);
	pfree(inserted_right);
	pfree(items);
	_bwt_free_materialized_page(&mpage);
}

bool
_bwt_prepare_split(Relation rel, BWTreeMetaPageData *metad,
				   BWTreePid pid, BWTreeNodeView *view)
{
	if (view == NULL)
		elog(ERROR, "bwtree: prepare-split requires node view output");

	_bwt_materialize_node(rel, metad, pid, view);
	if (!view->snapshot.is_leaf)
		return false;

	return (view->page.nitems >= BWTREE_LEAF_SPLIT_THRESHOLD);
}

void
_bwt_finish_split(Relation rel, BWTreeMetaPageData *metad,
				  BWTreePid pid, BWTreePid parent_pid,
				  BWTreeNodeView *view)
{
	(void) view;
	_bwt_split(rel, metad, InvalidBuffer, pid, parent_pid);
}

void
_bwt_install_new_root(Relation rel, BWTreeMetaPageData *metad,
					  BWTreePid left_pid, BWTreePid right_pid,
					  IndexTuple sep_itup, uint32 child_level)
{
	Buffer		metabuf;
	Buffer		rootbuf;
	Page		rootpage;
	BWTreePid	root_pid;
	IndexTuple	left_itup;
	IndexTuple	right_itup;

	if (sep_itup == NULL)
		elog(ERROR, "bwtree: install-new-root requires separator tuple");

	/*
	 * Caller is expected to already hold metapage write lock; here we only
	 * take a pin to pass the buffer identity into mapping-table helpers.
	 */
	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_NOLOCK);
	rootbuf = _bwt_allocbuf(rel);
	rootpage = BufferGetPage(rootbuf);

	root_pid = _bwt_map_alloc_pid(rel, metad, metabuf,
								  BufferGetBlockNumber(rootbuf),
								  InvalidBlockNumber);
	_bwt_initpage(rootpage, BWT_ROOT, root_pid, child_level + 1);

	left_itup = _bwt_copy_itup(sep_itup);
	right_itup = _bwt_copy_itup(sep_itup);
	BWTreeTupleSetDownLink(left_itup, left_pid);
	BWTreeTupleSetDownLink(right_itup, right_pid);
	/*
	 * Correctness-first trade-off:
	 * root stores two pivot tuples with the same separator key and different
	 * downlinks. Routing logic interprets this as:
	 *   key < sep  -> left
	 *   key >= sep -> right
	 */

	_bwt_fill_page_with_items(rootpage, &left_itup, 1);
	_bwt_fill_page_with_items(rootpage, &right_itup, 1);
	MarkBufferDirty(rootbuf);

	metad->bwt_root_pid = root_pid;
	metad->bwt_level = child_level + 1;
	MarkBufferDirty(metabuf);

	pfree(left_itup);
	pfree(right_itup);
	_bwt_relbuf(rel, rootbuf, BWT_WRITE);
	_bwt_relbuf(rel, metabuf, BWT_NOLOCK);
}

void
_bwt_insert_item(Relation rel, BWTreeMetaPageData *metad,
				 BWTreePid pid, BWTreePid parent_pid,
				 IndexTuple itup, bool is_leaf)
{
	BWTreeNodeView view;
	bool		   need_split;

	if (itup == NULL)
		elog(ERROR, "bwtree: insert-item requires index tuple");

	if (!is_leaf)
	{
		/*
		 * For internal insertion path, parent_pid carries the split-left child
		 * PID of the lower-level page that just split.
		 */
		_bwt_insert_into_internal(rel, metad, pid, parent_pid, itup);
		return;
	}

	_bwt_delta_install(rel, metad, pid, BW_DELTA_INSERT, itup, InvalidBWTreePid);

	if (_bwt_should_consolidate(rel, metad, pid))
		_bwt_consolidate(rel, metad, pid);

	need_split = _bwt_prepare_split(rel, metad, pid, &view);
	if (need_split)
		_bwt_finish_split(rel, metad, pid, parent_pid, &view);
	_bwt_free_node_view(&view);
}

void
_bwt_split(Relation rel, BWTreeMetaPageData *metad,
		   Buffer leafbuf, BWTreePid leaf_pid,
		   BWTreePid parent_pid)
{
	BWTreeNodeSnapshot	snapshot;
	BWTMaterializedPage mpage;
	int					split_at;
	Buffer				metabuf;
	Buffer				leftbuf;
	Buffer				rightbuf;
	Page				leftpage;
	Page				rightpage;
	BWTreePageOpaque	left_opaque;
	BWTreePageOpaque	right_opaque;
	BWTreePid			right_pid;
	BlockNumber			right_blkno;
	IndexTuple			sep_itup;

	(void) leafbuf;

	if (!_bwt_capture_node_snapshot(rel, metad, leaf_pid, &snapshot))
		elog(ERROR, "bwtree: split cannot find leaf PID %u",
			 (unsigned int) leaf_pid);
	if (!snapshot.is_leaf)
		elog(ERROR, "bwtree: split target PID %u is not a leaf",
			 (unsigned int) leaf_pid);

	_bwt_materialize_page(rel, metad, leaf_pid, NULL, &mpage);
	if (mpage.nitems < 2)
	{
		_bwt_free_materialized_page(&mpage);
		return;
	}

	_bwt_sort_tuples(rel, mpage.items, mpage.nitems);
	split_at = mpage.nitems / 2;
	if (split_at <= 0)
		split_at = 1;
	if (split_at >= mpage.nitems)
		split_at = mpage.nitems - 1;

	/*
	 * Caller is expected to already hold metapage write lock; we pin only.
	 */
	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_NOLOCK);

	leftbuf = _bwt_getbuf(rel, snapshot.base_blkno, BWT_WRITE);
	rightbuf = _bwt_allocbuf(rel);
	right_blkno = BufferGetBlockNumber(rightbuf);
	right_pid = _bwt_map_alloc_pid(rel, metad, metabuf,
								   right_blkno, InvalidBlockNumber);

	leftpage = BufferGetPage(leftbuf);
	rightpage = BufferGetPage(rightbuf);
	_bwt_initpage(leftpage, BWT_LEAF, leaf_pid, snapshot.level);
	_bwt_initpage(rightpage, BWT_LEAF, right_pid, snapshot.level);

	left_opaque = BWTreePageGetOpaque(leftpage);
	right_opaque = BWTreePageGetOpaque(rightpage);
	left_opaque->bwto_prev = mpage.prev_blkno;
	left_opaque->bwto_next = right_blkno;
	right_opaque->bwto_prev = snapshot.base_blkno;
	right_opaque->bwto_next = mpage.next_blkno;

	_bwt_fill_page_with_items(leftpage, mpage.items, split_at);
	_bwt_fill_page_with_items(rightpage, &mpage.items[split_at],
							  mpage.nitems - split_at);

	MarkBufferDirty(leftbuf);
	MarkBufferDirty(rightbuf);
	_bwt_relbuf(rel, leftbuf, BWT_WRITE);
	_bwt_relbuf(rel, rightbuf, BWT_WRITE);

	_bwt_map_update(rel, metad, leaf_pid, snapshot.base_blkno, InvalidBlockNumber);

	if (BlockNumberIsValid(mpage.next_blkno))
	{
		Buffer			nextbuf;
		Page			nextpage;
		BWTreePageOpaque next_opaque;

		nextbuf = _bwt_getbuf(rel, mpage.next_blkno, BWT_WRITE);
		nextpage = BufferGetPage(nextbuf);
		next_opaque = BWTreePageGetOpaque(nextpage);
		if (next_opaque->bwto_page_id == BWTREE_PAGE_ID)
		{
			next_opaque->bwto_prev = right_blkno;
			MarkBufferDirty(nextbuf);
		}
		_bwt_relbuf(rel, nextbuf, BWT_WRITE);
	}

	sep_itup = _bwt_copy_itup(mpage.items[split_at]);
	BWTreeTupleSetDownLink(sep_itup, right_pid);
	_bwt_publish_split_metadata(rel, metad, leaf_pid, right_pid, sep_itup);

	if (parent_pid == InvalidBWTreePid)
		_bwt_install_new_root(rel, metad, leaf_pid, right_pid, sep_itup,
							  snapshot.level);
	else
	{
		/*
		 * Correctness-first trade-off:
		 *
		 * Parent update is currently performed via full parent-base rewrite
		 * (through _bwt_insert_item with is_leaf=false), not separator delta.
		 */
		_bwt_insert_item(rel, metad, parent_pid, leaf_pid,
						 sep_itup, false);
	}

	pfree(sep_itup);
	_bwt_free_materialized_page(&mpage);
	_bwt_relbuf(rel, metabuf, BWT_NOLOCK);
}
