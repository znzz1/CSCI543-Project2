/*-------------------------------------------------------------------------
 *
 * bwtreedelta.c
 *    Delta/materialization skeletons for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufpage.h"

typedef struct BWTreeDeltaApplyEntry
{
	BWTreeDeltaRecordData *drec;
} BWTreeDeltaApplyEntry;

static bool
_bwt_delete_tid_from_page(Page page, const ItemPointerData *target_tid)
{
	OffsetNumber	maxoff;
	OffsetNumber	off;
	ItemPointerData	target_tid_local;

	if (target_tid == NULL)
		return false;

	target_tid_local = *target_tid;

	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
	{
		ItemId		itemid;
		IndexTuple	itup;

		itemid = PageGetItemId(page, off);
		if (!ItemIdIsUsed(itemid))
			continue;

		itup = (IndexTuple) PageGetItem(page, itemid);
		if (ItemPointerEquals(&itup->t_tid, &target_tid_local))
		{
			PageIndexTupleDelete(page, off);
			return true;
		}
	}

	return false;
}

static void
_bwt_collect_page_items(Page page, BWTMaterializedPage *mpage)
{
	OffsetNumber	maxoff;
	OffsetNumber	off;
	int				capacity;

	maxoff = PageGetMaxOffsetNumber(page);
	capacity = (int) maxoff;

	if (capacity <= 0)
	{
		mpage->items = NULL;
		mpage->nitems = 0;
		return;
	}

	/*
	 * Correctness-first trade-off:
	 *
	 * Materialization deep-copies tuples into palloc memory to avoid exposing
	 * buffer-backed tuple pointers to callers. This is safer but uses more
	 * CPU and memory than zero-copy views.
	 */
	mpage->items = (IndexTuple *) palloc0(sizeof(IndexTuple) * capacity);
	mpage->nitems = 0;

	for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
	{
		ItemId		itemid;
		IndexTuple	src;
		IndexTuple	dst;
		Size		sz;

		itemid = PageGetItemId(page, off);
		if (!ItemIdIsUsed(itemid))
			continue;

		src = (IndexTuple) PageGetItem(page, itemid);
		sz = IndexTupleSize(src);
		dst = (IndexTuple) palloc(sz);
		memcpy(dst, src, sz);
		mpage->items[mpage->nitems++] = dst;
	}
}

static void
_bwt_materialize_from_snapshot(Relation rel,
							   const BWTreeNodeSnapshot *snapshot,
							   BWTMaterializedPage *mpage)
{
	Buffer				basebuf;
	Page				basepage;
	Page				workpage;
	BWTreePageOpaque	work_opaque;

	basebuf = _bwt_getbuf(rel, snapshot->base_blkno, BWT_READ);
	basepage = BufferGetPage(basebuf);
	/*
	 * Correctness-first trade-off:
	 *
	 * Always materialize on a private page copy. We never mutate the shared
	 * base buffer page directly in this stage.
	 */
	workpage = (Page) palloc(BLCKSZ);
	memcpy(workpage, basepage, BLCKSZ);

	if (BlockNumberIsValid(snapshot->delta_blkno))
		_bwt_delta_apply(rel, snapshot->delta_blkno, workpage, NULL);

	work_opaque = BWTreePageGetOpaque(workpage);
	mpage->base_blkno = snapshot->base_blkno;
	mpage->prev_blkno = work_opaque->bwto_prev;
	mpage->next_blkno = work_opaque->bwto_next;
	mpage->flags = work_opaque->bwto_flags;
	mpage->level = work_opaque->bwto_level;

	_bwt_collect_page_items(workpage, mpage);
	pfree(workpage);
	_bwt_relbuf(rel, basebuf, BWT_READ);
}

BWTreeNodeKind
_bwt_classify_node(Page page)
{
	BWTreePageOpaque		opaque;
	OffsetNumber			maxoff;
	ItemId					itemid;
	BWTreeDeltaRecordData  *drec;

	if (page == NULL)
		elog(ERROR, "bwtree: classify-node requires a valid page");

	opaque = BWTreePageGetOpaque(page);
	if (opaque->bwto_page_id != BWTREE_PAGE_ID)
		elog(ERROR, "bwtree: cannot classify page with invalid page id");

	if (!BWTreePageIsDelta(opaque))
		return BWTreePageIsLeaf(opaque) ? BWT_NODE_BASE_LEAF : BWT_NODE_BASE_INNER;

	maxoff = PageGetMaxOffsetNumber(page);
	if (maxoff < FirstOffsetNumber)
		elog(ERROR, "bwtree: delta page has no record to classify");

	itemid = PageGetItemId(page, FirstOffsetNumber);
	if (!ItemIdIsUsed(itemid))
		elog(ERROR, "bwtree: delta page first item is invalid");

	drec = (BWTreeDeltaRecordData *) PageGetItem(page, itemid);
	switch (drec->type)
	{
		case BW_DELTA_INSERT:
			return BWT_NODE_DELTA_INSERT;
		case BW_DELTA_DELETE:
			return BWT_NODE_DELTA_DELETE;
		case BW_DELTA_SPLIT:
			return BWT_NODE_DELTA_SPLIT;
		case BW_DELTA_SEPARATOR:
			return BWT_NODE_DELTA_SEPARATOR;
	}

	elog(ERROR, "bwtree: unknown delta record type %d", (int) drec->type);
	return BWT_NODE_DELTA_INSERT;
}

void
_bwt_delta_install(Relation rel, BWTreeMetaPageData *metad,
				   BWTreePid pid, BWTreeDeltaType type,
				   IndexTuple itup, BWTreePid related_pid)
{
	int				map_page_idx;
	int				entry_idx;
	BlockNumber		map_blkno;
	Buffer			mapbuf;
	Page			mappage;
	BWTreeMapEntry *entries;
	BlockNumber		base_blkno;
	BlockNumber		old_delta_blkno;
	Buffer			basebuf;
	Page			basepage;
	BWTreePageOpaque base_opaque;
	Buffer			deltabuf;
	Page			deltapage;
	BWTreePageOpaque delta_opaque;
	BWTreeDeltaRecordData *drec;
	char		   *item_data;
	Size			data_len;
	Size			item_size;
	OffsetNumber	offnum;
	uint16			flags = BWT_DELTA;

	if (pid >= metad->bwt_next_pid)
		elog(ERROR, "bwtree: cannot install delta on unknown PID %u",
			 (unsigned int) pid);

	map_page_idx = pid / BWTREE_MAP_ENTRIES_PER_PAGE;
	entry_idx = pid % BWTREE_MAP_ENTRIES_PER_PAGE;
	if (map_page_idx >= (int) metad->bwt_num_map_pages)
		elog(ERROR, "bwtree: PID %u map page %d does not exist",
			 (unsigned int) pid, map_page_idx);

	map_blkno = metad->bwt_map_blknos[map_page_idx];
	if (!BlockNumberIsValid(map_blkno))
		elog(ERROR, "bwtree: map page %d has invalid block number",
			 map_page_idx);

	/*
	 * Hold the mapping entry write lock while reading old delta head and
	 * publishing the new head, so concurrent installers cannot overwrite each
	 * other's link updates.
	 *
	 * Correctness-first trade-off:
	 *   Use coarse mapping-page latches instead of lock-free CAS publish.
	 *   This is simpler/safer now, but lowers update concurrency.
	 */
	mapbuf = _bwt_getbuf(rel, map_blkno, BWT_WRITE);
	mappage = BufferGetPage(mapbuf);
	entries = BWTreeMapPageGetEntries(mappage);
	base_blkno = entries[entry_idx].base_blkno;
	old_delta_blkno = entries[entry_idx].delta_blkno;

	if (!BlockNumberIsValid(base_blkno))
		elog(ERROR, "bwtree: PID %u has invalid base block number",
			 (unsigned int) pid);

	basebuf = _bwt_getbuf(rel, base_blkno, BWT_READ);
	basepage = BufferGetPage(basebuf);
	base_opaque = BWTreePageGetOpaque(basepage);

	if (BWTreePageIsLeaf(base_opaque))
		flags |= BWT_LEAF;
	if (BWTreePageIsRoot(base_opaque))
		flags |= BWT_ROOT;

	data_len = (itup != NULL) ? IndexTupleSize(itup) : 0;
	item_size = SizeOfBWTreeDeltaRecord + data_len;
	item_data = (char *) palloc0(item_size);
	drec = (BWTreeDeltaRecordData *) item_data;

	drec->type = type;
	drec->related_pid = related_pid;
	drec->data_len = (uint16) data_len;
	if (type == BW_DELTA_DELETE && itup != NULL)
		drec->target_tid = itup->t_tid;
	else
		ItemPointerSetInvalid(&drec->target_tid);

	if (data_len > 0)
		memcpy(item_data + SizeOfBWTreeDeltaRecord, itup, data_len);

	deltabuf = _bwt_allocbuf(rel);
	deltapage = BufferGetPage(deltabuf);
	_bwt_initpage(deltapage, flags, pid, base_opaque->bwto_level);
	delta_opaque = BWTreePageGetOpaque(deltapage);
	delta_opaque->bwto_next = old_delta_blkno;

	offnum = PageAddItem(deltapage, (Item) item_data, item_size,
						 InvalidOffsetNumber, false, false);
	if (offnum == InvalidOffsetNumber)
		elog(ERROR, "bwtree: failed to add delta record for PID %u",
			 (unsigned int) pid);

	MarkBufferDirty(deltabuf);
	_bwt_relbuf(rel, basebuf, BWT_READ);

	entries[entry_idx].base_blkno = base_blkno;
	entries[entry_idx].delta_blkno = BufferGetBlockNumber(deltabuf);
	MarkBufferDirty(mapbuf);
	_bwt_relbuf(rel, mapbuf, BWT_WRITE);
	_bwt_relbuf(rel, deltabuf, BWT_WRITE);

	pfree(item_data);
}

int
_bwt_delta_apply(Relation rel, BlockNumber delta_blkno,
				 Page base_page, OffsetNumber *maxoff)
{
	/*
	 * IMPORTANT: base_page must be a writable working copy. Callers must not
	 * pass the shared base buffer page directly, because this routine applies
	 * deltas in place.
	 *
	 * Correctness-first trade-off:
	 *   We copy every delta record before releasing its page latch, then apply
	 *   from the copied list. This avoids dangling page pointers but costs
	 *   extra memory and CPU.
	 */
	BlockNumber				cur_blkno;
	BWTreeDeltaApplyEntry   *entries = NULL;
	int						nentries = 0;
	int						capacity = 0;
	int						i;
	int						applied = 0;

	if (base_page == NULL)
		elog(ERROR, "bwtree: delta-apply requires a valid base page");

	cur_blkno = delta_blkno;
	while (BlockNumberIsValid(cur_blkno))
	{
		Buffer					buf;
		Page					page;
		BWTreePageOpaque		opaque;
		OffsetNumber			page_maxoff;
		ItemId					itemid;
		BWTreeDeltaRecordData  *drec;
		Size					rec_size;

		buf = _bwt_getbuf(rel, cur_blkno, BWT_READ);
		page = BufferGetPage(buf);
		opaque = BWTreePageGetOpaque(page);
		if (opaque->bwto_page_id != BWTREE_PAGE_ID || !BWTreePageIsDelta(opaque))
		{
			_bwt_relbuf(rel, buf, BWT_READ);
			elog(ERROR, "bwtree: block %u is not a valid delta page",
				 (unsigned int) cur_blkno);
		}
		page_maxoff = PageGetMaxOffsetNumber(page);

		if (page_maxoff < FirstOffsetNumber)
		{
			_bwt_relbuf(rel, buf, BWT_READ);
			elog(ERROR, "bwtree: delta page %u contains no records",
				 cur_blkno);
		}

		itemid = PageGetItemId(page, FirstOffsetNumber);
		if (!ItemIdIsUsed(itemid))
		{
			_bwt_relbuf(rel, buf, BWT_READ);
			elog(ERROR, "bwtree: delta page %u has invalid first item",
				 cur_blkno);
		}

		drec = (BWTreeDeltaRecordData *) PageGetItem(page, itemid);
		rec_size = BWTreeDeltaRecordSize(drec);

		if (nentries == capacity)
		{
			capacity = (capacity == 0) ? 8 : capacity * 2;
			if (entries == NULL)
				entries = (BWTreeDeltaApplyEntry *)
					palloc(capacity * sizeof(BWTreeDeltaApplyEntry));
			else
				entries = (BWTreeDeltaApplyEntry *)
					repalloc(entries, capacity * sizeof(BWTreeDeltaApplyEntry));
		}

		entries[nentries].drec = (BWTreeDeltaRecordData *) palloc(rec_size);
		memcpy(entries[nentries].drec, drec, rec_size);
		nentries++;

		cur_blkno = opaque->bwto_next;
		_bwt_relbuf(rel, buf, BWT_READ);
	}

	for (i = nentries - 1; i >= 0; i--)
	{
		BWTreeDeltaRecordData *drec = entries[i].drec;

		switch (drec->type)
		{
			case BW_DELTA_INSERT:
			{
				IndexTuple	itup = BWTreeDeltaRecordGetTuple(drec);
				OffsetNumber off;

				if (itup == NULL)
					break;

				off = PageAddItem(base_page, (Item) itup, drec->data_len,
								  InvalidOffsetNumber, false, false);
				if (off == InvalidOffsetNumber)
					elog(ERROR, "bwtree: failed to apply INSERT delta to base page");
				applied++;
				break;
			}
			case BW_DELTA_DELETE:
			{
				if (ItemPointerIsValid(&drec->target_tid) &&
					_bwt_delete_tid_from_page(base_page, &drec->target_tid))
					applied++;
				break;
			}
			case BW_DELTA_SPLIT:
			case BW_DELTA_SEPARATOR:
				/*
				 * Split/separator deltas are control-plane metadata and do not
				 * directly add/remove tuples on the base page.
				 *
				 * Correctness-first note:
				 * consolidation is disabled in this project stage, so these
				 * structural deltas remain in-chain and are not dropped by
				 * materialization-only paths.
				 */
				break;
		}
	}

	if (maxoff != NULL)
		*maxoff = PageGetMaxOffsetNumber(base_page);

	for (i = 0; i < nentries; i++)
		pfree(entries[i].drec);
	if (entries != NULL)
		pfree(entries);

	return applied;
}

bool
_bwt_capture_node_snapshot(Relation rel, BWTreeMetaPageData *metad,
						   BWTreePid pid,
						   BWTreeNodeSnapshot *snapshot)
{
	BlockNumber		base_blkno;
	BlockNumber		delta_blkno;
	Buffer			basebuf;
	Page			basepage;
	BWTreePageOpaque base_opaque;

	if (snapshot == NULL)
		elog(ERROR, "bwtree: snapshot output pointer must not be NULL");

	if (!_bwt_map_lookup(rel, metad, pid, &base_blkno, &delta_blkno))
		return false;

	if (!BlockNumberIsValid(base_blkno))
		elog(ERROR, "bwtree: PID %u has invalid base block number",
			 (unsigned int) pid);

	basebuf = _bwt_getbuf(rel, base_blkno, BWT_READ);
	basepage = BufferGetPage(basebuf);
	base_opaque = BWTreePageGetOpaque(basepage);

	if (base_opaque->bwto_page_id != BWTREE_PAGE_ID)
	{
		_bwt_relbuf(rel, basebuf, BWT_READ);
		elog(ERROR, "bwtree: PID %u base block %u has invalid page id",
			 (unsigned int) pid, (unsigned int) base_blkno);
	}

	if (base_opaque->bwto_pid != pid)
	{
		_bwt_relbuf(rel, basebuf, BWT_READ);
		elog(ERROR, "bwtree: PID %u base block %u belongs to PID %u",
			 (unsigned int) pid,
			 (unsigned int) base_blkno,
			 (unsigned int) base_opaque->bwto_pid);
	}

	snapshot->pid = pid;
	snapshot->base_blkno = base_blkno;
	snapshot->delta_blkno = delta_blkno;
	snapshot->level = base_opaque->bwto_level;
	snapshot->is_leaf = BWTreePageIsLeaf(base_opaque);
	snapshot->is_root = BWTreePageIsRoot(base_opaque);

	_bwt_relbuf(rel, basebuf, BWT_READ);
	return true;
}

void
_bwt_materialize_page(Relation rel, BWTreeMetaPageData *metad,
					  BWTreePid pid, Buffer *basebuf_out,
					  BWTMaterializedPage *mpage)
{
	BWTreeNodeSnapshot	snapshot;

	if (mpage == NULL)
		elog(ERROR, "bwtree: materialize-page output pointer must not be NULL");

	memset(mpage, 0, sizeof(*mpage));
	/*
	 * Correctness-first trade-off:
	 *
	 * We intentionally do not return a pinned base buffer for caller-side
	 * in-place edits in this stage. basebuf_out is always InvalidBuffer to
	 * keep all writes funneled through explicit update paths.
	 */
	if (basebuf_out != NULL)
		*basebuf_out = InvalidBuffer;

	if (!_bwt_capture_node_snapshot(rel, metad, pid, &snapshot))
		elog(ERROR, "bwtree: cannot materialize unknown PID %u",
			 (unsigned int) pid);

	_bwt_materialize_from_snapshot(rel, &snapshot, mpage);
	if (basebuf_out != NULL)
		*basebuf_out = InvalidBuffer;
}

void
_bwt_free_materialized_page(BWTMaterializedPage *mpage)
{
	int	i;

	if (mpage == NULL)
		return;

	if (mpage->items != NULL)
	{
		for (i = 0; i < mpage->nitems; i++)
		{
			if (mpage->items[i] != NULL)
				pfree(mpage->items[i]);
		}
		pfree(mpage->items);
	}

	memset(mpage, 0, sizeof(*mpage));
}

void
_bwt_materialize_node(Relation rel, BWTreeMetaPageData *metad,
					  BWTreePid pid, BWTreeNodeView *view)
{
	BlockNumber				cur_blkno;

	if (view == NULL)
		elog(ERROR, "bwtree: materialize-node output pointer must not be NULL");

	memset(view, 0, sizeof(*view));
	view->split_right_pid = InvalidBWTreePid;
	view->state = BWT_STATE_STABLE;

	if (!_bwt_capture_node_snapshot(rel, metad, pid, &view->snapshot))
		elog(ERROR, "bwtree: cannot materialize unknown PID %u",
			 (unsigned int) pid);
	_bwt_materialize_from_snapshot(rel, &view->snapshot, &view->page);

	cur_blkno = view->snapshot.delta_blkno;
	while (BlockNumberIsValid(cur_blkno))
	{
		Buffer					buf;
		Page					page;
		BWTreePageOpaque		opaque;
		OffsetNumber			maxoff;
		ItemId					itemid;
		BWTreeDeltaRecordData  *drec;

		buf = _bwt_getbuf(rel, cur_blkno, BWT_READ);
		page = BufferGetPage(buf);
		opaque = BWTreePageGetOpaque(page);
		if (opaque->bwto_page_id != BWTREE_PAGE_ID || !BWTreePageIsDelta(opaque))
		{
			_bwt_relbuf(rel, buf, BWT_READ);
			elog(ERROR, "bwtree: block %u is not a valid delta page",
				 (unsigned int) cur_blkno);
		}

		maxoff = PageGetMaxOffsetNumber(page);
		if (maxoff >= FirstOffsetNumber)
		{
			itemid = PageGetItemId(page, FirstOffsetNumber);
			if (ItemIdIsUsed(itemid))
			{
				drec = (BWTreeDeltaRecordData *) PageGetItem(page, itemid);
				if (!view->has_split_delta && drec->type == BW_DELTA_SPLIT)
				{
					view->has_split_delta = true;
					view->split_right_pid = drec->related_pid;
					view->state = BWT_STATE_SPLIT_PENDING;
				}
				if (view->split_separator == NULL &&
					drec->type == BW_DELTA_SEPARATOR &&
					drec->data_len > 0)
				{
					IndexTuple	sep_itup;

					sep_itup = BWTreeDeltaRecordGetTuple(drec);
					if (sep_itup == NULL)
					{
						_bwt_relbuf(rel, buf, BWT_READ);
						elog(ERROR, "bwtree: separator delta has NULL tuple payload");
					}

					view->split_separator = (IndexTuple) palloc(drec->data_len);
					memcpy(view->split_separator,
						   sep_itup,
						   drec->data_len);
				}
			}
		}

		cur_blkno = opaque->bwto_next;
		_bwt_relbuf(rel, buf, BWT_READ);
	}
}

void
_bwt_free_node_view(BWTreeNodeView *view)
{
	if (view == NULL)
		return;

	_bwt_free_materialized_page(&view->page);
	if (view->split_separator != NULL)
		pfree(view->split_separator);

	memset(view, 0, sizeof(*view));
	view->split_right_pid = InvalidBWTreePid;
	view->state = BWT_STATE_STABLE;
}

bool
_bwt_should_consolidate(Relation rel, BWTreeMetaPageData *metad,
						BWTreePid pid)
{
	/*
	 * Correctness-first policy for the course deliverable:
	 *
	 * We intentionally disable consolidation for now. The current project
	 * stage prioritizes concurrent correctness over space efficiency, and
	 * avoids publishing a consolidated base while split/separator semantics
	 * and garbage reclamation are still incomplete.
	 *
	 * Trade-off:
	 *   - Simpler and safer behavior under concurrency
	 *   - Larger delta chains, more space usage, and extra read amplification
	 */
	(void) rel;
	(void) metad;
	(void) pid;
	return false;
}

void
_bwt_consolidate(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid)
{
	/*
	 * See _bwt_should_consolidate(): consolidation is intentionally disabled
	 * at this stage to preserve correctness while SMO semantics and GC are
	 * not fully implemented.
	 */
	(void) rel;
	(void) metad;
	(void) pid;
}
