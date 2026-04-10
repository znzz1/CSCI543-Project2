#include "postgres.h"

#include <limits.h>

#include "access/bwtree.h"
#include "storage/indexfsm.h"
#include "storage/bufpage.h"

typedef struct BWTreeDeltaApplyEntry
{
	BWTreeDeltaRecordData *drec;
} BWTreeDeltaApplyEntry;

static bool
_bwt_relink_neighbors_on_base_swap(Relation rel,
								   BlockNumber old_base_blkno,
								   BlockNumber new_base_blkno,
								   BlockNumber prev_blkno,
								   BlockNumber next_blkno)
{
	bool old_detached = true;

	if (!BlockNumberIsValid(old_base_blkno) ||
		!BlockNumberIsValid(new_base_blkno))
		return false;

	if (BlockNumberIsValid(prev_blkno))
	{
		Buffer			prevbuf;
		Page			prevpage;
		BWTreePageOpaque prev_opaque;

		prevbuf = _bwt_getbuf(rel, prev_blkno, BWT_WRITE);
		prevpage = BufferGetPage(prevbuf);
		prev_opaque = BWTreePageGetOpaque(prevpage);

		if (prev_opaque->bwto_page_id != BWTREE_PAGE_ID ||
			BWTreePageIsDelta(prev_opaque))
			old_detached = false;
		else if (prev_opaque->bwto_next == old_base_blkno)
		{
			prev_opaque->bwto_next = new_base_blkno;
			MarkBufferDirty(prevbuf);
		}
		else if (prev_opaque->bwto_next != new_base_blkno)
			old_detached = false;

		_bwt_relbuf(rel, prevbuf, BWT_WRITE);
	}

	if (BlockNumberIsValid(next_blkno))
	{
		Buffer			nextbuf;
		Page			nextpage;
		BWTreePageOpaque next_opaque;

		nextbuf = _bwt_getbuf(rel, next_blkno, BWT_WRITE);
		nextpage = BufferGetPage(nextbuf);
		next_opaque = BWTreePageGetOpaque(nextpage);

		if (next_opaque->bwto_page_id != BWTREE_PAGE_ID ||
			BWTreePageIsDelta(next_opaque))
			old_detached = false;
		else if (next_opaque->bwto_prev == old_base_blkno)
		{
			next_opaque->bwto_prev = new_base_blkno;
			MarkBufferDirty(nextbuf);
		}
		else if (next_opaque->bwto_prev != new_base_blkno)
			old_detached = false;

		_bwt_relbuf(rel, nextbuf, BWT_WRITE);
	}

	return old_detached;
}

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
_bwt_materialized_reserve_items(BWTMaterializedPage *mpage, int *capacity, int needed)
{
	int	newcap;

	if (mpage == NULL || capacity == NULL)
		elog(ERROR, "bwtree: materialized reserve requires valid pointers");
	if (needed <= *capacity)
		return;

	newcap = (*capacity > 0) ? *capacity : 16;
	while (newcap < needed)
	{
		if (newcap > (INT_MAX / 2))
			elog(ERROR, "bwtree: materialized item capacity overflow");
		newcap *= 2;
	}

	if (mpage->items == NULL)
		mpage->items = (IndexTuple *) palloc(sizeof(IndexTuple) * newcap);
	else
		mpage->items = (IndexTuple *) repalloc(mpage->items,
											   sizeof(IndexTuple) * newcap);
	*capacity = newcap;
}

static void
_bwt_materialized_append_tuple_copy(BWTMaterializedPage *mpage,
									int *capacity,
									IndexTuple src)
{
	Size		sz;
	IndexTuple	dst;

	if (mpage == NULL || capacity == NULL)
		elog(ERROR, "bwtree: materialized append requires valid pointers");
	if (src == NULL)
		return;

	_bwt_materialized_reserve_items(mpage, capacity, mpage->nitems + 1);

	sz = IndexTupleSize(src);
	dst = (IndexTuple) palloc(sz);
	memcpy(dst, src, sz);
	mpage->items[mpage->nitems++] = dst;
}

static bool
_bwt_materialized_delete_tid(BWTMaterializedPage *mpage,
							 const ItemPointerData *target_tid)
{
	int				i;
	ItemPointerData target_tid_local;

	if (mpage == NULL || target_tid == NULL)
		return false;

	target_tid_local = *target_tid;
	for (i = 0; i < mpage->nitems; i++)
	{
		IndexTuple itup = mpage->items[i];

		if (itup == NULL)
			continue;
		if (ItemPointerEquals(&itup->t_tid, &target_tid_local))
		{
			pfree(itup);
			if (i + 1 < mpage->nitems)
				memmove(&mpage->items[i],
						&mpage->items[i + 1],
						sizeof(IndexTuple) * (mpage->nitems - i - 1));
			mpage->nitems--;
			return true;
		}
	}

	return false;
}

static void
_bwt_collect_base_tuples(Page page, BWTMaterializedPage *mpage, int *capacity)
{
	OffsetNumber maxoff;
	OffsetNumber off;

	if (page == NULL || mpage == NULL || capacity == NULL)
		elog(ERROR, "bwtree: base tuple collection requires valid inputs");

	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off = OffsetNumberNext(off))
	{
		ItemId		itemid;
		IndexTuple	itup;

		itemid = PageGetItemId(page, off);
		if (!ItemIdIsUsed(itemid))
			continue;

		itup = (IndexTuple) PageGetItem(page, itemid);
		_bwt_materialized_append_tuple_copy(mpage, capacity, itup);
	}
}

static void
_bwt_apply_delta_chain_to_items(Relation rel,
								BlockNumber delta_blkno,
								BWTMaterializedPage *mpage,
								int *capacity)
{
	BlockNumber				cur_blkno;
	BlockNumber				rel_nblocks;
	BlockNumber				hops = 0;
	BlockNumber				hops_limit;
	BWTreeDeltaApplyEntry   *entries = NULL;
	int						nentries = 0;
	int						entry_cap = 0;
	int						i;

	if (rel == NULL || mpage == NULL || capacity == NULL)
		elog(ERROR, "bwtree: delta-chain apply requires valid inputs");

	cur_blkno = delta_blkno;
	rel_nblocks = RelationGetNumberOfBlocks(rel);
	hops_limit = rel_nblocks + 1;

	while (BlockNumberIsValid(cur_blkno))
	{
		Buffer					buf;
		Page					page;
		BWTreePageOpaque		opaque;
		OffsetNumber			page_maxoff;
		ItemId					itemid;
		BWTreeDeltaRecordData  *drec;
		Size					rec_size;

		if (cur_blkno >= rel_nblocks)
			elog(ERROR, "bwtree: delta chain points outside relation (blk=%u nblocks=%u)",
				 (unsigned int) cur_blkno,
				 (unsigned int) rel_nblocks);

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
				 (unsigned int) cur_blkno);
		}

		itemid = PageGetItemId(page, FirstOffsetNumber);
		if (!ItemIdIsUsed(itemid))
		{
			_bwt_relbuf(rel, buf, BWT_READ);
			elog(ERROR, "bwtree: delta page %u has invalid first item",
				 (unsigned int) cur_blkno);
		}

		drec = (BWTreeDeltaRecordData *) PageGetItem(page, itemid);
		rec_size = BWTreeDeltaRecordSize(drec);

		if (nentries == entry_cap)
		{
			entry_cap = (entry_cap == 0) ? 8 : entry_cap * 2;
			if (entries == NULL)
				entries = (BWTreeDeltaApplyEntry *)
					palloc(entry_cap * sizeof(BWTreeDeltaApplyEntry));
			else
				entries = (BWTreeDeltaApplyEntry *)
					repalloc(entries, entry_cap * sizeof(BWTreeDeltaApplyEntry));
		}

		entries[nentries].drec = (BWTreeDeltaRecordData *) palloc(rec_size);
		memcpy(entries[nentries].drec, drec, rec_size);
		nentries++;

		cur_blkno = opaque->bwto_next;
		_bwt_relbuf(rel, buf, BWT_READ);
		hops++;
		if (hops > hops_limit)
			elog(ERROR, "bwtree: delta-chain apply exceeded safety bound (possible cycle)");
	}

	for (i = nentries - 1; i >= 0; i--)
	{
		BWTreeDeltaRecordData *drec = entries[i].drec;

		switch (drec->type)
		{
			case BW_DELTA_INSERT:
			{
				IndexTuple itup = BWTreeDeltaRecordGetTuple(drec);

				if (itup != NULL)
					_bwt_materialized_append_tuple_copy(mpage, capacity, itup);
				break;
			}
			case BW_DELTA_DELETE:
			{
				if (ItemPointerIsValid(&drec->target_tid))
					(void) _bwt_materialized_delete_tid(mpage, &drec->target_tid);
				break;
			}
			case BW_DELTA_SPLIT:
			case BW_DELTA_SEPARATOR:
				break;
		}
	}

	for (i = 0; i < nentries; i++)
		pfree(entries[i].drec);
	if (entries != NULL)
		pfree(entries);
}

static void
_bwt_materialized_build_from_base_and_delta(Relation rel,
											Page basepage,
											const BWTreeNodeSnapshot *snapshot,
											BWTMaterializedPage *mpage)
{
	int					capacity = 0;
	BWTreePageOpaque	base_opaque;

	if (rel == NULL || basepage == NULL || snapshot == NULL || mpage == NULL)
		elog(ERROR, "bwtree: materialized build requires valid inputs");

	mpage->items = NULL;
	mpage->nitems = 0;
	mpage->backing_page = NULL;

	base_opaque = BWTreePageGetOpaque(basepage);
	mpage->base_blkno = snapshot->base_blkno;
	mpage->prev_blkno = base_opaque->bwto_prev;
	mpage->next_blkno = base_opaque->bwto_next;
	mpage->flags = base_opaque->bwto_flags;
	mpage->level = base_opaque->bwto_level;

	_bwt_collect_base_tuples(basepage, mpage, &capacity);

	if (BlockNumberIsValid(snapshot->delta_blkno))
	{
		/*
		 * Correctness-first fix:
		 *
		 * Apply deltas into a dynamic tuple vector instead of a single BLCKSZ
		 * scratch page. This avoids false overflow errors during build/split
		 * materialization when logical tuple count temporarily exceeds one-page
		 * physical capacity prior to split publication.
		 */
		_bwt_apply_delta_chain_to_items(rel, snapshot->delta_blkno, mpage, &capacity);
	}
}

static void
_bwt_materialize_from_snapshot(Relation rel,
							   const BWTreeNodeSnapshot *snapshot,
							   BWTMaterializedPage *mpage)
{
	Buffer				basebuf;
	Page				basepage;

	basebuf = _bwt_getbuf(rel, snapshot->base_blkno, BWT_READ);
	basepage = BufferGetPage(basebuf);
	_bwt_materialized_build_from_base_and_delta(rel, basepage, snapshot, mpage);
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
	Size				data_len;
	Size				item_size;
	char			   *item_data;
	BWTreeDeltaRecordData *drec;
	int					retry_guard = 0;

	if (pid >= metad->bwt_next_pid)
		elog(ERROR, "bwtree: cannot install delta on unknown PID %u",
			 (unsigned int) pid);

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

	/*
	 * Paper-aligned optimistic install:
	 * 1) snapshot current mapping entry
	 * 2) build new delta page
	 * 3) CAS publish as new delta head
	 * 4) on conflict, retry (including base change restart)
	 */
	for (;;)
	{
		BlockNumber			base_blkno;
		BlockNumber			old_delta_blkno;
		Buffer				basebuf;
		Page				basepage;
		BWTreePageOpaque	base_opaque;
		Buffer				deltabuf;
		Page				deltapage;
		BWTreePageOpaque	delta_opaque;
		BlockNumber			new_delta_blkno;
		OffsetNumber		offnum;
		uint16				flags = BWT_DELTA;
		bool				restart = false;

		if (retry_guard++ > 1024)
		{
			pfree(item_data);
			elog(ERROR, "bwtree: delta install exceeded retry bound for PID %u",
				 (unsigned int) pid);
		}

		if (!_bwt_map_lookup(rel, metad, pid, &base_blkno, &old_delta_blkno))
		{
			pfree(item_data);
			elog(ERROR, "bwtree: cannot install delta on unknown PID %u",
				 (unsigned int) pid);
		}
		if (!BlockNumberIsValid(base_blkno))
		{
			pfree(item_data);
			elog(ERROR, "bwtree: PID %u has invalid base block number",
				 (unsigned int) pid);
		}

		basebuf = _bwt_getbuf(rel, base_blkno, BWT_READ);
		basepage = BufferGetPage(basebuf);
		base_opaque = BWTreePageGetOpaque(basepage);
		if (BWTreePageIsLeaf(base_opaque))
			flags |= BWT_LEAF;
		if (BWTreePageIsRoot(base_opaque))
			flags |= BWT_ROOT;

		deltabuf = _bwt_allocbuf(rel);
		deltapage = BufferGetPage(deltabuf);
		_bwt_initpage(deltapage, flags, pid, base_opaque->bwto_level);
		delta_opaque = BWTreePageGetOpaque(deltapage);
		new_delta_blkno = BufferGetBlockNumber(deltabuf);

		offnum = PageAddItem(deltapage, (Item) item_data, item_size,
							 InvalidOffsetNumber, false, false);
		_bwt_relbuf(rel, basebuf, BWT_READ);
		if (offnum == InvalidOffsetNumber)
		{
			_bwt_relbuf(rel, deltabuf, BWT_WRITE);
			RecordFreeIndexPage(rel, new_delta_blkno);
			pfree(item_data);
			elog(ERROR, "bwtree: failed to add delta record for PID %u",
				 (unsigned int) pid);
		}

		for (;;)
		{
			BlockNumber observed_base_blkno = InvalidBlockNumber;
			BlockNumber observed_delta_blkno = InvalidBlockNumber;
			bool		published;

			delta_opaque->bwto_next = old_delta_blkno;
			MarkBufferDirty(deltabuf);

			published = _bwt_map_cas(rel, metad, pid,
									 base_blkno, old_delta_blkno,
									 base_blkno, new_delta_blkno,
									 &observed_base_blkno,
									 &observed_delta_blkno);
			if (published)
			{
				_bwt_relbuf(rel, deltabuf, BWT_WRITE);
				pfree(item_data);
				return;
			}

			if (observed_base_blkno != base_blkno)
			{
				/*
				 * Base changed (e.g. consolidation/split rewrite). Drop this
				 * unpublished delta page and restart from fresh mapping state.
				 */
				_bwt_relbuf(rel, deltabuf, BWT_WRITE);
				RecordFreeIndexPage(rel, new_delta_blkno);
				restart = true;
				break;
			}

			/* Same base, newer delta head: relink and retry CAS publish. */
			old_delta_blkno = observed_delta_blkno;
		}

		if (restart)
			continue;
	}
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
	BlockNumber				rel_nblocks;
	BlockNumber				hops = 0;
	BlockNumber				hops_limit;
	BWTreeDeltaApplyEntry   *entries = NULL;
	int						nentries = 0;
	int						capacity = 0;
	int						i;
	int						applied = 0;

	if (base_page == NULL)
		elog(ERROR, "bwtree: delta-apply requires a valid base page");

	cur_blkno = delta_blkno;
	rel_nblocks = RelationGetNumberOfBlocks(rel);
	hops_limit = rel_nblocks + 1;
	while (BlockNumberIsValid(cur_blkno))
	{
		Buffer					buf;
		Page					page;
		BWTreePageOpaque		opaque;
		OffsetNumber			page_maxoff;
		ItemId					itemid;
		BWTreeDeltaRecordData  *drec;
		Size					rec_size;

		if (cur_blkno >= rel_nblocks)
			elog(ERROR, "bwtree: delta chain points outside relation (blk=%u nblocks=%u)",
				 (unsigned int) cur_blkno,
				 (unsigned int) rel_nblocks);

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
		hops++;
		if (hops > hops_limit)
			elog(ERROR, "bwtree: delta-apply exceeded safety bound (possible chain cycle)");
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
	snapshot->flags = base_opaque->bwto_flags;
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
	int i;

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
	if (mpage->backing_page != NULL)
		pfree(mpage->backing_page);

	memset(mpage, 0, sizeof(*mpage));
}

void
_bwt_materialize_node(Relation rel, BWTreeMetaPageData *metad,
					  BWTreePid pid, BWTreeNodeView *view)
{
	BlockNumber				cur_blkno;
	BlockNumber				rel_nblocks;
	BlockNumber				hops = 0;
	BlockNumber				hops_limit;

	if (view == NULL)
		elog(ERROR, "bwtree: materialize-node output pointer must not be NULL");

	memset(view, 0, sizeof(*view));
	view->split_right_pid = InvalidBWTreePid;
	view->state = BWT_STATE_STABLE;

	if (!_bwt_capture_node_snapshot(rel, metad, pid, &view->snapshot))
		elog(ERROR, "bwtree: cannot materialize unknown PID %u",
			 (unsigned int) pid);
	_bwt_materialize_from_snapshot(rel, &view->snapshot, &view->page);

	if (!BlockNumberIsValid(view->snapshot.delta_blkno) ||
		(view->snapshot.flags & BWT_SPLIT_PENDING) == 0)
		return;

	cur_blkno = view->snapshot.delta_blkno;
	rel_nblocks = RelationGetNumberOfBlocks(rel);
	hops_limit = rel_nblocks + 1;
	while (BlockNumberIsValid(cur_blkno))
	{
		Buffer					buf;
		Page					page;
		BWTreePageOpaque		opaque;
		OffsetNumber			maxoff;
		ItemId					itemid;
		BWTreeDeltaRecordData  *drec;

		if (cur_blkno >= rel_nblocks)
			elog(ERROR, "bwtree: materialize-node delta chain points outside relation (blk=%u nblocks=%u)",
				 (unsigned int) cur_blkno,
				 (unsigned int) rel_nblocks);

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
		hops++;
		if (hops > hops_limit)
			elog(ERROR, "bwtree: materialize-node exceeded safety bound (possible delta cycle)");
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
	BWTreeNodeSnapshot	snapshot;
	BlockNumber			cur_blkno;
	BlockNumber			hops = 0;
	BlockNumber			hops_limit;
	int					chain_len = 0;

	if (!_bwt_capture_node_snapshot(rel, metad, pid, &snapshot))
		return false;
	if (!BlockNumberIsValid(snapshot.delta_blkno))
		return false;

	cur_blkno = snapshot.delta_blkno;
	hops_limit = RelationGetNumberOfBlocks(rel) + 1;
	while (BlockNumberIsValid(cur_blkno))
	{
		Buffer			buf;
		Page			page;
		BWTreePageOpaque opaque;

		buf = _bwt_getbuf(rel, cur_blkno, BWT_READ);
		page = BufferGetPage(buf);
		opaque = BWTreePageGetOpaque(page);
		if (opaque->bwto_page_id != BWTREE_PAGE_ID || !BWTreePageIsDelta(opaque))
		{
			_bwt_relbuf(rel, buf, BWT_READ);
			break;
		}

		chain_len++;
		cur_blkno = opaque->bwto_next;
		_bwt_relbuf(rel, buf, BWT_READ);
		hops++;
		if (hops > hops_limit)
			elog(ERROR, "bwtree: delta chain walk exceeded safety bound for PID %u",
				 (unsigned int) pid);

		if (chain_len >= BWTREE_DELTA_CHAIN_THRESHOLD)
			return true;
	}

	return false;
}

void
_bwt_consolidate(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid)
{
	int	retry;

	for (retry = 0; retry < 16; retry++)
	{
		BWTreeNodeSnapshot	snapshot;
		BWTMaterializedPage	mpage;
		Buffer				newbasebuf;
		Page				newbasepage;
		BWTreePageOpaque	newopaque;
		BlockNumber			newbase_blkno;
		BlockNumber			observed_base_blkno = InvalidBlockNumber;
		BlockNumber			observed_delta_blkno = InvalidBlockNumber;
		Buffer				oldbasebuf;
		uint16				flags = 0;
		bool				published;
		int					i;

		if (!_bwt_capture_node_snapshot(rel, metad, pid, &snapshot))
			return;
		if (!BlockNumberIsValid(snapshot.delta_blkno))
			return;

		/*
		 * Build a stable logical view from (base + delta chain).
		 */
		_bwt_materialize_from_snapshot(rel, &snapshot, &mpage);

		newbasebuf = _bwt_allocbuf(rel);
		newbasepage = BufferGetPage(newbasebuf);
		newbase_blkno = BufferGetBlockNumber(newbasebuf);

		if (snapshot.is_leaf)
			flags |= BWT_LEAF;
		if (snapshot.is_root)
			flags |= BWT_ROOT;
		_bwt_initpage(newbasepage, flags, snapshot.pid, snapshot.level);
		newopaque = BWTreePageGetOpaque(newbasepage);
		newopaque->bwto_prev = mpage.prev_blkno;
		newopaque->bwto_next = mpage.next_blkno;

		for (i = 0; i < mpage.nitems; i++)
		{
			IndexTuple	itup;
			OffsetNumber offnum;

			itup = mpage.items[i];
			if (itup == NULL)
				continue;

			offnum = PageAddItem(newbasepage, (Item) itup, IndexTupleSize(itup),
								 InvalidOffsetNumber, false, false);
			if (offnum == InvalidOffsetNumber)
			{
				_bwt_relbuf(rel, newbasebuf, BWT_WRITE);
				RecordFreeIndexPage(rel, newbase_blkno);
				_bwt_free_materialized_page(&mpage);
				elog(ERROR, "bwtree: consolidation output overflow for PID %u",
					 (unsigned int) pid);
			}
		}
		MarkBufferDirty(newbasebuf);

		/*
		 * Concurrency hardening:
		 * hold old-base write latch across mapping validation + publish CAS.
		 * This blocks concurrent split/internal rewrite that operate on the
		 * same base page while consolidation attempts base swap.
		 */
		oldbasebuf = _bwt_getbuf(rel, snapshot.base_blkno, BWT_WRITE);
		if (!_bwt_map_lookup(rel, metad, pid,
							 &observed_base_blkno, &observed_delta_blkno) ||
			observed_base_blkno != snapshot.base_blkno ||
			observed_delta_blkno != snapshot.delta_blkno)
		{
			_bwt_relbuf(rel, oldbasebuf, BWT_WRITE);
			_bwt_relbuf(rel, newbasebuf, BWT_WRITE);
			RecordFreeIndexPage(rel, newbase_blkno);
			_bwt_free_materialized_page(&mpage);
			if (observed_delta_blkno == InvalidBlockNumber)
				return;
			continue;
		}

		/*
		 * Publish consolidated snapshot with one CAS:
		 * (old_base, old_delta_head) -> (new_base, InvalidDelta)
		 */
		published = _bwt_map_cas(rel, metad, pid,
								 snapshot.base_blkno, snapshot.delta_blkno,
								 newbase_blkno, InvalidBlockNumber,
								 &observed_base_blkno,
								 &observed_delta_blkno);
		_bwt_relbuf(rel, oldbasebuf, BWT_WRITE);
		_bwt_relbuf(rel, newbasebuf, BWT_WRITE);

		if (published)
		{
			bool	old_detached;
			uint64	retire_epoch;

			/*
			 * Correctness-first requirement:
			 *
			 * This codebase uses physical sibling block links for range scan.
			 * After base swap, relink neighbors to the new base block before
			 * reclaiming the old base block.
			 */
			old_detached = _bwt_relink_neighbors_on_base_swap(rel,
															  snapshot.base_blkno,
															  newbase_blkno,
															  mpage.prev_blkno,
															  mpage.next_blkno);

			retire_epoch = _bwt_epoch_enter();
			PG_TRY();
			{
				_bwt_gc_retire_delta_chain(rel, pid, snapshot.delta_blkno, retire_epoch);
				if (old_detached)
					_bwt_gc_retire_block(rel, snapshot.base_blkno, retire_epoch);
			}
			PG_CATCH();
			{
				_bwt_epoch_exit();
				PG_RE_THROW();
			}
			PG_END_TRY();
			_bwt_epoch_exit();
			/* Keep silent on conservative reclaim skip under concurrent relink. */
			(void) old_detached;
			_bwt_free_materialized_page(&mpage);
			return;
		}

		/*
		 * This new base page was never published; recycle it immediately.
		 */
		RecordFreeIndexPage(rel, newbase_blkno);
		_bwt_free_materialized_page(&mpage);

		/*
		 * If someone already published a consolidated state, we are done.
		 * Otherwise, retry from fresh snapshot.
		 */
		if (observed_delta_blkno == InvalidBlockNumber)
			return;
	}

	elog(ERROR, "bwtree: consolidation exceeded retry bound for PID %u",
		 (unsigned int) pid);
}
