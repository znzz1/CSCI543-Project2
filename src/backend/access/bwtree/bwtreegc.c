#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufpage.h"
#include "storage/ipc.h"
#include "storage/indexfsm.h"
#include "storage/shmem.h"
#include "storage/spin.h"

#define BWT_GC_RECLAIM_BUDGET		32
/*
 * Shared retire queue capacity.
 *
 * Correctness-first rationale:
 * A larger queue substantially reduces "drop-on-backend-exit" risk under
 * bursty retire traffic, while keeping implementation simple.
 */
#define BWT_GC_SHARED_RETIRE_CAP	65536
#define BWT_GC_EAGER_RECLAIM_INTERVAL 128

typedef struct BWTreeGCRetiredEntry
{
	BWTreeGCRetireObject	obj;
	uint64					retire_epoch;
	struct BWTreeGCRetiredEntry *next;
} BWTreeGCRetiredEntry;

typedef struct BWTreeGCSharedRetireEntry
{
	BWTreeGCRetireObject	obj;
	uint64					retire_epoch;
} BWTreeGCSharedRetireEntry;

typedef struct BWTreeGCSharedState
{
	slock_t					mutex;
	uint32					head;
	uint32					tail;
	uint32					count;
	BWTreeGCSharedRetireEntry entries[BWT_GC_SHARED_RETIRE_CAP];
} BWTreeGCSharedState;

typedef struct BWTreeGCBackendState
{
	bool				inited;
	uint64				op_counter;
	uint64				pending_count;
	BWTreeGCRetiredEntry *head;
	BWTreeGCRetiredEntry *tail;
} BWTreeGCBackendState;

static BWTreeGCBackendState bwt_gc_backend_state = {0};
static BWTreeGCSharedState *bwt_gc_shared = NULL;
static bool bwt_gc_exit_callback_registered = false;

static void _bwt_gc_backend_state_init(void);
static void _bwt_gc_backend_state_reset(void);
static void _bwt_gc_on_shmem_exit(int code, Datum arg);
static BWTreeGCRetiredEntry *_bwt_gc_alloc_entry(const BWTreeGCRetireObject *obj,
												  uint64 retire_epoch);
static void _bwt_gc_enqueue_entry(BWTreeGCRetiredEntry *entry);
static bool _bwt_gc_entry_reclaimable(const BWTreeGCRetiredEntry *entry);
static int _bwt_gc_reclaim_object(Relation rel, const BWTreeGCRetireObject *obj);
static int _bwt_gc_reclaim_entry(Relation rel, BWTreeGCRetiredEntry *entry);
static int _bwt_gc_reclaim_delta_chain(Relation rel, BlockNumber head_blkno);
static bool _bwt_gc_read_metad_snapshot(Relation rel, BWTreeMetaPageData *snapshot);
static BWTreeGCSharedState *_bwt_gc_get_shared(void);
static bool _bwt_gc_shared_push(const BWTreeGCRetireObject *obj, uint64 retire_epoch);
static bool _bwt_gc_shared_pop_if_reclaimable(Relation rel,
											  BWTreeGCRetireObject *obj,
											  uint64 *retire_epoch);
static uint32 _bwt_gc_shared_count(void);
static bool _bwt_gc_should_run(void);
static bool _bwt_gc_local_entry_matches_rel(Relation rel,
											 const BWTreeGCRetiredEntry *entry);

Size BwTreeGCShmemSize(void)
{
	return MAXALIGN(sizeof(BWTreeGCSharedState));
}

void BwTreeGCShmemInit(void)
{
	BWTreeGCSharedState *shared;
	bool found;

	shared = (BWTreeGCSharedState *)
		ShmemInitStruct("BwTreeGCSharedState", BwTreeGCShmemSize(), &found);
	if (!found)
	{
		SpinLockInit(&shared->mutex);
		shared->head = 0;
		shared->tail = 0;
		shared->count = 0;
	}

	bwt_gc_shared = shared;
}

static BWTreeGCSharedState *
_bwt_gc_get_shared(void)
{
	if (bwt_gc_shared == NULL)
		BwTreeGCShmemInit();
	return bwt_gc_shared;
}

static bool
_bwt_gc_shared_push(const BWTreeGCRetireObject *obj, uint64 retire_epoch)
{
	BWTreeGCSharedState *shared;
	uint32				pos;

	if (obj == NULL)
		return false;

	shared = _bwt_gc_get_shared();
	SpinLockAcquire(&shared->mutex);
	if (shared->count >= BWT_GC_SHARED_RETIRE_CAP)
	{
		SpinLockRelease(&shared->mutex);
		return false;
	}

	pos = shared->tail;
	shared->entries[pos].obj = *obj;
	shared->entries[pos].retire_epoch = retire_epoch;
	shared->tail = (pos + 1) % BWT_GC_SHARED_RETIRE_CAP;
	shared->count++;
	SpinLockRelease(&shared->mutex);
	return true;
}

static bool
_bwt_gc_shared_pop_if_reclaimable(Relation rel,
								  BWTreeGCRetireObject *obj,
								  uint64 *retire_epoch)
{
	BWTreeGCSharedState *shared;
	Oid					relid;
	uint32				scan;
	uint32				scan_limit;

	if (rel == NULL || obj == NULL || retire_epoch == NULL)
		return false;
	relid = RelationGetRelid(rel);

	shared = _bwt_gc_get_shared();
	SpinLockAcquire(&shared->mutex);
	if (shared->count == 0)
	{
		SpinLockRelease(&shared->mutex);
		return false;
	}

	/*
	 * Remove head-of-line blocking:
	 * rotate non-matching / not-yet-reclaimable entries to tail while scanning
	 * at most one full queue turn.
	 */
	scan_limit = shared->count;
	for (scan = 0; scan < scan_limit; scan++)
	{
		BWTreeGCSharedRetireEntry entry;

		entry = shared->entries[shared->head];
		if (entry.obj.relid == relid &&
			_bwt_epoch_can_reclaim(entry.retire_epoch))
		{
			shared->head = (shared->head + 1) % BWT_GC_SHARED_RETIRE_CAP;
			shared->count--;
			SpinLockRelease(&shared->mutex);

			*obj = entry.obj;
			*retire_epoch = entry.retire_epoch;
			return true;
		}

		shared->head = (shared->head + 1) % BWT_GC_SHARED_RETIRE_CAP;
		shared->entries[shared->tail] = entry;
		shared->tail = (shared->tail + 1) % BWT_GC_SHARED_RETIRE_CAP;
	}

	SpinLockRelease(&shared->mutex);
	return false;
}

static uint32
_bwt_gc_shared_count(void)
{
	BWTreeGCSharedState *shared;
	uint32				count;

	shared = _bwt_gc_get_shared();
	SpinLockAcquire(&shared->mutex);
	count = shared->count;
	SpinLockRelease(&shared->mutex);

	return count;
}

static void _bwt_gc_backend_state_init(void)
{
	(void) _bwt_gc_get_shared();

	bwt_gc_backend_state.inited = true;
	bwt_gc_backend_state.op_counter = 0;
	bwt_gc_backend_state.pending_count = 0;
	bwt_gc_backend_state.head = NULL;
	bwt_gc_backend_state.tail = NULL;

	if (!bwt_gc_exit_callback_registered)
	{
		on_shmem_exit(_bwt_gc_on_shmem_exit, (Datum) 0);
		bwt_gc_exit_callback_registered = true;
	}
}

static void _bwt_gc_backend_state_reset(void)
{
	BWTreeGCRetiredEntry *cur;
	uint64				spilled = 0;
	uint64				dropped = 0;

	cur = bwt_gc_backend_state.head;
	while (cur != NULL)
	{
		BWTreeGCRetiredEntry *next;

		next = cur->next;
		/*
		 * Best-effort spill retry:
		 * queue may be transiently full when many backends exit together.
		 */
		if (_bwt_gc_shared_push(&cur->obj, cur->retire_epoch))
			spilled++;
		else
		{
			int retry;
			bool pushed = false;

			for (retry = 0; retry < 16; retry++)
			{
				pg_usleep(1000L);
				if (_bwt_gc_shared_push(&cur->obj, cur->retire_epoch))
				{
					pushed = true;
					spilled++;
					break;
				}
			}

			if (!pushed)
				dropped++;
		}
		pfree(cur);
		cur = next;
	}

	if (dropped > 0)
	{
		elog(WARNING,
			 "bwtree: dropped %" UINT64_FORMAT " unreclaimed GC entries at backend shutdown (shared retire queue full, spilled=%" UINT64_FORMAT ")",
			 dropped, spilled);
	}

	bwt_gc_backend_state.inited = false;
	bwt_gc_backend_state.op_counter = 0;
	bwt_gc_backend_state.pending_count = 0;
	bwt_gc_backend_state.head = NULL;
	bwt_gc_backend_state.tail = NULL;
}

static void
_bwt_gc_on_shmem_exit(int code, Datum arg)
{
	(void) code;
	(void) arg;

	_bwt_gc_fini();
}

static BWTreeGCRetiredEntry *_bwt_gc_alloc_entry(const BWTreeGCRetireObject *obj,
												  uint64 retire_epoch)
{
	BWTreeGCRetiredEntry *entry;
	MemoryContext oldcxt;

	if (obj == NULL)
		return NULL;

	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	entry = (BWTreeGCRetiredEntry *) palloc0(sizeof(BWTreeGCRetiredEntry));
	MemoryContextSwitchTo(oldcxt);

	entry->obj = *obj;
	entry->retire_epoch = retire_epoch;
	entry->next = NULL;
	return entry;
}

static void _bwt_gc_enqueue_entry(BWTreeGCRetiredEntry *entry)
{
	if (entry == NULL)
		return;

	if (bwt_gc_backend_state.tail == NULL)
	{
		bwt_gc_backend_state.head = entry;
		bwt_gc_backend_state.tail = entry;
		return;
	}

	bwt_gc_backend_state.tail->next = entry;
	bwt_gc_backend_state.tail = entry;
}

static bool _bwt_gc_entry_reclaimable(const BWTreeGCRetiredEntry *entry)
{
	if (entry == NULL)
		return false;

	return _bwt_epoch_can_reclaim(entry->retire_epoch);
}

static bool
_bwt_gc_local_entry_matches_rel(Relation rel, const BWTreeGCRetiredEntry *entry)
{
	Oid relid;

	if (rel == NULL || entry == NULL)
		return false;

	relid = RelationGetRelid(rel);
	if (entry->obj.relid == InvalidOid)
		return true;
	return entry->obj.relid == relid;
}

static int
_bwt_gc_reclaim_object(Relation rel, const BWTreeGCRetireObject *obj)
{
	if (obj == NULL)
		return 0;

	switch (obj->kind)
	{
		case BWT_GC_RETIRE_DELTA_CHAIN:
			return _bwt_gc_reclaim_delta_chain(rel, obj->head_blkno);
		case BWT_GC_RETIRE_PID:
			/* RETIRE_PID is converted into block/chain retirement on enqueue. */
			return 0;
		case BWT_GC_RETIRE_BLOCK:
			if (BlockNumberIsValid(obj->head_blkno))
			{
				RecordFreeIndexPage(rel, obj->head_blkno);
				return 1;
			}
			return 0;
	}

	return 0;
}

static int _bwt_gc_reclaim_entry(Relation rel, BWTreeGCRetiredEntry *entry)
{
	if (entry == NULL)
		return 0;

	return _bwt_gc_reclaim_object(rel, &entry->obj);
}

static int _bwt_gc_reclaim_delta_chain(Relation rel, BlockNumber head_blkno)
{
	BlockNumber cur_blkno;
	BlockNumber rel_nblocks;
	int reclaimed_blocks;
	BlockNumber hop_guard;

	if (!BlockNumberIsValid(head_blkno))
		return 0;

	cur_blkno = head_blkno;
	rel_nblocks = RelationGetNumberOfBlocks(rel);
	reclaimed_blocks = 0;
	hop_guard = 0;

	while (BlockNumberIsValid(cur_blkno))
	{
		Buffer buf;
		Page page;
		BWTreePageOpaque opaque;
		BlockNumber next_blkno;

		if (cur_blkno >= rel_nblocks)
			break;

		buf = _bwt_getbuf(rel, cur_blkno, BWT_READ);
		page = BufferGetPage(buf);
		opaque = BWTreePageGetOpaque(page);
		if (opaque->bwto_page_id != BWTREE_PAGE_ID || !BWTreePageIsDelta(opaque))
		{
			_bwt_relbuf(rel, buf, BWT_READ);
			break;
		}

		next_blkno = opaque->bwto_next;
		_bwt_relbuf(rel, buf, BWT_READ);

		RecordFreeIndexPage(rel, cur_blkno);
		reclaimed_blocks++;

		cur_blkno = next_blkno;
		hop_guard++;
		if (hop_guard > rel_nblocks)
			break;
	}

	return reclaimed_blocks;
}

static bool
_bwt_gc_read_metad_snapshot(Relation rel, BWTreeMetaPageData *snapshot)
{
	Buffer				metabuf;
	Page				metapage;
	BWTreeMetaPageData *metad;

	if (rel == NULL || snapshot == NULL)
		return false;

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
	return true;
}

static bool _bwt_gc_should_run(void)
{
	if (!bwt_gc_backend_state.inited)
		return false;

	/*
	 * Correctness-first policy:
	 * run whenever there is pending garbage, instead of deferring to coarse
	 * intervals. This aggressively drains backend-local retire state so fewer
	 * entries survive until backend exit.
	 */
	if (bwt_gc_backend_state.pending_count > 0)
		return true;

	return (_bwt_gc_shared_count() > 0);
}

void _bwt_gc_init(void)
{
	if (bwt_gc_backend_state.inited)
		return;

	_bwt_gc_backend_state_init();
}

void _bwt_gc_fini(void)
{
	if (!bwt_gc_backend_state.inited)
		return;

	_bwt_gc_backend_state_reset();
}

void _bwt_gc_retire_object(Relation rel, const BWTreeGCRetireObject *obj,
						   uint64 retire_epoch)
{
	BWTreeGCRetiredEntry *entry;

	if (obj == NULL)
		return;

	if (!bwt_gc_backend_state.inited)
		_bwt_gc_backend_state_init();

	entry = _bwt_gc_alloc_entry(obj, retire_epoch);
	if (entry == NULL)
		return;

	if (entry->obj.relid == InvalidOid && rel != NULL)
		entry->obj.relid = RelationGetRelid(rel);

	_bwt_gc_enqueue_entry(entry);
	bwt_gc_backend_state.pending_count++;

	/*
	 * Avoid paying eager-reclaim overhead on every retire; run periodically.
	 */
	bwt_gc_backend_state.op_counter++;
	if ((bwt_gc_backend_state.op_counter % BWT_GC_EAGER_RECLAIM_INTERVAL) == 0)
	{
		_bwt_epoch_try_advance();
		(void) _bwt_gc_try_reclaim(rel, BWT_GC_RECLAIM_BUDGET);
	}
}

void _bwt_gc_retire_delta_chain(Relation rel, BWTreePid pid,
								BlockNumber old_delta_head,
								uint64 retire_epoch)
{
	BWTreeGCRetireObject obj;

	obj.kind = BWT_GC_RETIRE_DELTA_CHAIN;
	obj.relid = (rel != NULL) ? RelationGetRelid(rel) : InvalidOid;
	obj.pid = pid;
	obj.head_blkno = old_delta_head;

	_bwt_gc_retire_object(rel, &obj, retire_epoch);
}

void _bwt_gc_retire_pid(Relation rel, BWTreePid pid, uint64 retire_epoch)
{
	BWTreeMetaPageData	metad;
	BlockNumber			base_blkno;
	BlockNumber			delta_blkno;

	if (rel == NULL || pid == InvalidBWTreePid)
		return;

	if (!_bwt_gc_read_metad_snapshot(rel, &metad))
		return;
	if (!_bwt_map_lookup(rel, &metad, pid, &base_blkno, &delta_blkno))
		return;

	if (BlockNumberIsValid(delta_blkno))
		_bwt_gc_retire_delta_chain(rel, pid, delta_blkno, retire_epoch);
	if (BlockNumberIsValid(base_blkno))
		_bwt_gc_retire_block(rel, base_blkno, retire_epoch);
}

void _bwt_gc_retire_block(Relation rel, BlockNumber blkno, uint64 retire_epoch)
{
	BWTreeGCRetireObject obj;

	if (!BlockNumberIsValid(blkno))
		return;

	obj.kind = BWT_GC_RETIRE_BLOCK;
	obj.relid = (rel != NULL) ? RelationGetRelid(rel) : InvalidOid;
	obj.pid = InvalidBWTreePid;
	obj.head_blkno = blkno;
	_bwt_gc_retire_object(rel, &obj, retire_epoch);
}

int _bwt_gc_try_reclaim(Relation rel, int budget)
{
	int reclaimed;
	BWTreeGCRetiredEntry *prev;
	BWTreeGCRetiredEntry *cur;

	if (!bwt_gc_backend_state.inited)
		return 0;
	if (rel == NULL)
		return 0;

	if (budget <= 0)
		budget = BWT_GC_RECLAIM_BUDGET;

	reclaimed = 0;
	/*
	 * Local reclaim pass:
	 * walk reclaimable prefix and reclaim only entries belonging to current
	 * relation to avoid cross-relation free on shared backend-local queue.
	 */
	prev = NULL;
	cur = bwt_gc_backend_state.head;
	while (cur != NULL && reclaimed < budget)
	{
		BWTreeGCRetiredEntry *next;
		int reclaimed_units;

		if (!_bwt_gc_entry_reclaimable(cur))
			break;
		next = cur->next;

		if (!_bwt_gc_local_entry_matches_rel(rel, cur))
		{
			prev = cur;
			cur = next;
			continue;
		}

		reclaimed_units = _bwt_gc_reclaim_entry(rel, cur);
		if (prev == NULL)
			bwt_gc_backend_state.head = next;
		else
			prev->next = next;
		if (next == NULL)
			bwt_gc_backend_state.tail = prev;

		(void) reclaimed_units;
		pfree(cur);
		bwt_gc_backend_state.pending_count--;
		reclaimed++;
		cur = next;
	}

	while (reclaimed < budget)
	{
		BWTreeGCRetireObject	obj;
		uint64					retire_epoch;
		int						reclaimed_units;

		if (!_bwt_gc_shared_pop_if_reclaimable(rel, &obj, &retire_epoch))
			break;

		(void) retire_epoch;
		reclaimed_units = _bwt_gc_reclaim_object(rel, &obj);
		(void) reclaimed_units;
		reclaimed++;
	}

	return reclaimed;
}

void _bwt_gc_maybe_run(Relation rel)
{
	if (!bwt_gc_backend_state.inited)
		_bwt_gc_backend_state_init();

	if (_bwt_gc_should_run())
	{
		_bwt_epoch_try_advance();
		(void) _bwt_gc_try_reclaim(rel, BWT_GC_RECLAIM_BUDGET);
	}
}

uint64 _bwt_gc_pending_count(Relation rel)
{
	(void) rel;
	return bwt_gc_backend_state.pending_count + (uint64) _bwt_gc_shared_count();
}
