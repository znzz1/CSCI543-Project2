#include "postgres.h"

#include "access/bwtree.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"

typedef struct BWTreeEpochBackendState
{
	bool	inited;
	bool	active;
	uint32	nest_depth;
	uint64	local_epoch;
	ProcNumber slotno;
	bool	slot_valid;
} BWTreeEpochBackendState;

typedef struct BWTreeEpochSharedState
{
	pg_atomic_uint64 global_epoch;
	uint32	num_slots;
	pg_atomic_uint64 active_epochs[FLEXIBLE_ARRAY_MEMBER];
} BWTreeEpochSharedState;

static BWTreeEpochBackendState bwt_epoch_backend_state = {0};
static BWTreeEpochSharedState *bwt_epoch_shared = NULL;

#define BWT_EPOCH_MAX_SLOTS	256U
static BWTreeEpochSharedState *_bwt_epoch_get_shared(void);
static void _bwt_epoch_on_shmem_exit(int code, Datum arg);
static uint32 _bwt_epoch_nslots(void)
{
	uint32	nslots;

	nslots = (uint32) (MaxBackends + NUM_AUXILIARY_PROCS);
	if (nslots > BWT_EPOCH_MAX_SLOTS)
		elog(ERROR, "bwtree: epoch slot capacity exceeded (%u > %u)",
			 nslots, (uint32) BWT_EPOCH_MAX_SLOTS);

	return BWT_EPOCH_MAX_SLOTS;
}

Size BwTreeEpochShmemSize(void)
{
	uint32	nslots;
	Size	size;

	nslots = _bwt_epoch_nslots();
	size = offsetof(BWTreeEpochSharedState, active_epochs);
	size = add_size(size, mul_size((Size) nslots, sizeof(pg_atomic_uint64)));
	return size;
}

void BwTreeEpochShmemInit(void)
{
	(void) _bwt_epoch_get_shared();
}

static BWTreeEpochSharedState *_bwt_epoch_get_shared(void)
{
	BWTreeEpochSharedState *shared;
	bool	found;
	Size	size;
	uint32	nslots;
	uint32	i;

	if (bwt_epoch_shared != NULL)
		return bwt_epoch_shared;

	if (ProcGlobal == NULL || ProcGlobal->allProcCount <= 0)
		elog(ERROR, "bwtree: ProcGlobal is not ready for epoch shared state");

	nslots = (uint32) ProcGlobal->allProcCount;
	if (nslots > BWT_EPOCH_MAX_SLOTS)
		elog(ERROR, "bwtree: ProcGlobal slot count %u exceeds BwTree epoch cap %u",
			 nslots, (uint32) BWT_EPOCH_MAX_SLOTS);
	size = BwTreeEpochShmemSize();

	shared = (BWTreeEpochSharedState *)
		ShmemInitStruct("BwTreeEpochSharedState", size, &found);
	if (!found)
	{
		pg_atomic_init_u64(&shared->global_epoch, 1);
		shared->num_slots = BWT_EPOCH_MAX_SLOTS;
		for (i = 0; i < shared->num_slots; i++)
			pg_atomic_init_u64(&shared->active_epochs[i], PG_UINT64_MAX);
	}
	else if (shared->num_slots != BWT_EPOCH_MAX_SLOTS)
	{
		elog(ERROR, "bwtree: epoch shared slot count mismatch");
	}

	bwt_epoch_shared = shared;
	return shared;
}

void _bwt_epoch_backend_init(void)
{
	BWTreeEpochSharedState *shared;

	if (bwt_epoch_backend_state.inited)
		return;

	shared = _bwt_epoch_get_shared();

	bwt_epoch_backend_state.inited = true;
	bwt_epoch_backend_state.active = false;
	bwt_epoch_backend_state.nest_depth = 0;
	bwt_epoch_backend_state.local_epoch = 0;
	bwt_epoch_backend_state.slotno = MyProcNumber;
	bwt_epoch_backend_state.slot_valid =
		(MyProcNumber != INVALID_PROC_NUMBER &&
		 (uint32) MyProcNumber < shared->num_slots);

	if (!bwt_epoch_backend_state.slot_valid)
		elog(ERROR, "bwtree: invalid epoch slot for backend (MyProcNumber=%d, slots=%u)",
			 (int) MyProcNumber, shared->num_slots);

	pg_atomic_write_u64(
		&shared->active_epochs[(uint32) bwt_epoch_backend_state.slotno],
		PG_UINT64_MAX);

	on_shmem_exit(_bwt_epoch_on_shmem_exit, (Datum) 0);
}

void _bwt_epoch_backend_fini(void)
{
	BWTreeEpochSharedState *shared;

	if (!bwt_epoch_backend_state.inited)
		return;

	shared = _bwt_epoch_get_shared();
	pg_atomic_write_u64(
		&shared->active_epochs[(uint32) bwt_epoch_backend_state.slotno],
		PG_UINT64_MAX);

	bwt_epoch_backend_state.inited = false;
	bwt_epoch_backend_state.active = false;
	bwt_epoch_backend_state.nest_depth = 0;
	bwt_epoch_backend_state.local_epoch = 0;
	bwt_epoch_backend_state.slotno = INVALID_PROC_NUMBER;
	bwt_epoch_backend_state.slot_valid = false;
}

uint64 _bwt_epoch_enter(void)
{
	BWTreeEpochSharedState *shared;
	uint64	cur_epoch;

	if (!bwt_epoch_backend_state.inited)
		_bwt_epoch_backend_init();
	shared = _bwt_epoch_get_shared();

	if (bwt_epoch_backend_state.active)
	{
		if (bwt_epoch_backend_state.nest_depth == PG_UINT32_MAX)
			elog(ERROR, "bwtree: epoch nest depth overflow");
		bwt_epoch_backend_state.nest_depth++;
		return bwt_epoch_backend_state.local_epoch;
	}

	cur_epoch = _bwt_epoch_current();
	bwt_epoch_backend_state.active = true;
	bwt_epoch_backend_state.nest_depth = 1;
	bwt_epoch_backend_state.local_epoch = cur_epoch;

	pg_atomic_write_u64(
		&shared->active_epochs[(uint32) bwt_epoch_backend_state.slotno],
		cur_epoch);

	return bwt_epoch_backend_state.local_epoch;
}

void _bwt_epoch_exit(void)
{
	if (!bwt_epoch_backend_state.inited ||
		!bwt_epoch_backend_state.active ||
		bwt_epoch_backend_state.nest_depth == 0)
		elog(ERROR, "bwtree: epoch-exit without matching epoch-enter");

	bwt_epoch_backend_state.nest_depth--;
	if (bwt_epoch_backend_state.nest_depth > 0)
		return;

	{
		BWTreeEpochSharedState *shared = _bwt_epoch_get_shared();
		pg_atomic_write_u64(
			&shared->active_epochs[(uint32) bwt_epoch_backend_state.slotno],
			PG_UINT64_MAX);
	}

	bwt_epoch_backend_state.active = false;
	bwt_epoch_backend_state.local_epoch = 0;
}

uint64 _bwt_epoch_current(void)
{
	BWTreeEpochSharedState *shared;

	shared = _bwt_epoch_get_shared();
	return pg_atomic_read_u64(&shared->global_epoch);
}

void _bwt_epoch_try_advance(void)
{
	BWTreeEpochSharedState *shared;
	uint64	current_epoch;
	uint64	min_active;
	uint64	expected;

	if (!bwt_epoch_backend_state.inited)
		_bwt_epoch_backend_init();

	shared = _bwt_epoch_get_shared();
	current_epoch = pg_atomic_read_u64(&shared->global_epoch);
	min_active = _bwt_epoch_min_active();

	/*
	 * Paper-aligned advancement rule:
	 * only move to the next epoch when all active participants are already in
	 * the current epoch (or there is no active participant).
	 */
	if (min_active != PG_UINT64_MAX && min_active < current_epoch)
		return;

	expected = current_epoch;
	(void) pg_atomic_compare_exchange_u64(&shared->global_epoch,
										  &expected,
										  current_epoch + 1);
}

uint64 _bwt_epoch_min_active(void)
{
	BWTreeEpochSharedState *shared;
	uint64	min_epoch = PG_UINT64_MAX;
	uint32	i;

	shared = _bwt_epoch_get_shared();
	for (i = 0; i < shared->num_slots; i++)
	{
		uint64	e;

		e = pg_atomic_read_u64(&shared->active_epochs[i]);
		if (e < min_epoch)
			min_epoch = e;
	}

	return min_epoch;
}

bool _bwt_epoch_can_reclaim(uint64 retire_epoch)
{
	uint64	min_active;

	min_active = _bwt_epoch_min_active();
	if (min_active == PG_UINT64_MAX)
		return true;

	return retire_epoch < min_active;
}

static void _bwt_epoch_on_shmem_exit(int code, Datum arg)
{
	(void) code;
	(void) arg;

	_bwt_epoch_backend_fini();
}
