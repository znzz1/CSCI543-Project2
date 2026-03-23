/*-------------------------------------------------------------------------
 *
 * bwtreeepoch.c
 *    Epoch-based memory reclamation for the Bw-tree access method.
 *
 *    In a latch-free structure, pages and delta records cannot be freed
 *    immediately after they are replaced, because concurrent readers may
 *    still hold references.  The epoch mechanism works as follows:
 *
 *      - A global epoch counter is maintained.
 *      - Each thread "enters" the current epoch before accessing the
 *        index, and "leaves" when done.
 *      - When a page or delta is replaced, it is "retired" along with
 *        the current epoch number.
 *      - Periodically the epoch is advanced and retired objects whose
 *        epoch is older than every active thread's epoch are freed.
 *
 *    In PostgreSQL each backend is single-threaded, so the epoch
 *    manager is per-backend.  The mechanism protects against freeing
 *    objects that are still reachable within a nested operation
 *    (e.g. an ongoing scan while a consolidation replaces a node).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

#define BWTREE_EPOCH_RECLAIM_THRESHOLD 64

/*
 * Create and initialize an epoch manager.
 */
BWTreeEpochManager *
bwtree_epoch_create(void)
{
	BWTreeEpochManager *em;

	em = (BWTreeEpochManager *) palloc0(sizeof(BWTreeEpochManager));
	em->global_epoch = 1;
	em->local_epoch = 0;
	em->in_epoch = false;
	em->nesting = 0;
	em->retired_head = NULL;
	em->retired_count = 0;

	return em;
}

/*
 * Destroy the epoch manager, freeing all remaining retired objects.
 */
void
bwtree_epoch_destroy(BWTreeEpochManager *em)
{
	BWTreeRetiredEntry *entry;
	BWTreeRetiredEntry *next;

	if (em == NULL)
		return;

	for (entry = em->retired_head; entry != NULL; entry = next)
	{
		next = entry->next;
		pfree(entry->ptr);
		pfree(entry);
	}

	pfree(em);
}

/*
 * Enter the current epoch.  Must be called before accessing any
 * Bw-tree node.  Supports nesting: only the outermost enter/leave
 * pair actually changes the epoch state.
 */
void
bwtree_epoch_enter(BWTreeEpochManager *em)
{
	Assert(em != NULL);

	em->nesting++;

	if (!em->in_epoch)
	{
		em->local_epoch = em->global_epoch;
		em->in_epoch = true;
	}
}

/*
 * Leave the current epoch.  When the outermost nesting level is
 * reached, we try to reclaim retired objects if enough have
 * accumulated.
 */
void
bwtree_epoch_leave(BWTreeEpochManager *em)
{
	Assert(em != NULL);
	Assert(em->nesting > 0);

	em->nesting--;

	if (em->nesting == 0)
	{
		em->in_epoch = false;
		em->local_epoch = 0;

		/* advance the global epoch */
		em->global_epoch++;

		if (em->retired_count >= BWTREE_EPOCH_RECLAIM_THRESHOLD)
			bwtree_epoch_reclaim(em);
	}
}

/*
 * Mark a palloc'd pointer as retired.  It will be freed once it is
 * safe to do so (i.e. no thread/operation can still reference it).
 *
 * The pointer is tagged with the current global epoch so that
 * reclaim() knows when it is safe to pfree().
 */
void
bwtree_epoch_retire(BWTreeEpochManager *em, void *ptr)
{
	BWTreeRetiredEntry *entry;

	Assert(em != NULL);

	if (ptr == NULL)
		return;

	entry = (BWTreeRetiredEntry *) palloc(sizeof(BWTreeRetiredEntry));
	entry->ptr = ptr;
	entry->retired_epoch = em->global_epoch;
	entry->next = em->retired_head;

	em->retired_head = entry;
	em->retired_count++;
}

/*
 * Reclaim all retired objects that are safe to free.
 *
 * An object retired in epoch E is safe to free when no backend is
 * still "in" epoch E or earlier.  In the single-backend model this
 * means: if we are NOT inside an epoch (nesting == 0), everything
 * can be freed.  If we ARE inside an epoch, objects retired strictly
 * before our local_epoch can be freed.
 */
void
bwtree_epoch_reclaim(BWTreeEpochManager *em)
{
	BWTreeRetiredEntry *entry;
	BWTreeRetiredEntry *next;
	BWTreeRetiredEntry *prev;
	uint64              safe_epoch;

	Assert(em != NULL);

	/*
	 * Determine the safe-to-free boundary.  Objects retired in epochs
	 * strictly less than safe_epoch can be freed.
	 */
	if (em->in_epoch)
		safe_epoch = em->local_epoch;
	else
		safe_epoch = em->global_epoch;

	prev = NULL;
	entry = em->retired_head;

	while (entry != NULL)
	{
		next = entry->next;

		if (entry->retired_epoch < safe_epoch)
		{
			/* safe to free */
			pfree(entry->ptr);

			if (prev != NULL)
				prev->next = next;
			else
				em->retired_head = next;

			pfree(entry);
			em->retired_count--;
		}
		else
		{
			prev = entry;
		}

		entry = next;
	}
}
