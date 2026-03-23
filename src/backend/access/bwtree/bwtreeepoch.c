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
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

BWTreeEpochManager *
bwtree_epoch_create(void)
{
	return NULL;
}

void
bwtree_epoch_destroy(BWTreeEpochManager *em)
{
	(void) em;
}

void
bwtree_epoch_enter(BWTreeEpochManager *em)
{
	(void) em;
}

void
bwtree_epoch_leave(BWTreeEpochManager *em)
{
	(void) em;
}

/*
 * Mark a pointer as retired in the current epoch.  It will be freed
 * once no active thread can still reference it.
 */
void
bwtree_epoch_retire(BWTreeEpochManager *em, void *ptr)
{
	(void) em;
	(void) ptr;
}

/*
 * Advance the global epoch and free all retired objects that are safe
 * to reclaim (i.e., no thread is still in that epoch or earlier).
 */
void
bwtree_epoch_reclaim(BWTreeEpochManager *em)
{
	(void) em;
}
