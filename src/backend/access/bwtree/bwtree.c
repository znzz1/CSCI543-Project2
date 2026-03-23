/*-------------------------------------------------------------------------
 *
 * bwtree.c
 *    Public access-method entry points for the Bw-tree index.
 *
 *    The Bw-tree is a latch-free index structure (ICDE 2013) that uses
 *    a mapping table, delta chains, CAS-based updates, and epoch-based
 *    reclamation instead of the traditional latch-based B+tree approach.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/stratnum.h"

/*
 * Bw-tree handler function: return IndexAmRoutine with access method
 * parameters and callbacks.
 *
 * Bw-tree supports ordered access (like B+tree) but is latch-free.
 * Several features that are B+tree-specific (CLUSTER, parallel scan,
 * INCLUDE columns) are disabled for the initial implementation.
 */
Datum
bwtreehandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BWTREE_NPROCS;
	amroutine->amoptsprocnum = BWTREE_OPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcanbuildparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amsummarizing = false;
	amroutine->amparallelvacuumoptions = VACUUM_OPTION_PARALLEL_BULKDEL;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = bwtreebuild;
	amroutine->ambuildempty = bwtreebuildempty;
	amroutine->aminsert = bwtreeinsert;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = bwtreebulkdelete;
	amroutine->amvacuumcleanup = bwtreevacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = bwtreecostestimate;
	amroutine->amoptions = bwtreeoptions;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = bwtreebuildphasename;
	amroutine->amvalidate = bwtreevalidate;
	amroutine->amadjustmembers = bwtreeadjustmembers;
	amroutine->ambeginscan = bwtreebeginscan;
	amroutine->amrescan = bwtreerescan;
	amroutine->amgettuple = bwtreegettuple;
	amroutine->amgetbitmap = bwtreegetbitmap;
	amroutine->amendscan = bwtreeendscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

IndexBulkDeleteResult *
bwtreebulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				 IndexBulkDeleteCallback callback, void *callback_state)
{
	(void) info;
	(void) callback;
	(void) callback_state;

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	return stats;
}

IndexBulkDeleteResult *
bwtreevacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	(void) info;
	return stats;
}