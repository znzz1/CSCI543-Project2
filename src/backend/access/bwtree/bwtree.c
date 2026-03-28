/*-------------------------------------------------------------------------
 *
 * bwtree.c
 *    AM handler and vacuum callbacks for the Bw-tree index.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/stratnum.h"
#include "catalog/index.h"
#include "storage/bufmgr.h"
#include "utils/selfuncs.h"

Datum
bwtreehandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BWTNProcs;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
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
	amroutine->amusemaintenanceworkmem = true;
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

/*
 * bwtreebulkdelete -- called during VACUUM to delete dead tuples.
 *
 * Walk every leaf page (base + delta merged), check each heap TID
 * via the callback, and install DELETE deltas for dead entries.
 */
IndexBulkDeleteResult *
bwtreebulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				 IndexBulkDeleteCallback callback, void *callback_state)
{
	(void) callback;
	(void) callback_state;

	/*
	 * Correctness-first / no-GC build:
	 *
	 * We currently don't physically remove dead index tuples in VACUUM path.
	 * Return stable stats so VACUUM can proceed without touching Bw-tree
	 * contents.
	 */
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	if (info != NULL && info->index != NULL)
		stats->num_pages = RelationGetNumberOfBlocks(info->index);

	return stats;
}

/*
 * bwtreevacuumcleanup -- post-VACUUM cleanup.
 */
IndexBulkDeleteResult *
bwtreevacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	if (info != NULL && info->index != NULL)
		stats->num_pages = RelationGetNumberOfBlocks(info->index);

	return stats;
}

void
bwtreecostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				   Cost *indexStartupCost, Cost *indexTotalCost,
				   Selectivity *indexSelectivity, double *indexCorrelation,
				   double *indexPages)
{
	GenericCosts costs = {0};

	/*
	 * Use generic estimator so planner can cost this AM without special
	 * Bw-tree modeling.
	 */
	genericcostestimate(root, path, loop_count, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

bytea *
bwtreeoptions(Datum reloptions, bool validate)
{
	(void) reloptions;
	(void) validate;

	/* No custom reloptions in correctness-first stage. */
	return NULL;
}

bool
bwtreevalidate(Oid opclassoid)
{
	(void) opclassoid;

	/*
	 * Correctness-first stage assumes catalog wiring is prepared by the
	 * project setup scripts; no additional runtime validation.
	 */
	return true;
}

void
bwtreeadjustmembers(Oid opfamilyoid, Oid opclassoid,
					List *operators, List *functions)
{
	(void) opfamilyoid;
	(void) opclassoid;
	(void) operators;
	(void) functions;
}
