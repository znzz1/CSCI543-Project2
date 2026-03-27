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
	(void) info;
	(void) stats;
	(void) callback;
	(void) callback_state;

	elog(ERROR, "bwtree: bulkdelete interface defined but implementation not written yet");
	return NULL;
}

/*
 * bwtreevacuumcleanup -- post-VACUUM cleanup.
 */
IndexBulkDeleteResult *
bwtreevacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	(void) info;
	(void) stats;

	elog(ERROR, "bwtree: vacuumcleanup interface defined but implementation not written yet");
	return NULL;
}

void
bwtreecostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				   Cost *indexStartupCost, Cost *indexTotalCost,
				   Selectivity *indexSelectivity, double *indexCorrelation,
				   double *indexPages)
{
	(void) root;
	(void) path;
	(void) loop_count;
	(void) indexStartupCost;
	(void) indexTotalCost;
	(void) indexSelectivity;
	(void) indexCorrelation;
	(void) indexPages;

	elog(ERROR, "bwtree: costestimate interface defined but implementation not written yet");
}

bytea *
bwtreeoptions(Datum reloptions, bool validate)
{
	(void) reloptions;
	(void) validate;

	elog(ERROR, "bwtree: options interface defined but implementation not written yet");
	return NULL;
}

bool
bwtreevalidate(Oid opclassoid)
{
	(void) opclassoid;

	elog(ERROR, "bwtree: validate interface defined but implementation not written yet");
	return false;
}

void
bwtreeadjustmembers(Oid opfamilyoid, Oid opclassoid,
					List *operators, List *functions)
{
	(void) opfamilyoid;
	(void) opclassoid;
	(void) operators;
	(void) functions;

	elog(ERROR, "bwtree: adjustmembers interface defined but implementation not written yet");
}
