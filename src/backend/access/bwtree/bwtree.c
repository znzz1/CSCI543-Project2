#include "postgres.h"

#include "access/amapi.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "fmgr.h"
#include "nodes/execnodes.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(bwtreehandler);

static IndexBuildResult *bwtree_build(Relation heap, Relation index,
									  IndexInfo *indexInfo);
static void bwtree_buildempty(Relation index);

/*
 * bwtree handler
 */
Datum
bwtreehandler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 0;
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
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amsummarizing = false;
	amroutine->amparallelvacuumoptions = 0;

	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = bwtree_build;
	amroutine->ambuildempty = bwtree_buildempty;
	amroutine->aminsert = NULL;
	amroutine->aminsertcleanup = NULL;
	amroutine->ambulkdelete = NULL;
	amroutine->amvacuumcleanup = NULL;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = NULL;
	amroutine->amoptions = NULL;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = NULL;
	amroutine->amadjustmembers = NULL;
	amroutine->ambeginscan = NULL;
	amroutine->amrescan = NULL;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = NULL;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

/*
 * bwtree_build
 * 最小实现：先不扫描 heap，不插任何 tuple
 */
static IndexBuildResult *
bwtree_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = 0;
	result->index_tuples = 0;

	return result;
}

/*
 * bwtree_buildempty
 * 当前先不做初始化页
 */
static void
bwtree_buildempty(Relation index)
{
}