/*-------------------------------------------------------------------------
 *
 * bwtreesort.c
 *    Build-time scaffolding for the Bw-tree access method.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "commands/progress.h"
#include "nodes/execnodes.h"

IndexBuildResult *
bwtreebuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;

	(void) heap;
	(void) indexInfo;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	result = (IndexBuildResult *) palloc0(sizeof(IndexBuildResult));
	return result;
}

void
bwtreebuildempty(Relation index)
{
	(void) index;
}

char *
bwtreebuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_BWTREE_PHASE_TABLE_SCAN:
			return "scanning table";
		case PROGRESS_BWTREE_PHASE_SORT_LOAD:
			return "loading tuples";
		case PROGRESS_BWTREE_PHASE_MAPPING_TABLE_LOAD:
			return "building mapping table";
		case PROGRESS_BWTREE_PHASE_DELTA_CONSOLIDATION:
			return "consolidating deltas";
		default:
			return NULL;
	}
}
