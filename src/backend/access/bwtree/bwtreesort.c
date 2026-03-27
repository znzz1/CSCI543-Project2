/*-------------------------------------------------------------------------
 *
 * bwtreesort.c
 *    Build skeleton for the Bw-tree index.
 *
 *------------------------------------------------------------------------- */
#include "postgres.h"

#include "access/bwtree.h"
#include "nodes/execnodes.h"

IndexBuildResult *
bwtreebuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	(void) heap;
	(void) index;
	(void) indexInfo;

	elog(ERROR, "bwtree: build interface defined but implementation not written yet");
	return NULL;
}

void
bwtreebuildempty(Relation index)
{
	(void) index;

	elog(ERROR, "bwtree: buildempty interface defined but implementation not written yet");
}

char *
bwtreebuildphasename(int64 phasenum)
{
	(void) phasenum;

	elog(ERROR, "bwtree: buildphasename interface defined but implementation not written yet");
	return NULL;
}
