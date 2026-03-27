/*-------------------------------------------------------------------------
 *
 * bwtreeinsert.c
 *    Insert entrypoint skeleton for the Bw-tree index.
 *
 *------------------------------------------------------------------------- */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/genam.h"
#include "nodes/execnodes.h"

bool
bwtreeinsert(Relation rel, Datum *values, bool *isnull,
			 ItemPointer ht_ctid, Relation heapRel,
			 IndexUniqueCheck checkUnique,
			 bool indexUnchanged,
			 IndexInfo *indexInfo)
{
	(void) rel;
	(void) values;
	(void) isnull;
	(void) ht_ctid;
	(void) heapRel;
	(void) checkUnique;
	(void) indexUnchanged;
	(void) indexInfo;

	elog(ERROR, "bwtree: insert path interface defined but implementation not written yet");
	return false;
}
