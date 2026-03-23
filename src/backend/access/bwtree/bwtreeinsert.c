/*-------------------------------------------------------------------------
 *
 * bwtreeinsert.c
 *    Insert-path scaffolding for the Bw-tree access method.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
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

	return false;
}

void
bwtreeinsertcleanup(Relation index, IndexInfo *indexInfo)
{
	(void) index;
	(void) indexInfo;
}
