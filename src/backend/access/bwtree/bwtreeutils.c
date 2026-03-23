/*-------------------------------------------------------------------------
 *
 * bwtreeutils.c
 *    Utility and planner callback scaffolding for the Bw-tree access method.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

bool
bwtreecanreturn(Relation index, int attno)
{
	(void) index;
	(void) attno;

	return false;
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

	*indexStartupCost = 1.0e10;
	*indexTotalCost = 1.0e10;
	*indexSelectivity = 1.0;
	*indexCorrelation = 0.0;
	*indexPages = 1.0;
}

bytea *
bwtreeoptions(Datum reloptions, bool validate)
{
	(void) reloptions;
	(void) validate;

	return NULL;
}

bool
bwtreeproperty(Oid index_oid, int attno,
			   IndexAMProperty prop, const char *propname,
			   bool *res, bool *isnull)
{
	(void) index_oid;
	(void) attno;
	(void) prop;
	(void) propname;
	(void) res;
	(void) isnull;

	return false;
}
