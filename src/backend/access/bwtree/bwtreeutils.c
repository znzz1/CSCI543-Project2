/*-------------------------------------------------------------------------
 *
 * bwtreeutils.c
 *    Planner support and relation options for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "utils/selfuncs.h"

void
bwtreecostestimate(PlannerInfo *root, IndexPath *path, double loop_count,
				   Cost *indexStartupCost, Cost *indexTotalCost,
				   Selectivity *indexSelectivity, double *indexCorrelation,
				   double *indexPages)
{
	GenericCosts costs = {0};

	genericcostestimate(root, path, loop_count, &costs);

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost   = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages       = costs.numIndexPages;
}

bytea *
bwtreeoptions(Datum reloptions, bool validate)
{
	(void) reloptions;
	(void) validate;
	return NULL;
}
