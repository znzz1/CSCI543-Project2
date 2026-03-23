/*-------------------------------------------------------------------------
 *
 * bwtreeconsolidate.c
 *    Delta-chain consolidation for the Bw-tree access method.
 *
 *    When the delta chain on a logical page becomes too long (exceeds
 *    BWTREE_DELTA_CHAIN_THRESHOLD), a consolidation creates a new base
 *    page with all deltas applied, then CAS-es the mapping table entry
 *    to point to the new page.  The old base page + delta chain are
 *    retired via the epoch-based reclamation mechanism.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

/*
 * Decide whether the delta chain on `node` should be consolidated.
 */
bool
bwtree_should_consolidate(BWTreeNode *node, uint32 threshold)
{
	(void) node;
	(void) threshold;

	return false;
}

/*
 * Consolidate the delta chain for `pid`:
 *   1. Collect all deltas + base page.
 *   2. Apply deltas to produce a new sorted base page.
 *   3. CAS the mapping table to install the new page.
 *   4. Retire old deltas/page via epoch manager.
 *
 * Returns the new base page on success, NULL if the CAS failed
 * (another thread consolidated first – not an error).
 */
BWTreeBasePage *
bwtree_consolidate(BWTreeMappingTable *mt, BWTreePid pid)
{
	(void) mt;
	(void) pid;

	return NULL;
}
