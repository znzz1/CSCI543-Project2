/*-------------------------------------------------------------------------
 *
 * bwtreesmo.c
 *    Structure Modification Operations (SMO) for the Bw-tree.
 *
 *    Bw-tree splits and merges are performed latch-free via two-phase
 *    delta-record protocols.
 *
 *    === SPLIT (two-phase) ===
 *    Phase 1 – child side:
 *      1. Create a new sibling page with the upper half of keys.
 *      2. Install a SPLIT delta on the original page recording the
 *         split key and the logical pointer (PID) to the new sibling.
 *         (CAS on the original page's mapping-table entry.)
 *
 *    Phase 2 – parent side:
 *      3. Install a SEPARATOR (index-entry) delta on the parent page
 *         that adds the new child PID for the split key range.
 *         (CAS on the parent page's mapping-table entry.)
 *
 *    === MERGE (two-phase) ===
 *    Phase 1 – removed child:
 *      1. Install a REMOVE_NODE delta on the page being removed.
 *
 *    Phase 2 – absorbing sibling + parent:
 *      2. Install a MERGE delta on the left sibling that logically
 *         absorbs the removed page's key range.
 *      3. Remove the separator in the parent for the merged child.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

/*
 * Perform a latch-free split on the leaf/internal page identified by `pid`.
 * `parent_pid` is used for the second phase (separator delta).
 *
 * Returns true on success; false if any CAS step failed (caller retries).
 */
bool
bwtree_smo_split(BWTreeMappingTable *mt,
				 BWTreePid pid,
				 BWTreePid parent_pid)
{
	(void) mt;
	(void) pid;
	(void) parent_pid;

	return false;
}

/*
 * Perform a latch-free merge: `right_pid` is absorbed into `left_pid`.
 * `parent_pid` is used to remove the separator key.
 *
 * Returns true on success; false if any CAS step failed.
 */
bool
bwtree_smo_merge(BWTreeMappingTable *mt,
				 BWTreePid left_pid,
				 BWTreePid right_pid,
				 BWTreePid parent_pid)
{
	(void) mt;
	(void) left_pid;
	(void) right_pid;
	(void) parent_pid;

	return false;
}
