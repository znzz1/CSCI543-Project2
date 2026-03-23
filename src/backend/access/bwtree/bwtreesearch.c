/*-------------------------------------------------------------------------
 *
 * bwtreesearch.c
 *    Tree traversal for the Bw-tree access method.
 *
 *    Searching a Bw-tree works like a regular B+tree traversal, except:
 *
 *      1. All page accesses go through the mapping table (PID -> node).
 *      2. At each node, the delta chain is walked first; if the search
 *         key matches a delta record we can return immediately.
 *         If a SPLIT delta is encountered and the key falls into the
 *         split range, we follow the logical pointer to the new sibling.
 *      3. If no delta matches, fall through to binary search on the
 *         consolidated base page.
 *      4. No latches are acquired at any point.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

/*
 * Descend from root_pid to find the leaf page whose key range contains
 * `key`.  Returns the leaf PID.
 */
BWTreePid
bwtree_search_leaf(BWTreeMappingTable *mt,
				   BWTreePid root_pid,
				   Datum key, ScanKey scankey)
{
	(void) mt;
	(void) root_pid;
	(void) key;
	(void) scankey;

	return 0;
}

/*
 * Full point lookup: descend to the leaf, then search the delta chain
 * and base page for the key.
 */
bool
bwtree_search(BWTreeMappingTable *mt,
			  BWTreePid root_pid,
			  Datum key, ScanKey scankey,
			  ItemPointer result_tid)
{
	(void) mt;
	(void) root_pid;
	(void) key;
	(void) scankey;
	(void) result_tid;

	return false;
}

/*
 * Given a leaf PID, return the PID of the next leaf to the right
 * (for forward range scans).
 */
BWTreePid
bwtree_search_next_leaf(BWTreeMappingTable *mt, BWTreePid cur_pid)
{
	(void) mt;
	(void) cur_pid;

	return 0;
}
