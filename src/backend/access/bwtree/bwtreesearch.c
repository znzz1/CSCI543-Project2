/*-------------------------------------------------------------------------
 *
 * bwtreesearch.c
 *    Search skeletons for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

void
_bwt_begin_traverse(BWTreeContext *ctx, MemoryContext memcxt)
{
	(void) ctx;
	(void) memcxt;

	elog(ERROR, "bwtree: begin-traverse interface defined but implementation not written yet");
}

void
_bwt_finish_traverse(BWTreeContext *ctx)
{
	(void) ctx;

	elog(ERROR, "bwtree: finish-traverse interface defined but implementation not written yet");
}

BWTreePid
_bwt_search_leaf(Relation rel, BWTreeMetaPageData *metad,
				  ScanKey scankey, int nkeys)
{
	(void) rel;
	(void) metad;
	(void) scankey;
	(void) nkeys;

	elog(ERROR, "bwtree: search-leaf interface defined but implementation not written yet");
	return InvalidBWTreePid;
}

BWTreePid
_bwt_search_leaf_with_parent(Relation rel, BWTreeMetaPageData *metad,
							 ScanKey scankey, int nkeys,
							 BWTreePid *parent_pid)
{
	(void) rel;
	(void) metad;
	(void) scankey;
	(void) nkeys;
	(void) parent_pid;

	elog(ERROR, "bwtree: search-leaf-with-parent interface defined but implementation not written yet");
	return InvalidBWTreePid;
}

bool
_bwt_descend_to_leaf(Relation rel, BWTreeMetaPageData *metad,
					 ScanKey scankey, int nkeys,
					 BWTreeContext *ctx,
					 BWTreeNodeSnapshot *leaf_snapshot)
{
	(void) rel;
	(void) metad;
	(void) scankey;
	(void) nkeys;
	(void) ctx;
	(void) leaf_snapshot;

	elog(ERROR, "bwtree: descend-to-leaf interface defined but implementation not written yet");
	return false;
}

int32
_bwt_compare(Relation rel, ScanKey scankey, int nkeys,
			 Page page, OffsetNumber offnum)
{
	(void) rel;
	(void) scankey;
	(void) nkeys;
	(void) page;
	(void) offnum;

	elog(ERROR, "bwtree: compare interface defined but implementation not written yet");
	return 0;
}
