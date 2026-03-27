/*-------------------------------------------------------------------------
 *
 * bwtreemap.c
 *    Mapping-table skeletons for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

BlockNumber
_bwt_map_ensure_page(Relation rel, BWTreeMetaPageData *metad, int map_page_idx)
{
	(void) rel;
	(void) metad;
	(void) map_page_idx;

	elog(ERROR, "bwtree: map-ensure-page interface defined but implementation not written yet");
	return InvalidBlockNumber;
}

bool
_bwt_map_lookup(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid,
				BlockNumber *base_blkno, BlockNumber *delta_blkno)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) base_blkno;
	(void) delta_blkno;

	elog(ERROR, "bwtree: map-lookup interface defined but implementation not written yet");
	return false;
}

void
_bwt_map_update(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid,
				BlockNumber base_blkno, BlockNumber delta_blkno)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) base_blkno;
	(void) delta_blkno;

	elog(ERROR, "bwtree: map-update interface defined but implementation not written yet");
}

BWTreePid
_bwt_map_alloc_pid(Relation rel, BWTreeMetaPageData *metad,
				   BlockNumber base_blkno, BlockNumber delta_blkno)
{
	(void) rel;
	(void) metad;
	(void) base_blkno;
	(void) delta_blkno;

	elog(ERROR, "bwtree: map-alloc-pid interface defined but implementation not written yet");
	return InvalidBWTreePid;
}
