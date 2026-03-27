/*-------------------------------------------------------------------------
 *
 * bwtreedelta.c
 *    Delta/materialization skeletons for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

BWTreeNodeKind
_bwt_classify_node(Page page)
{
	(void) page;

	elog(ERROR, "bwtree: classify-node interface defined but implementation not written yet");
	return BWT_NODE_BASE_LEAF;
}

void
_bwt_delta_install(Relation rel, BWTreeMetaPageData *metad,
				   BWTreePid pid, BWTreeDeltaType type,
				   IndexTuple itup, BWTreePid related_pid)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) type;
	(void) itup;
	(void) related_pid;

	elog(ERROR, "bwtree: delta-install interface defined but implementation not written yet");
}

int
_bwt_delta_apply(Relation rel, BlockNumber delta_blkno,
				 Page base_page, OffsetNumber *maxoff)
{
	(void) rel;
	(void) delta_blkno;
	(void) base_page;
	(void) maxoff;

	elog(ERROR, "bwtree: delta-apply interface defined but implementation not written yet");
	return 0;
}

bool
_bwt_capture_node_snapshot(Relation rel, BWTreeMetaPageData *metad,
						   BWTreePid pid,
						   BWTreeNodeSnapshot *snapshot)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) snapshot;

	elog(ERROR, "bwtree: capture-node-snapshot interface defined but implementation not written yet");
	return false;
}

void
_bwt_materialize_page(Relation rel, BWTreeMetaPageData *metad,
					  BWTreePid pid, Buffer *basebuf_out,
					  BWTMaterializedPage *mpage)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) basebuf_out;
	(void) mpage;

	elog(ERROR, "bwtree: materialize-page interface defined but implementation not written yet");
}

void
_bwt_free_materialized_page(BWTMaterializedPage *mpage)
{
	(void) mpage;

	elog(ERROR, "bwtree: free-materialized-page interface defined but implementation not written yet");
}

void
_bwt_materialize_node(Relation rel, BWTreeMetaPageData *metad,
					  BWTreePid pid, BWTreeNodeView *view)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) view;

	elog(ERROR, "bwtree: materialize-node interface defined but implementation not written yet");
}

void
_bwt_free_node_view(BWTreeNodeView *view)
{
	(void) view;

	elog(ERROR, "bwtree: free-node-view interface defined but implementation not written yet");
}

bool
_bwt_should_consolidate(Relation rel, BWTreeMetaPageData *metad,
						BWTreePid pid)
{
	(void) rel;
	(void) metad;
	(void) pid;

	elog(ERROR, "bwtree: should-consolidate interface defined but implementation not written yet");
	return false;
}

void
_bwt_consolidate(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid)
{
	(void) rel;
	(void) metad;
	(void) pid;

	elog(ERROR, "bwtree: consolidate interface defined but implementation not written yet");
}
