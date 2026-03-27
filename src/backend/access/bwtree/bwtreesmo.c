/*-------------------------------------------------------------------------
 *
 * bwtreesmo.c
 *    Structure-modification skeleton for the Bw-tree index.
 *
 *------------------------------------------------------------------------- */
#include "postgres.h"

#include "access/bwtree.h"

bool
_bwt_prepare_split(Relation rel, BWTreeMetaPageData *metad,
				   BWTreePid pid, BWTreeNodeView *view)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) view;

	elog(ERROR, "bwtree: prepare-split interface defined but implementation not written yet");
	return false;
}

void
_bwt_finish_split(Relation rel, BWTreeMetaPageData *metad,
				  BWTreePid pid, BWTreePid parent_pid,
				  BWTreeNodeView *view)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) parent_pid;
	(void) view;

	elog(ERROR, "bwtree: finish-split interface defined but implementation not written yet");
}

void
_bwt_install_new_root(Relation rel, BWTreeMetaPageData *metad,
					  BWTreePid left_pid, BWTreePid right_pid,
					  IndexTuple sep_itup, uint32 child_level)
{
	(void) rel;
	(void) metad;
	(void) left_pid;
	(void) right_pid;
	(void) sep_itup;
	(void) child_level;

	elog(ERROR, "bwtree: install-new-root interface defined but implementation not written yet");
}

void
_bwt_insert_item(Relation rel, BWTreeMetaPageData *metad,
				 BWTreePid pid, BWTreePid parent_pid,
				 IndexTuple itup, bool is_leaf)
{
	(void) rel;
	(void) metad;
	(void) pid;
	(void) parent_pid;
	(void) itup;
	(void) is_leaf;

	elog(ERROR, "bwtree: SMO insert interface defined but implementation not written yet");
}

void
_bwt_split(Relation rel, BWTreeMetaPageData *metad,
		   Buffer leafbuf, BWTreePid leaf_pid,
		   BWTreePid parent_pid)
{
	(void) rel;
	(void) metad;
	(void) leafbuf;
	(void) leaf_pid;
	(void) parent_pid;

	elog(ERROR, "bwtree: split interface defined but implementation not written yet");
}
