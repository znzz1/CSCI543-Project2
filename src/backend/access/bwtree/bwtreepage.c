/*-------------------------------------------------------------------------
 *
 * bwtreepage.c
 *    Buffer/page helper skeletons for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

Buffer
_bwt_getbuf(Relation rel, BlockNumber blkno, int access)
{
	(void) rel;
	(void) blkno;
	(void) access;

	elog(ERROR, "bwtree: getbuf interface defined but implementation not written yet");
	return InvalidBuffer;
}

void
_bwt_relbuf(Relation rel, Buffer buf)
{
	(void) rel;
	(void) buf;

	elog(ERROR, "bwtree: relbuf interface defined but implementation not written yet");
}

Buffer
_bwt_allocbuf(Relation rel)
{
	(void) rel;

	elog(ERROR, "bwtree: allocbuf interface defined but implementation not written yet");
	return InvalidBuffer;
}

void
_bwt_initpage(Page page, uint16 flags, BWTreePid pid, uint32 level)
{
	(void) page;
	(void) flags;
	(void) pid;
	(void) level;

	elog(ERROR, "bwtree: initpage interface defined but implementation not written yet");
}

void
_bwt_initmetapage(Page page, BWTreePid root_pid, uint32 level)
{
	(void) page;
	(void) root_pid;
	(void) level;

	elog(ERROR, "bwtree: initmetapage interface defined but implementation not written yet");
}
