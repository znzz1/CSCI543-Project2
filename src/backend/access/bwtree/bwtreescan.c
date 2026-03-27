/*-------------------------------------------------------------------------
 *
 * bwtreescan.c
 *    Scan skeleton for the Bw-tree index.
 *
 *------------------------------------------------------------------------- */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/relscan.h"
#include "nodes/tidbitmap.h"

IndexScanDesc
bwtreebeginscan(Relation rel, int nkeys, int norderbys)
{
	(void) rel;
	(void) nkeys;
	(void) norderbys;

	elog(ERROR, "bwtree: beginscan interface defined but implementation not written yet");
	return NULL;
}

void
bwtreerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			 ScanKey orderbys, int norderbys)
{
	(void) scan;
	(void) scankey;
	(void) nscankeys;
	(void) orderbys;
	(void) norderbys;

	elog(ERROR, "bwtree: rescan interface defined but implementation not written yet");
}

bool
bwtreegettuple(IndexScanDesc scan, ScanDirection dir)
{
	(void) scan;
	(void) dir;

	elog(ERROR, "bwtree: tuple scan interface defined but implementation not written yet");
	return false;
}

int64
bwtreegetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	(void) scan;
	(void) tbm;

	elog(ERROR, "bwtree: bitmap scan interface defined but implementation not written yet");
	return 0;
}

void
bwtreeendscan(IndexScanDesc scan)
{
	(void) scan;

	elog(ERROR, "bwtree: endscan interface defined but implementation not written yet");
}
