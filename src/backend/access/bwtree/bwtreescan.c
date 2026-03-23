/*-------------------------------------------------------------------------
 *
 * bwtreescan.c
 *    Scan-path scaffolding for the Bw-tree access method.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/relscan.h"

IndexScanDesc
bwtreebeginscan(Relation rel, int nkeys, int norderbys)
{
	return RelationGetIndexScan(rel, nkeys, norderbys);
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
}

bool
bwtreegettuple(IndexScanDesc scan, ScanDirection dir)
{
	(void) dir;

	scan->xs_recheck = false;
	return false;
}

int64
bwtreegetbitmap(IndexScanDesc scan, TIDBitmap *tbm)
{
	(void) scan;
	(void) tbm;

	return 0;
}

void
bwtreeendscan(IndexScanDesc scan)
{
	if (scan->opaque != NULL)
		pfree(scan->opaque);
}

void
bwtreemarkpos(IndexScanDesc scan)
{
	(void) scan;
}

void
bwtreerestrpos(IndexScanDesc scan)
{
	(void) scan;
}

Size
bwtreeestimateparallelscan(int nkeys, int norderbys)
{
	(void) nkeys;
	(void) norderbys;

	return 0;
}

void
bwtreeinitparallelscan(void *target)
{
	(void) target;
}

void
bwtreeparallelrescan(IndexScanDesc scan)
{
	(void) scan;
}
