/*-------------------------------------------------------------------------
 *
 * bwtreeinsert.c
 *    Insert path for the Bw-tree index.
 *
 *    Insertions work by:
 *      1. Searching for the target leaf PID
 *      2. Installing an INSERT delta record on that leaf's delta chain
 *      3. Optionally triggering consolidation if the chain is too long
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/genam.h"
#include "catalog/index.h"
#include "nodes/execnodes.h"

/*
 * bwtreeinsert -- AM callback for index tuple insertion.
 */
bool
bwtreeinsert(Relation rel, Datum *values, bool *isnull,
			 ItemPointer ht_ctid, Relation heapRel,
			 IndexUniqueCheck checkUnique,
			 bool indexUnchanged,
			 IndexInfo *indexInfo)
{
	IndexTuple		itup;
	BWTreePid		leaf_pid;
	Buffer			metabuf;
	Page			metapage;
	BWTreeMetaPageData *metad;
	ScanKey			scankey;
	int				nkeys;

	itup = index_form_tuple(RelationGetDescr(rel), values, isnull);
	itup->t_tid = *ht_ctid;

	/* read meta page */
	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
	metapage = BufferGetPage(metabuf);
	metad = BWTreeMetaPageGetData(metapage);

	/* build a scan key from the tuple for tree descent */
	{
		TupleDesc	itupdesc = RelationGetDescr(rel);
		int			i;

		nkeys = IndexRelationGetNumberOfKeyAttributes(rel);
		scankey = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);

		for (i = 0; i < nkeys; i++)
		{
			Datum	val;
			bool	null;

			val = index_getattr(itup, i + 1, itupdesc, &null);

			ScanKeyEntryInitialize(&scankey[i],
								   null ? SK_ISNULL : 0,
								   i + 1,
								   BTEqualStrategyNumber,
								   InvalidOid,
								   rel->rd_indcollation[i],
								   rel->rd_support[i],
								   val);
		}
	}

	/* find the target leaf */
	leaf_pid = _bwt_search_leaf(rel, metad, scankey, nkeys);

	_bwt_relbuf(rel, metabuf);

	/* install INSERT delta */
	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
	metapage = BufferGetPage(metabuf);
	metad = BWTreeMetaPageGetData(metapage);

	_bwt_delta_install(rel, metad, leaf_pid,
					   BW_DELTA_INSERT, itup, InvalidBWTreePid);

	/* update tuple count */
	_bwt_relbuf(rel, metabuf);

	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_WRITE);
	metapage = BufferGetPage(metabuf);
	metad = BWTreeMetaPageGetData(metapage);
	metad->bwt_num_tuples++;
	MarkBufferDirty(metabuf);
	_bwt_relbuf(rel, metabuf);

	/* check if consolidation is needed */
	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_READ);
	metapage = BufferGetPage(metabuf);
	metad = BWTreeMetaPageGetData(metapage);

	if (_bwt_should_consolidate(rel, metad, leaf_pid))
		_bwt_consolidate(rel, metad, leaf_pid);

	_bwt_relbuf(rel, metabuf);

	pfree(scankey);
	pfree(itup);

	return false;
}
