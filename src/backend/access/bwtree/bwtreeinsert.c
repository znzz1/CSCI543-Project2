/*-------------------------------------------------------------------------
 *
 * bwtreeinsert.c
 *    Insert entrypoint skeleton for the Bw-tree index.
 *
 *------------------------------------------------------------------------- */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/genam.h"
#include "access/nbtree.h"
#include "access/skey.h"
#include "nodes/execnodes.h"

static ScanKey
_bwt_build_insert_scankey(Relation rel, Datum *values, bool *isnull, int *nkeys_out)
{
	int			indnkeyatts;
	ScanKeyData *skey;
	int			i;

	if (nkeys_out == NULL)
		elog(ERROR, "bwtree: insert scankey requires nkeys output pointer");
	if (values == NULL || isnull == NULL)
		elog(ERROR, "bwtree: insert scankey requires values/isnull");

	indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	*nkeys_out = indnkeyatts;

	if (indnkeyatts <= 0)
		return NULL;

	skey = (ScanKeyData *) palloc(sizeof(ScanKeyData) * indnkeyatts);
	for (i = 0; i < indnkeyatts; i++)
	{
		FmgrInfo   *procinfo;
		int			flags;

		procinfo = index_getprocinfo(rel, i + 1, BWTORDER_PROC);
		flags = (isnull[i] ? SK_ISNULL : 0) |
			(rel->rd_indoption[i] << SK_BT_INDOPTION_SHIFT);

		ScanKeyEntryInitializeWithInfo(&skey[i],
									   flags,
									   (AttrNumber) (i + 1),
									   InvalidStrategy,
									   InvalidOid,
									   rel->rd_indcollation[i],
									   procinfo,
									   values[i]);
	}

	return skey;
}

bool
bwtreeinsert(Relation rel, Datum *values, bool *isnull,
			 ItemPointer ht_ctid, Relation heapRel,
			 IndexUniqueCheck checkUnique,
			 bool indexUnchanged,
			 IndexInfo *indexInfo)
{
	Buffer				metabuf;
	Page				metapage;
	BWTreeMetaPageData *metad;
	BWTreePid			target_pid;
	BWTreePid			parent_pid;
	ScanKey				route_key;
	int					route_nkeys;
	IndexTuple			itup;

	(void) heapRel;
	(void) indexUnchanged;
	(void) indexInfo;

	if (ht_ctid == NULL)
		elog(ERROR, "bwtree: insert requires a valid heap TID");

	/*
	 * Correctness-first trade-off:
	 *
	 * UNIQUE semantics are intentionally not implemented yet. We fail fast
	 * instead of accepting requests that could silently violate correctness.
	 */
	if (checkUnique != UNIQUE_CHECK_NO)
		elog(ERROR, "bwtree: unique-check insert is not supported in current stage");

	/*
	 * Correctness-first trade-off:
	 *
	 * We serialize insert through the metapage write latch. This keeps
	 * routing + publish deterministic while SMO/CAS paths are incomplete.
	 * Throughput is lower, but correctness is simpler to reason about.
	 */
	metabuf = _bwt_getbuf(rel, BWTREE_METAPAGE, BWT_WRITE);
	metapage = BufferGetPage(metabuf);
	metad = BWTreeMetaPageGetData(metapage);

	if (metad->bwt_magic != BWTREE_MAGIC || metad->bwt_version != BWTREE_VERSION)
	{
		_bwt_relbuf(rel, metabuf, BWT_WRITE);
		elog(ERROR, "bwtree: invalid metapage (magic/version mismatch)");
	}

	target_pid = metad->bwt_root_pid;
	parent_pid = InvalidBWTreePid;
	if (target_pid == InvalidBWTreePid)
	{
		_bwt_relbuf(rel, metabuf, BWT_WRITE);
		elog(ERROR, "bwtree: invalid root PID for insert");
	}

	route_key = _bwt_build_insert_scankey(rel, values, isnull, &route_nkeys);
	target_pid = _bwt_search_leaf_with_parent(rel, metad,
											  route_key, route_nkeys,
											  &parent_pid);
	if (route_key != NULL)
		pfree(route_key);

	if (target_pid == InvalidBWTreePid)
	{
		_bwt_relbuf(rel, metabuf, BWT_WRITE);
		elog(ERROR, "bwtree: insert cannot find target leaf");
	}

	itup = index_form_tuple(RelationGetDescr(rel), values, isnull);
	itup->t_tid = *ht_ctid;

	_bwt_insert_item(rel, metad, target_pid, parent_pid, itup, true);

	metad->bwt_num_tuples += 1.0;
	MarkBufferDirty(metabuf);
	_bwt_relbuf(rel, metabuf, BWT_WRITE);

	pfree(itup);
	return false;
}
