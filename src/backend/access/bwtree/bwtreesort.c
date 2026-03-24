/*-------------------------------------------------------------------------
 *
 * bwtreesort.c
 *    Build (CREATE INDEX) for the Bw-tree index.
 *
 *    Scan the heap, collect tuples, sort them, build leaf pages
 *    bottom-up, then create internal pages, mapping pages, and meta.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "access/tableam.h"
#include "catalog/index.h"
#include "commands/progress.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "utils/rel.h"
#include "utils/sortsupport.h"

typedef struct BWTBuildPage
{
	BWTreePid		pid;
	BlockNumber		blkno;
} BWTBuildPage;

typedef struct BWTBuildState
{
	Relation	heap;
	Relation	index;
	double		indtuples;

	IndexTuple *tuples;
	int			num_tuples;
	int			max_tuples;
} BWTBuildState;

static Relation _bwt_sort_rel;
static TupleDesc _bwt_sort_desc;
static FmgrInfo _bwt_sort_cmp_func;
static Oid _bwt_sort_collation;

static void bwtbuildCallback(Relation index, ItemPointer tid,
							 Datum *values, bool *isnull,
							 bool tupleIsAlive, void *state);

static int
_bwt_tuple_compare(const void *a, const void *b)
{
	IndexTuple	ta = *(IndexTuple *) a;
	IndexTuple	tb = *(IndexTuple *) b;
	Datum		da, db;
	bool		na, nb;

	da = index_getattr(ta, 1, _bwt_sort_desc, &na);
	db = index_getattr(tb, 1, _bwt_sort_desc, &nb);

	if (na && nb) return 0;
	if (na) return 1;
	if (nb) return -1;

	return DatumGetInt32(FunctionCall2Coll(&_bwt_sort_cmp_func,
										   _bwt_sort_collation,
										   da, db));
}

static int
_bwt_build_leaf_pages(Relation rel, IndexTuple *tuples, int ntuples,
					  BWTBuildPage *leaves, int max_leaves)
{
	int			nleaves = 0;
	int			i;
	Buffer		curbuf = InvalidBuffer;
	Page		curpage = NULL;

	for (i = 0; i < ntuples; i++)
	{
		IndexTuple	itup = tuples[i];
		Size		itupsz = MAXALIGN(IndexTupleSize(itup));

		if (!BufferIsValid(curbuf) ||
			PageGetFreeSpace(curpage) < itupsz + sizeof(ItemIdData))
		{
			if (BufferIsValid(curbuf))
			{
				MarkBufferDirty(curbuf);
				_bwt_relbuf(rel, curbuf);
			}

			if (nleaves >= max_leaves)
				elog(ERROR, "bwtree: too many leaf pages during build");

			curbuf = _bwt_allocbuf(rel);
			curpage = BufferGetPage(curbuf);
			_bwt_initpage(curpage, BWT_LEAF, InvalidBWTreePid, 0);

			leaves[nleaves].blkno = BufferGetBlockNumber(curbuf);
			leaves[nleaves].pid = InvalidBWTreePid;
			nleaves++;
		}

		if (PageAddItem(curpage, (Item) itup, IndexTupleSize(itup),
						InvalidOffsetNumber, false, false) == InvalidOffsetNumber)
			elog(ERROR, "bwtree: failed to add tuple to leaf page");
	}

	if (BufferIsValid(curbuf))
	{
		MarkBufferDirty(curbuf);
		_bwt_relbuf(rel, curbuf);
	}

	return nleaves;
}

static int
_bwt_build_internal_pages(Relation rel, TupleDesc itupdesc,
						  BWTBuildPage *children, int nchildren,
						  BWTBuildPage *parents, int max_parents,
						  uint32 level)
{
	int			nparents = 0;
	int			i;
	Buffer		curbuf = InvalidBuffer;
	Page		curpage = NULL;

	for (i = 0; i < nchildren; i++)
	{
		IndexTuple	sep_itup;
		bool		need_new = false;

		if (!BufferIsValid(curbuf))
			need_new = true;
		else if (PageGetFreeSpace(curpage) <
				 MAXALIGN(sizeof(IndexTupleData)) + sizeof(ItemIdData) + 64)
			need_new = true;

		if (need_new)
		{
			if (BufferIsValid(curbuf))
			{
				MarkBufferDirty(curbuf);
				_bwt_relbuf(rel, curbuf);
			}

			if (nparents >= max_parents)
				elog(ERROR, "bwtree: too many internal pages during build");

			curbuf = _bwt_allocbuf(rel);
			curpage = BufferGetPage(curbuf);
			_bwt_initpage(curpage, 0, InvalidBWTreePid, level);

			parents[nparents].blkno = BufferGetBlockNumber(curbuf);
			parents[nparents].pid = InvalidBWTreePid;
			nparents++;
		}

		{
			Datum		values[INDEX_MAX_KEYS];
			bool		nulls[INDEX_MAX_KEYS];
			int			natts = itupdesc->natts;
			int			j;

			if (i == 0 || PageGetMaxOffsetNumber(curpage) == 0)
			{
				for (j = 0; j < natts; j++)
				{
					values[j] = (Datum) 0;
					nulls[j] = true;
				}
			}
			else
			{
				Buffer		childbuf;
				Page		childpage;
				IndexTuple	first;

				childbuf = _bwt_getbuf(rel, children[i].blkno, BWT_READ);
				childpage = BufferGetPage(childbuf);

				if (PageGetMaxOffsetNumber(childpage) >= FirstOffsetNumber)
				{
					first = (IndexTuple) PageGetItem(childpage,
									PageGetItemId(childpage, FirstOffsetNumber));
					for (j = 0; j < natts; j++)
						values[j] = index_getattr(first, j + 1,
												  itupdesc, &nulls[j]);
				}
				else
				{
					for (j = 0; j < natts; j++)
					{
						values[j] = (Datum) 0;
						nulls[j] = true;
					}
				}

				_bwt_relbuf(rel, childbuf);
			}

			sep_itup = index_form_tuple(itupdesc, values, nulls);
			BWTreeTupleSetDownLink(sep_itup, children[i].pid);

			if (PageAddItem(curpage, (Item) sep_itup,
							IndexTupleSize(sep_itup),
							InvalidOffsetNumber, false, false)
				== InvalidOffsetNumber)
				elog(ERROR, "bwtree: failed to add separator to internal page");

			pfree(sep_itup);
		}
	}

	if (BufferIsValid(curbuf))
	{
		MarkBufferDirty(curbuf);
		_bwt_relbuf(rel, curbuf);
	}

	return nparents;
}

static void
_bwt_link_leaves(Relation rel, BWTBuildPage *leaves, int nleaves)
{
	int i;

	for (i = 0; i < nleaves; i++)
	{
		Buffer		buf;
		Page		page;
		BWTreePageOpaque opaque;

		buf = _bwt_getbuf(rel, leaves[i].blkno, BWT_WRITE);
		page = BufferGetPage(buf);
		opaque = BWTreePageGetOpaque(page);

		opaque->bwto_pid = leaves[i].pid;

		if (i > 0)
			opaque->bwto_prev = leaves[i - 1].blkno;
		if (i < nleaves - 1)
			opaque->bwto_next = leaves[i + 1].blkno;

		MarkBufferDirty(buf);
		_bwt_relbuf(rel, buf);
	}
}

IndexBuildResult *
bwtreebuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	BWTBuildState	buildstate;
	double			reltuples;
	int				nleaves;
	int				nchildren;
	int				nparents;
	uint32			level;
	BWTreePid		root_pid;
	Buffer			metabuf;
	Page			metapage;
	BWTreeMetaPageData *metad;
	int				max_pages = 10000;
	BWTBuildPage   *level_a;
	BWTBuildPage   *level_b;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
			 RelationGetRelationName(index));

	metabuf = _bwt_allocbuf(index);
	metapage = BufferGetPage(metabuf);
	_bwt_initmetapage(metapage, InvalidBWTreePid, 0);
	metad = BWTreeMetaPageGetData(metapage);
	MarkBufferDirty(metabuf);

	level_a = (BWTBuildPage *) palloc(sizeof(BWTBuildPage) * max_pages);
	level_b = (BWTBuildPage *) palloc(sizeof(BWTBuildPage) * max_pages);

	buildstate.heap = heap;
	buildstate.index = index;
	buildstate.indtuples = 0;
	buildstate.max_tuples = 10000;
	buildstate.num_tuples = 0;
	buildstate.tuples = (IndexTuple *) palloc(sizeof(IndexTuple) * buildstate.max_tuples);

	reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
									   bwtbuildCallback,
									   (void *) &buildstate, NULL);

	/* sort collected tuples using the index's comparison function */
	_bwt_sort_desc = RelationGetDescr(index);
	_bwt_sort_rel = index;
	_bwt_sort_collation = index->rd_indcollation[0];
	fmgr_info(index->rd_support[0], &_bwt_sort_cmp_func);

	qsort(buildstate.tuples, buildstate.num_tuples,
		  sizeof(IndexTuple), _bwt_tuple_compare);

	nleaves = _bwt_build_leaf_pages(index, buildstate.tuples,
									buildstate.num_tuples,
									level_a, max_pages);

	{
		int i;
		for (i = 0; i < buildstate.num_tuples; i++)
			pfree(buildstate.tuples[i]);
		pfree(buildstate.tuples);
	}

	{
		int i;
		for (i = 0; i < nleaves; i++)
			level_a[i].pid = _bwt_map_alloc_pid(index, metad,
												 level_a[i].blkno,
												 InvalidBlockNumber);
	}

	_bwt_link_leaves(index, level_a, nleaves);

	nchildren = nleaves;
	level = 1;
	{
		BWTBuildPage *cur_children = level_a;
		BWTBuildPage *cur_parents = level_b;

		while (nchildren > 1)
		{
			int i;

			nparents = _bwt_build_internal_pages(index,
												 RelationGetDescr(index),
												 cur_children, nchildren,
												 cur_parents, max_pages,
												 level);

			for (i = 0; i < nparents; i++)
				cur_parents[i].pid = _bwt_map_alloc_pid(index, metad,
														cur_parents[i].blkno,
														InvalidBlockNumber);

			nchildren = nparents;
			level++;

			{
				BWTBuildPage *tmp = cur_children;
				cur_children = cur_parents;
				cur_parents = tmp;
			}
		}

		if (nchildren == 1)
			root_pid = cur_children[0].pid;
		else
			root_pid = InvalidBWTreePid;
	}

	metad->bwt_root_pid = root_pid;
	metad->bwt_level = (level > 0) ? level - 1 : 0;
	metad->bwt_num_tuples = buildstate.indtuples;

	if (root_pid != InvalidBWTreePid)
	{
		BlockNumber root_blkno;
		BlockNumber dummy_delta;

		if (_bwt_map_lookup(index, metad, root_pid, &root_blkno, &dummy_delta))
		{
			Buffer		rbuf = _bwt_getbuf(index, root_blkno, BWT_WRITE);
			Page		rpage = BufferGetPage(rbuf);
			BWTreePageOpaque ropaque = BWTreePageGetOpaque(rpage);

			ropaque->bwto_flags |= BWT_ROOT;
			MarkBufferDirty(rbuf);
			_bwt_relbuf(index, rbuf);
		}
	}

	MarkBufferDirty(metabuf);
	_bwt_relbuf(index, metabuf);

	pfree(level_a);
	pfree(level_b);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = reltuples;
	result->index_tuples = buildstate.indtuples;

	return result;
}

static void
bwtbuildCallback(Relation index, ItemPointer tid,
				 Datum *values, bool *isnull,
				 bool tupleIsAlive, void *state)
{
	BWTBuildState *buildstate = (BWTBuildState *) state;
	IndexTuple itup;

	if (buildstate->num_tuples >= buildstate->max_tuples)
	{
		buildstate->max_tuples *= 2;
		buildstate->tuples = (IndexTuple *)
			repalloc(buildstate->tuples,
					 sizeof(IndexTuple) * buildstate->max_tuples);
	}

	itup = index_form_tuple(RelationGetDescr(buildstate->index), values, isnull);
	itup->t_tid = *tid;

	buildstate->tuples[buildstate->num_tuples++] = itup;
	buildstate->indtuples++;
}

void
bwtreebuildempty(Relation index)
{
}

char *
bwtreebuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		default:
			return NULL;
	}
}
