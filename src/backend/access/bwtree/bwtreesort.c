/*-------------------------------------------------------------------------
 *
 * bwtreesort.c
 *    Build support for the Bw-tree index.
 *
 *------------------------------------------------------------------------- */
#include "postgres.h"

#include "access/bwtree.h"
#include "nodes/execnodes.h"
#include "storage/bulk_write.h"

static void
_bwt_init_mapping_page(Page page)
{
	PageInit(page, BLCKSZ, 0);
	memset(PageGetContents(page), 0,
		   BWTREE_MAP_ENTRIES_PER_PAGE * sizeof(BWTreeMapEntry));
	((PageHeader) page)->pd_lower =
		((char *) PageGetContents(page) +
		 BWTREE_MAP_ENTRIES_PER_PAGE * sizeof(BWTreeMapEntry)) -
		(char *) page;
}

static void
_bwt_build_empty_mainfork(Relation index)
{
	Buffer				metabuf;
	Page				metapage;
	BWTreeMetaPageData *metad;
	Buffer				rootbuf;
	Page				rootpage;
	BWTreePid			root_pid;

	metabuf = _bwt_allocbuf(index);
	metapage = BufferGetPage(metabuf);
	_bwt_initmetapage(metapage, InvalidBWTreePid, 0);
	metad = BWTreeMetaPageGetData(metapage);

	rootbuf = _bwt_allocbuf(index);
	rootpage = BufferGetPage(rootbuf);

	root_pid = _bwt_map_alloc_pid(index, metad,
								  metabuf,
								  BufferGetBlockNumber(rootbuf),
								  InvalidBlockNumber);
	_bwt_initpage(rootpage, BWT_ROOT | BWT_LEAF, root_pid, 0);

	metad->bwt_root_pid = root_pid;
	metad->bwt_level = 0;

	MarkBufferDirty(rootbuf);
	MarkBufferDirty(metabuf);

	_bwt_relbuf(index, rootbuf, BWT_WRITE);
	_bwt_relbuf(index, metabuf, BWT_WRITE);
}

IndexBuildResult *
bwtreebuild(Relation heap, Relation index, IndexInfo *indexInfo)
{
	(void) heap;
	(void) index;
	(void) indexInfo;

	elog(ERROR, "bwtree: build interface not implemented yet; buildempty path only is currently supported");
	return NULL;
}

void
bwtreebuildempty(Relation index)
{
	BulkWriteState		*bulkstate;
	BulkWriteBuffer		metabuf;
	BulkWriteBuffer		rootbuf;
	BulkWriteBuffer		mapbuf;
	Page				page;
	BWTreeMetaPageData *metad;
	BWTreeMapEntry		*entries;

	bulkstate = smgr_bulk_start_rel(index, INIT_FORKNUM);

	metabuf = smgr_bulk_get_buf(bulkstate);
	page = (Page) metabuf;
	_bwt_initmetapage(page, 0, 0);
	metad = BWTreeMetaPageGetData(page);
	metad->bwt_next_pid = 1;
	metad->bwt_num_map_pages = 1;
	metad->bwt_map_blknos[0] = 2;
	smgr_bulk_write(bulkstate, BWTREE_METAPAGE, metabuf, true);

	rootbuf = smgr_bulk_get_buf(bulkstate);
	page = (Page) rootbuf;
	_bwt_initpage(page, BWT_ROOT | BWT_LEAF, 0, 0);
	smgr_bulk_write(bulkstate, 1, rootbuf, true);

	mapbuf = smgr_bulk_get_buf(bulkstate);
	page = (Page) mapbuf;
	_bwt_init_mapping_page(page);
	entries = BWTreeMapPageGetEntries(page);
	entries[0].base_blkno = 1;
	entries[0].delta_blkno = InvalidBlockNumber;
	smgr_bulk_write(bulkstate, 2, mapbuf, true);

	smgr_bulk_finish(bulkstate);
}

char *
bwtreebuildphasename(int64 phasenum)
{
	(void) phasenum;

	elog(ERROR, "bwtree: buildphasename interface defined but implementation not written yet");
	return NULL;
}
