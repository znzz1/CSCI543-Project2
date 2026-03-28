/*-------------------------------------------------------------------------
 *
 * bwtree.h
 *    Header for PostgreSQL Bw-tree index access method.
 *
 *    The Bw-tree is a latch-free index (ICDE 2013) that stores all data
 *    in PostgreSQL buffer pages.  Core concepts:
 *
 *      1. Mapping Table  – PID -> (base_blkno, delta_blkno)
 *      2. Delta Pages    – modifications prepended as overflow page chains
 *      3. Consolidation  – merge delta chain into a new base page
 *      4. SMO            – split/merge via delta records
 *
 *    All state lives in shared buffer pages so every backend sees the
 *    same index.
 *
 * src/include/access/bwtree.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BWTREE_H
#define BWTREE_H

#include "access/amapi.h"
#include "access/itup.h"
#include "access/sdir.h"
#include "commands/vacuum.h"
#include "nodes/pathnodes.h"
#include "storage/bufmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

/* ----------------------------------------------------------------
 *  Constants
 * ---------------------------------------------------------------- */

#define BWTREE_MAGIC        0x42575452
#define BWTREE_VERSION      1
#define BWTREE_METAPAGE     0
#define BWTREE_PAGE_ID      0xFF90

#define BWT_READ            BUFFER_LOCK_SHARE
#define BWT_WRITE           BUFFER_LOCK_EXCLUSIVE
#define BWT_NOLOCK          (-1)

/* Page type flags stored in bwto_flags */
#define BWT_LEAF            (1 << 0)
#define BWT_ROOT            (1 << 1)
#define BWT_DELETED         (1 << 2)
#define BWT_META            (1 << 3)
#define BWT_DELTA           (1 << 4)

/* AM support procedure numbers (reuse btree strategies) */
#define BWTORDER_PROC       1
#define BWTNProcs           1

#define BWTREE_DELTA_CHAIN_THRESHOLD    1
#define BWTREE_MAX_MAP_PAGES            200

/* ----------------------------------------------------------------
 *  Logical page identifier
 * ---------------------------------------------------------------- */

typedef uint32 BWTreePid;
#define InvalidBWTreePid    ((BWTreePid) 0xFFFFFFFF)

/* ----------------------------------------------------------------
 *  Meta page  (block 0, accessed via PageGetContents)
 * ---------------------------------------------------------------- */

typedef struct BWTreeMetaPageData
{
	uint32      bwt_magic;
	uint32      bwt_version;
	BWTreePid   bwt_root_pid;
	uint32      bwt_level;
	BWTreePid   bwt_next_pid;
	double      bwt_num_tuples;
	uint32      bwt_num_map_pages;
	BlockNumber bwt_map_blknos[BWTREE_MAX_MAP_PAGES];
} BWTreeMetaPageData;

#define BWTreeMetaPageGetData(page) \
	((BWTreeMetaPageData *) PageGetContents(page))

/* ----------------------------------------------------------------
 *  Mapping page
 *
 *  Each page stores a flat array of BWTreeMapEntry accessed via
 *  PageGetContents.  PID N lives on mapping page N / ENTRIES_PER_PAGE
 *  at offset N % ENTRIES_PER_PAGE.
 * ---------------------------------------------------------------- */

typedef struct BWTreeMapEntry
{
	BlockNumber base_blkno;
	BlockNumber delta_blkno;
} BWTreeMapEntry;

#define BWTREE_MAP_ENTRIES_PER_PAGE \
	((int)((BLCKSZ - MAXALIGN(SizeOfPageHeaderData)) / sizeof(BWTreeMapEntry)))

#define BWTreeMapPageGetEntries(page) \
	((BWTreeMapEntry *) PageGetContents(page))

/* ----------------------------------------------------------------
 *  Page opaque  (special space for base + delta pages)
 * ---------------------------------------------------------------- */

typedef struct BWTreePageOpaqueData
{
	BlockNumber bwto_prev;      /* left sibling  (base pages) */
	BlockNumber bwto_next;      /* right sibling (base) / next delta (delta) */
	BWTreePid   bwto_pid;       /* logical PID of this page */
	uint32      bwto_level;     /* 0 = leaf */
	uint16      bwto_flags;     /* BWT_LEAF | BWT_ROOT | BWT_DELTA … */
	uint16      bwto_page_id;   /* BWTREE_PAGE_ID for identification */
} BWTreePageOpaqueData;

typedef BWTreePageOpaqueData *BWTreePageOpaque;

#define BWTreePageGetOpaque(page) \
	((BWTreePageOpaque) PageGetSpecialPointer(page))

#define BWTreePageIsLeaf(opaque)    (((opaque)->bwto_flags & BWT_LEAF) != 0)
#define BWTreePageIsRoot(opaque)    (((opaque)->bwto_flags & BWT_ROOT) != 0)
#define BWTreePageIsDeleted(opaque) (((opaque)->bwto_flags & BWT_DELETED) != 0)
#define BWTreePageIsDelta(opaque)   (((opaque)->bwto_flags & BWT_DELTA) != 0)

/* Internal-page downlink helpers (PID stored in t_tid block field) */
#define BWTreeTupleGetDownLink(itup) \
	((BWTreePid) ItemPointerGetBlockNumberNoCheck(&(itup)->t_tid))

#define BWTreeTupleSetDownLink(itup, pid) \
	ItemPointerSetBlockNumber(&(itup)->t_tid, (BlockNumber)(pid))

/* ----------------------------------------------------------------
 *  Delta record  (stored as items on delta pages)
 * ---------------------------------------------------------------- */

typedef enum BWTreeDeltaType
{
	BW_DELTA_INSERT,
	BW_DELTA_DELETE,
	BW_DELTA_SPLIT,
	BW_DELTA_SEPARATOR
} BWTreeDeltaType;

typedef struct BWTreeDeltaRecordData
{
	BWTreeDeltaType type;
	BWTreePid       related_pid;    /* SPLIT: sibling; SEPARATOR/inner DELETE: child */
	ItemPointerData target_tid;     /* leaf DELETE: exact heap TID to remove */
	uint16          data_len;       /* byte length of trailing IndexTuple key/payload */
} BWTreeDeltaRecordData;

#define SizeOfBWTreeDeltaRecord sizeof(BWTreeDeltaRecordData)

#define BWTreeDeltaRecordGetTuple(drec) \
	((drec)->data_len > 0 \
	 ? (IndexTuple)((char *)(drec) + SizeOfBWTreeDeltaRecord) \
	 : NULL)

#define BWTreeDeltaRecordSize(drec) \
	((Size)(SizeOfBWTreeDeltaRecord + (drec)->data_len))

/* ----------------------------------------------------------------
 *  Scan opaque (private state for an index scan)
 * ---------------------------------------------------------------- */

typedef struct BWTreeScanPosItem
{
	ItemPointerData heapTid;
} BWTreeScanPosItem;

typedef struct BWTreeScanOpaqueData
{
	bool        started;
	BWTreePid   cur_leaf_pid;
	int         cur_item;
	int         num_items;
	BWTreeScanPosItem *items;

	int         numberOfKeys;
	int         keyDataCapacity;
	ScanKey     keyData;
} BWTreeScanOpaqueData;

typedef BWTreeScanOpaqueData *BWTreeScanOpaque;

typedef struct BWTMaterializedPage
{
	IndexTuple *items;
	int			nitems;
	BlockNumber base_blkno;
	BlockNumber prev_blkno;
	BlockNumber next_blkno;
	uint16		flags;
	uint32		level;
} BWTMaterializedPage;

typedef enum BWTreeNodeKind
{
	BWT_NODE_BASE_LEAF,
	BWT_NODE_BASE_INNER,
	BWT_NODE_DELTA_INSERT,
	BWT_NODE_DELTA_DELETE,
	BWT_NODE_DELTA_SPLIT,
	BWT_NODE_DELTA_SEPARATOR
} BWTreeNodeKind;

typedef enum BWTreeNodeState
{
	BWT_STATE_STABLE,
	BWT_STATE_SPLIT_PENDING
} BWTreeNodeState;

typedef struct BWTreeNodeSnapshot
{
	BWTreePid	pid;
	BlockNumber	base_blkno;
	BlockNumber	delta_blkno;
	uint32		level;
	bool		is_leaf;
	bool		is_root;
} BWTreeNodeSnapshot;

typedef struct BWTreeNodeView
{
	BWTreeNodeSnapshot snapshot;
	BWTMaterializedPage page;
	BWTreeNodeState state;
	bool		has_split_delta;
	BWTreePid	split_right_pid;
	IndexTuple	split_separator;
} BWTreeNodeView;

typedef struct BWTreeContext
{
	BWTreePid	stack_pid[64];
	int			depth;
	MemoryContext memcxt;
} BWTreeContext;

/* ================================================================
 *  PUBLIC AM CALLBACKS
 * ================================================================ */

extern Datum bwtreehandler(PG_FUNCTION_ARGS);

extern IndexBuildResult *bwtreebuild(Relation heap, Relation index,
									 struct IndexInfo *indexInfo);
extern void bwtreebuildempty(Relation index);
extern bool bwtreeinsert(Relation rel, Datum *values, bool *isnull,
						 ItemPointer ht_ctid, Relation heapRel,
						 IndexUniqueCheck checkUnique,
						 bool indexUnchanged,
						 struct IndexInfo *indexInfo);
extern IndexScanDesc bwtreebeginscan(Relation rel, int nkeys, int norderbys);
extern void bwtreerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						 ScanKey orderbys, int norderbys);
extern bool bwtreegettuple(IndexScanDesc scan, ScanDirection dir);
extern int64 bwtreegetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void bwtreeendscan(IndexScanDesc scan);
extern IndexBulkDeleteResult *bwtreebulkdelete(IndexVacuumInfo *info,
											   IndexBulkDeleteResult *stats,
											   IndexBulkDeleteCallback callback,
											   void *callback_state);
extern IndexBulkDeleteResult *bwtreevacuumcleanup(IndexVacuumInfo *info,
												  IndexBulkDeleteResult *stats);
extern void bwtreecostestimate(PlannerInfo *root, IndexPath *path,
							   double loop_count,
							   Cost *indexStartupCost, Cost *indexTotalCost,
							   Selectivity *indexSelectivity,
							   double *indexCorrelation, double *indexPages);
extern bytea *bwtreeoptions(Datum reloptions, bool validate);
extern bool bwtreevalidate(Oid opclassoid);
extern void bwtreeadjustmembers(Oid opfamilyoid, Oid opclassoid,
								List *operators, List *functions);
extern char *bwtreebuildphasename(int64 phasenum);

/* ================================================================
 *  INTERNAL FUNCTIONS
 * ================================================================ */

/* --- bwtreepage.c ------------------------------------------------ */
extern Buffer _bwt_getbuf(Relation rel, BlockNumber blkno, int access);
extern void _bwt_relbuf(Relation rel, Buffer buf, int access);
extern Buffer _bwt_allocbuf(Relation rel);
extern void _bwt_initpage(Page page, uint16 flags, BWTreePid pid, uint32 level);
extern void _bwt_initmetapage(Page page, BWTreePid root_pid, uint32 level);

/* --- bwtreemap.c ------------------------------------------------- */
extern bool _bwt_map_lookup(Relation rel, BWTreeMetaPageData *metad,
							BWTreePid pid,
							BlockNumber *base_blkno, BlockNumber *delta_blkno);
extern void _bwt_map_update(Relation rel, BWTreeMetaPageData *metad,
							BWTreePid pid,
							BlockNumber base_blkno, BlockNumber delta_blkno);
extern BWTreePid _bwt_map_alloc_pid(Relation rel, BWTreeMetaPageData *metad,
									Buffer metabuf,
									BlockNumber base_blkno,
									BlockNumber delta_blkno);
extern BlockNumber _bwt_map_ensure_page(Relation rel,
										BWTreeMetaPageData *metad,
										Buffer metabuf,
										int map_page_idx);

/* --- bwtreedelta.c ----------------------------------------------- */
extern BWTreeNodeKind _bwt_classify_node(Page page);
extern void _bwt_delta_install(Relation rel, BWTreeMetaPageData *metad,
							   BWTreePid pid, BWTreeDeltaType type,
							   IndexTuple itup, BWTreePid related_pid);
/* base_page must be a writable copy; this function applies deltas in place. */
extern int _bwt_delta_apply(Relation rel, BlockNumber delta_blkno,
							Page base_page, OffsetNumber *maxoff);
extern bool _bwt_capture_node_snapshot(Relation rel, BWTreeMetaPageData *metad,
									   BWTreePid pid,
									   BWTreeNodeSnapshot *snapshot);
/*
 * In the current correctness-first stage, basebuf_out is always set to
 * InvalidBuffer by the implementation.
 */
extern void _bwt_materialize_page(Relation rel, BWTreeMetaPageData *metad,
								  BWTreePid pid, Buffer *basebuf_out,
								  BWTMaterializedPage *mpage);
extern void _bwt_free_materialized_page(BWTMaterializedPage *mpage);
extern void _bwt_materialize_node(Relation rel, BWTreeMetaPageData *metad,
								  BWTreePid pid, BWTreeNodeView *view);
extern void _bwt_free_node_view(BWTreeNodeView *view);

/* --- bwtreesearch.c ---------------------------------------------- */
extern void _bwt_begin_traverse(BWTreeContext *ctx, MemoryContext memcxt);
extern void _bwt_finish_traverse(BWTreeContext *ctx);
extern BWTreePid _bwt_search_leaf(Relation rel, BWTreeMetaPageData *metad,
								  ScanKey scankey, int nkeys);
extern BWTreePid _bwt_search_leaf_with_parent(Relation rel,
											  BWTreeMetaPageData *metad,
											  ScanKey scankey, int nkeys,
											  BWTreePid *parent_pid);
extern bool _bwt_descend_to_leaf(Relation rel, BWTreeMetaPageData *metad,
								 ScanKey scankey, int nkeys,
								 BWTreeContext *ctx,
								 BWTreeNodeSnapshot *leaf_snapshot);
extern int32 _bwt_compare(Relation rel, ScanKey scankey, int nkeys,
						   Page page, OffsetNumber offnum);

/* --- bwtreedelta.c ----------------------------------------------- */
extern void _bwt_consolidate(Relation rel, BWTreeMetaPageData *metad,
							 BWTreePid pid);
/* Consolidation is intentionally disabled for now (correctness-first mode). */
extern bool _bwt_should_consolidate(Relation rel, BWTreeMetaPageData *metad,
									BWTreePid pid);

/* --- bwtreesmo.c ------------------------------------------------- */
extern void _bwt_insert_item(Relation rel, BWTreeMetaPageData *metad,
							 BWTreePid pid, BWTreePid parent_pid,
							 IndexTuple itup, bool is_leaf);
extern bool _bwt_prepare_split(Relation rel, BWTreeMetaPageData *metad,
							   BWTreePid pid, BWTreeNodeView *view);
extern void _bwt_finish_split(Relation rel, BWTreeMetaPageData *metad,
							  BWTreePid pid, BWTreePid parent_pid,
							  BWTreeNodeView *view);
extern void _bwt_install_new_root(Relation rel, BWTreeMetaPageData *metad,
								  BWTreePid left_pid, BWTreePid right_pid,
								  IndexTuple sep_itup, uint32 child_level);
extern void _bwt_split(Relation rel, BWTreeMetaPageData *metad,
					   Buffer leafbuf, BWTreePid leaf_pid,
					   BWTreePid parent_pid);

#endif							/* BWTREE_H */
