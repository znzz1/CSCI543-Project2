/*-------------------------------------------------------------------------
 *
 * bwtree.h
 *    Header file for PostgreSQL Bw-tree index access method.
 *
 *    The Bw-tree is a latch-free B+tree variant designed for high-concurrency
 *    workloads (ICDE 2013, Microsoft Hekaton).  Its core ideas are:
 *
 *      1. Mapping Table  – logical page id (PID) -> physical pointer,
 *         enabling atomic page swaps via CAS.
 *      2. Delta Chains   – modifications are prepended as delta records
 *         instead of updating pages in place.
 *      3. Consolidation  – long delta chains are periodically collapsed
 *         into a new base page.
 *      4. Structure Modification Operations (SMO) – split / merge use
 *         split-delta and separator-delta records.
 *      5. Epoch-based Reclamation – safely free obsolete pages/deltas.
 *
 * src/include/access/bwtree.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BWTREE_H
#define BWTREE_H

#include "access/amapi.h"
#include "commands/vacuum.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"

/* ----------------------------------------------------------------
 *  Constants
 * ---------------------------------------------------------------- */
#define BWTREE_MAGIC   0x42575452
#define BWTREE_VERSION 1

#define BWTREE_OPTIONS_PROC 5
#define BWTREE_NPROCS       5

#define BWTREE_DELTA_CHAIN_THRESHOLD 8

/* Progress phase numbers for CREATE INDEX reporting */
#define PROGRESS_BWTREE_PHASE_TABLE_SCAN           2
#define PROGRESS_BWTREE_PHASE_SORT_LOAD            3
#define PROGRESS_BWTREE_PHASE_MAPPING_TABLE_LOAD   4
#define PROGRESS_BWTREE_PHASE_DELTA_CONSOLIDATION  5

/* ----------------------------------------------------------------
 *  Bw-tree specific data types
 * ---------------------------------------------------------------- */

typedef uint64 BWTreePid;          /* logical page identifier */

/*
 * Delta record types.
 *
 * Each modification to a Bw-tree page is recorded as a delta record that
 * is prepended to the delta chain.  The type determines how the record
 * should be interpreted during search or consolidation.
 */
typedef enum BWTreeDeltaType
{
	BW_DELTA_INSERT,               /* key-value insert */
	BW_DELTA_DELETE,               /* key-value delete */
	BW_DELTA_UPDATE,               /* in-place value update */
	BW_DELTA_SPLIT,                /* split delta: marks key range moved */
	BW_DELTA_SEPARATOR,            /* separator/index-entry delta in parent */
	BW_DELTA_MERGE,                /* merge delta on the receiving page */
	BW_DELTA_REMOVE_NODE           /* marks a page logically removed */
} BWTreeDeltaType;

/*
 * A single delta record.
 *
 * The record sits at the head of the delta chain for a logical page.
 * `next` points to the previous delta or, at the end of the chain, to
 * the base page.
 */
typedef struct BWTreeDeltaRecord
{
	BWTreeDeltaType type;

	/* key that this delta operates on (for INSERT / DELETE / UPDATE) */
	Datum           key;
	bool            key_is_null;

	/* heap TID for this index entry */
	ItemPointerData heap_tid;

	/* split/separator specific */
	Datum           split_key;         /* separator key for SPLIT / SEPARATOR */
	BWTreePid       sibling_pid;       /* logical id of the new sibling page */

	/* chain link */
	struct BWTreeDeltaRecord *next;    /* next older delta or base-page ptr */
} BWTreeDeltaRecord;

/*
 * A Bw-tree base page (leaf or internal).
 *
 * After consolidation, all deltas are merged into a sorted array of
 * index tuples stored inside a base page.  Between consolidations the
 * page is immutable; new changes go into delta records.
 */
typedef struct BWTreeBasePage
{
	bool            is_leaf;
	BWTreePid       pid;               /* this page's logical id */
	BWTreePid       right_sibling_pid; /* right-link for range scans */
	Datum           low_key;           /* inclusive lower bound */
	Datum           high_key;          /* exclusive upper bound */

	int             num_items;
	int             max_items;
	/* flexible array: sorted index tuples follow */
} BWTreeBasePage;

/*
 * Unified node handle.
 *
 * The mapping table stores pointers to BWTreeNode.  A node is either a
 * delta chain head (delta != NULL) or a consolidated base page.
 */
typedef struct BWTreeNode
{
	BWTreeDeltaRecord *delta_chain_head;
	BWTreeBasePage    *base_page;
	int                delta_chain_len;
} BWTreeNode;

/* ----------------------------------------------------------------
 *  Mapping Table
 *
 *  Maps BWTreePid -> BWTreeNode*.  CAS on the pointer is used to
 *  install new delta records or swap in a consolidated page without
 *  latches.
 * ---------------------------------------------------------------- */
typedef struct BWTreeMappingEntry
{
	BWTreePid       pid;
	BWTreeNode     *node;              /* physical pointer (CAS target) */
} BWTreeMappingEntry;

typedef struct BWTreeMappingTable
{
	int              capacity;
	int              num_entries;
	BWTreePid        next_pid;         /* monotonically increasing PID allocator */
	BWTreeMappingEntry *entries;
} BWTreeMappingTable;

/* ----------------------------------------------------------------
 *  Epoch-based Reclamation
 *
 *  Threads enter/leave epochs.  Pointers retired during epoch E can
 *  only be freed once no thread is still in epoch <= E.
 * ---------------------------------------------------------------- */
typedef struct BWTreeEpochManager
{
	uint64  global_epoch;
	uint64  min_active_epoch;
} BWTreeEpochManager;

/* ----------------------------------------------------------------
 *  Meta page (block 0 of the index)
 * ---------------------------------------------------------------- */
typedef struct BWTreeMetaPageData
{
	uint32      bwt_magic;
	uint32      bwt_version;
	BlockNumber bwt_root;
	BWTreePid   bwt_root_pid;
} BWTreeMetaPageData;

typedef struct BWTreeOptions
{
	int32       vl_len_;
	int         fillfactor;
	bool        enable_delta_consolidation;
} BWTreeOptions;

/* ----------------------------------------------------------------
 *  Scan opaque (private state for an index scan)
 * ---------------------------------------------------------------- */
typedef struct BWTreeScanOpaqueData
{
	BWTreePid       cur_pid;           /* current leaf page PID */
	int             cur_item_index;    /* position within consolidated view */
	bool            scan_started;
	ScanDirection   scan_dir;
} BWTreeScanOpaqueData;

typedef BWTreeScanOpaqueData *BWTreeScanOpaque;

/* ================================================================
 *  PUBLIC  AM  CALLBACKS
 * ================================================================ */

extern Datum bwtreehandler(PG_FUNCTION_ARGS);

/* bwtreesort.c – build */
extern IndexBuildResult *bwtreebuild(Relation heap, Relation index,
									 struct IndexInfo *indexInfo);
extern void bwtreebuildempty(Relation index);
extern char *bwtreebuildphasename(int64 phasenum);

/* bwtreeinsert.c – insert */
extern bool bwtreeinsert(Relation rel, Datum *values, bool *isnull,
						 ItemPointer ht_ctid, Relation heapRel,
						 IndexUniqueCheck checkUnique,
						 bool indexUnchanged,
						 struct IndexInfo *indexInfo);
extern void bwtreeinsertcleanup(Relation index, struct IndexInfo *indexInfo);

/* bwtreescan.c – scan */
extern IndexScanDesc bwtreebeginscan(Relation rel, int nkeys, int norderbys);
extern void bwtreerescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
						 ScanKey orderbys, int norderbys);
extern bool bwtreegettuple(IndexScanDesc scan, ScanDirection dir);
extern int64 bwtreegetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern void bwtreeendscan(IndexScanDesc scan);
extern void bwtreemarkpos(IndexScanDesc scan);
extern void bwtreerestrpos(IndexScanDesc scan);
extern Size bwtreeestimateparallelscan(int nkeys, int norderbys);
extern void bwtreeinitparallelscan(void *target);
extern void bwtreeparallelrescan(IndexScanDesc scan);

/* bwtree.c – vacuum */
extern IndexBulkDeleteResult *bwtreebulkdelete(IndexVacuumInfo *info,
											   IndexBulkDeleteResult *stats,
											   IndexBulkDeleteCallback callback,
											   void *callback_state);
extern IndexBulkDeleteResult *bwtreevacuumcleanup(IndexVacuumInfo *info,
												  IndexBulkDeleteResult *stats);

/* bwtreeutils.c – planner / cost / options */
extern bool bwtreecanreturn(Relation index, int attno);
extern void bwtreecostestimate(PlannerInfo *root, IndexPath *path,
							   double loop_count,
							   Cost *indexStartupCost,
							   Cost *indexTotalCost,
							   Selectivity *indexSelectivity,
							   double *indexCorrelation,
							   double *indexPages);
extern bytea *bwtreeoptions(Datum reloptions, bool validate);
extern bool bwtreeproperty(Oid index_oid, int attno,
						   IndexAMProperty prop, const char *propname,
						   bool *res, bool *isnull);

/* bwtreevalidate.c – opclass validation */
extern bool bwtreevalidate(Oid opclassoid);
extern void bwtreeadjustmembers(Oid opfamilyoid,
								Oid opclassoid,
								List *operators,
								List *functions);

/* ================================================================
 *  INTERNAL  FUNCTIONS  (Bw-tree core components)
 * ================================================================ */

/* --- bwtreepage.c: page management -------------------------------- */
extern void bwtree_init_metapage(Page page);
extern void bwtree_init_page(Page page, Size size);

/* --- bwtreemap.c: mapping table ----------------------------------- */
extern BWTreeMappingTable *bwtree_map_create(int capacity);
extern void bwtree_map_destroy(BWTreeMappingTable *mt);
extern BWTreePid bwtree_map_alloc_pid(BWTreeMappingTable *mt);
extern BWTreeNode *bwtree_map_get(BWTreeMappingTable *mt, BWTreePid pid);
extern bool bwtree_map_cas(BWTreeMappingTable *mt, BWTreePid pid,
						   BWTreeNode *expected, BWTreeNode *desired);

/* --- bwtreedelta.c: delta record operations ----------------------- */
extern BWTreeDeltaRecord *bwtree_delta_create(BWTreeDeltaType type);
extern bool bwtree_delta_install_insert(BWTreeMappingTable *mt,
										BWTreePid pid,
										Datum key, bool key_is_null,
										ItemPointer heap_tid);
extern bool bwtree_delta_install_delete(BWTreeMappingTable *mt,
										BWTreePid pid,
										Datum key, bool key_is_null,
										ItemPointer heap_tid);
extern bool bwtree_delta_install_split(BWTreeMappingTable *mt,
									   BWTreePid pid,
									   Datum split_key,
									   BWTreePid new_sibling_pid);
extern bool bwtree_delta_install_separator(BWTreeMappingTable *mt,
										   BWTreePid parent_pid,
										   Datum split_key,
										   BWTreePid new_child_pid);
extern bool bwtree_delta_install_merge(BWTreeMappingTable *mt,
									   BWTreePid pid,
									   BWTreePid merged_sibling_pid);
extern bool bwtree_delta_install_remove_node(BWTreeMappingTable *mt,
											 BWTreePid pid);

extern bool bwtree_delta_chain_search(BWTreeNode *node, Datum key,
									  ScanKey scankey,
									  ItemPointer result_tid);
extern void bwtree_delta_chain_collect(BWTreeNode *node,
									   BWTreeBasePage **out_page);

/* --- bwtreeconsolidate.c: consolidation --------------------------- */
extern bool bwtree_should_consolidate(BWTreeNode *node,
									  uint32 threshold);
extern BWTreeBasePage *bwtree_consolidate(BWTreeMappingTable *mt,
										  BWTreePid pid);

/* --- bwtreesmo.c: Structure Modification Operations --------------- */
extern bool bwtree_smo_split(BWTreeMappingTable *mt,
							 BWTreePid pid,
							 BWTreePid parent_pid);
extern bool bwtree_smo_merge(BWTreeMappingTable *mt,
							 BWTreePid left_pid,
							 BWTreePid right_pid,
							 BWTreePid parent_pid);

/* --- bwtreeepoch.c: epoch-based reclamation ----------------------- */
extern BWTreeEpochManager *bwtree_epoch_create(void);
extern void bwtree_epoch_destroy(BWTreeEpochManager *em);
extern void bwtree_epoch_enter(BWTreeEpochManager *em);
extern void bwtree_epoch_leave(BWTreeEpochManager *em);
extern void bwtree_epoch_retire(BWTreeEpochManager *em, void *ptr);
extern void bwtree_epoch_reclaim(BWTreeEpochManager *em);

/* --- bwtreesearch.c: tree traversal ------------------------------- */
extern BWTreePid bwtree_search_leaf(BWTreeMappingTable *mt,
									BWTreePid root_pid,
									Datum key, ScanKey scankey);
extern bool bwtree_search(BWTreeMappingTable *mt,
						  BWTreePid root_pid,
						  Datum key, ScanKey scankey,
						  ItemPointer result_tid);
extern BWTreePid bwtree_search_next_leaf(BWTreeMappingTable *mt,
										 BWTreePid cur_pid);

#endif							/* BWTREE_H */
