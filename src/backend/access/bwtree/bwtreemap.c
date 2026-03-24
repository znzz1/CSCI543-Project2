/*-------------------------------------------------------------------------
 *
 * bwtreemap.c
 *    Mapping-table operations for the Bw-tree.
 *
 *    The mapping table maps logical PIDs to (base_blkno, delta_blkno)
 *    pairs.  It is stored as a set of mapping pages whose block numbers
 *    are recorded in the meta page.
 *
 *    PID N lives on mapping page  N / BWTREE_MAP_ENTRIES_PER_PAGE
 *    at entry offset              N % BWTREE_MAP_ENTRIES_PER_PAGE.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufmgr.h"

/*
 * Make sure mapping page index `map_page_idx` exists.
 * If it does not, allocate a new page, initialize it, and register
 * its block number in the meta page.
 *
 * Caller must hold the meta page buffer with exclusive lock so that
 * bwt_map_blknos can be updated safely.  Returns the BlockNumber of
 * the mapping page.
 */
BlockNumber
_bwt_map_ensure_page(Relation rel, BWTreeMetaPageData *metad, int map_page_idx)
{
	Assert(map_page_idx >= 0 && map_page_idx < BWTREE_MAX_MAP_PAGES);

	if (map_page_idx < (int) metad->bwt_num_map_pages)
		return metad->bwt_map_blknos[map_page_idx];

	/* allocate a new mapping page */
	{
		Buffer		buf;
		Page		page;

		buf = _bwt_allocbuf(rel);
		page = BufferGetPage(buf);

		PageInit(page, BLCKSZ, 0);
		memset(PageGetContents(page), 0,
			   BWTREE_MAP_ENTRIES_PER_PAGE * sizeof(BWTreeMapEntry));
		((PageHeader) page)->pd_lower =
			((char *) PageGetContents(page) +
			 BWTREE_MAP_ENTRIES_PER_PAGE * sizeof(BWTreeMapEntry)) -
			(char *) page;

		MarkBufferDirty(buf);

		metad->bwt_map_blknos[map_page_idx] = BufferGetBlockNumber(buf);
		metad->bwt_num_map_pages = map_page_idx + 1;

		_bwt_relbuf(rel, buf);
	}

	return metad->bwt_map_blknos[map_page_idx];
}

/*
 * Look up a PID in the mapping table.
 *
 * Returns true if the PID is within the allocated range and fills in
 * *base_blkno and *delta_blkno.  Returns false if the PID has not
 * been allocated yet.
 */
bool
_bwt_map_lookup(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid,
				BlockNumber *base_blkno, BlockNumber *delta_blkno)
{
	int				map_page_idx;
	int				entry_idx;
	BlockNumber		map_blkno;
	Buffer			buf;
	Page			page;
	BWTreeMapEntry *entries;

	if (pid >= metad->bwt_next_pid)
		return false;

	map_page_idx = pid / BWTREE_MAP_ENTRIES_PER_PAGE;
	entry_idx    = pid % BWTREE_MAP_ENTRIES_PER_PAGE;

	if (map_page_idx >= (int) metad->bwt_num_map_pages)
		return false;

	map_blkno = metad->bwt_map_blknos[map_page_idx];

	buf = _bwt_getbuf(rel, map_blkno, BWT_READ);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);

	*base_blkno  = entries[entry_idx].base_blkno;
	*delta_blkno = entries[entry_idx].delta_blkno;

	_bwt_relbuf(rel, buf);
	return true;
}

/*
 * Update a mapping table entry.
 *
 * Acquires exclusive lock on the mapping page, writes the new values,
 * and marks the page dirty.
 */
void
_bwt_map_update(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid,
				BlockNumber base_blkno, BlockNumber delta_blkno)
{
	int				map_page_idx;
	int				entry_idx;
	BlockNumber		map_blkno;
	Buffer			buf;
	Page			page;
	BWTreeMapEntry *entries;

	map_page_idx = pid / BWTREE_MAP_ENTRIES_PER_PAGE;
	entry_idx    = pid % BWTREE_MAP_ENTRIES_PER_PAGE;

	map_blkno = metad->bwt_map_blknos[map_page_idx];

	buf = _bwt_getbuf(rel, map_blkno, BWT_WRITE);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);

	entries[entry_idx].base_blkno  = base_blkno;
	entries[entry_idx].delta_blkno = delta_blkno;

	MarkBufferDirty(buf);
	_bwt_relbuf(rel, buf);
}

/*
 * Allocate a new PID and write its initial mapping entry.
 *
 * Caller must hold the meta page buffer with exclusive lock because
 * we increment bwt_next_pid and may allocate new mapping pages.
 */
BWTreePid
_bwt_map_alloc_pid(Relation rel, BWTreeMetaPageData *metad,
				   BlockNumber base_blkno, BlockNumber delta_blkno)
{
	BWTreePid		pid;
	int				map_page_idx;
	int				entry_idx;
	BlockNumber		map_blkno;
	Buffer			buf;
	Page			page;
	BWTreeMapEntry *entries;

	pid = metad->bwt_next_pid;
	metad->bwt_next_pid++;

	map_page_idx = pid / BWTREE_MAP_ENTRIES_PER_PAGE;
	entry_idx    = pid % BWTREE_MAP_ENTRIES_PER_PAGE;

	map_blkno = _bwt_map_ensure_page(rel, metad, map_page_idx);

	buf = _bwt_getbuf(rel, map_blkno, BWT_WRITE);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);

	entries[entry_idx].base_blkno  = base_blkno;
	entries[entry_idx].delta_blkno = delta_blkno;

	MarkBufferDirty(buf);
	_bwt_relbuf(rel, buf);

	return pid;
}
