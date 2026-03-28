/*-------------------------------------------------------------------------
 *
 * bwtreemap.c
 *    Mapping-table helpers for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufmgr.h"

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

BlockNumber
_bwt_map_ensure_page(Relation rel, BWTreeMetaPageData *metad, Buffer metabuf,
					 int map_page_idx)
{
	Buffer	buf;
	Page	page;

	if (map_page_idx < 0 || map_page_idx >= BWTREE_MAX_MAP_PAGES)
		elog(ERROR, "bwtree: mapping page index %d is out of range", map_page_idx);

	if (map_page_idx < (int) metad->bwt_num_map_pages)
	{
		if (!BlockNumberIsValid(metad->bwt_map_blknos[map_page_idx]))
			elog(ERROR, "bwtree: mapping page %d has invalid block number",
				 map_page_idx);
		return metad->bwt_map_blknos[map_page_idx];
	}

	buf = _bwt_allocbuf(rel);
	page = BufferGetPage(buf);
	_bwt_init_mapping_page(page);
	MarkBufferDirty(buf);

	metad->bwt_map_blknos[map_page_idx] = BufferGetBlockNumber(buf);
	metad->bwt_num_map_pages = map_page_idx + 1;
	MarkBufferDirty(metabuf);

	_bwt_relbuf(rel, buf, BWT_WRITE);
	return metad->bwt_map_blknos[map_page_idx];
}

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

	if (base_blkno == NULL || delta_blkno == NULL)
		elog(ERROR, "bwtree: map lookup output pointers must not be NULL");

	if (pid >= metad->bwt_next_pid)
		return false;

	map_page_idx = pid / BWTREE_MAP_ENTRIES_PER_PAGE;
	entry_idx = pid % BWTREE_MAP_ENTRIES_PER_PAGE;

	if (map_page_idx >= (int) metad->bwt_num_map_pages)
		return false;

	map_blkno = metad->bwt_map_blknos[map_page_idx];
	if (!BlockNumberIsValid(map_blkno))
		elog(ERROR, "bwtree: PID %u maps to invalid mapping page block",
			 (unsigned int) pid);

	buf = _bwt_getbuf(rel, map_blkno, BWT_READ);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);

	*base_blkno = entries[entry_idx].base_blkno;
	*delta_blkno = entries[entry_idx].delta_blkno;

	_bwt_relbuf(rel, buf, BWT_READ);
	return true;
}

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

	if (pid >= metad->bwt_next_pid)
		elog(ERROR, "bwtree: cannot update unmapped PID %u",
			 (unsigned int) pid);

	map_page_idx = pid / BWTREE_MAP_ENTRIES_PER_PAGE;
	entry_idx = pid % BWTREE_MAP_ENTRIES_PER_PAGE;
	if (map_page_idx >= (int) metad->bwt_num_map_pages)
		elog(ERROR, "bwtree: PID %u map page %d does not exist",
			 (unsigned int) pid, map_page_idx);

	map_blkno = metad->bwt_map_blknos[map_page_idx];
	if (!BlockNumberIsValid(map_blkno))
		elog(ERROR, "bwtree: map page %d has invalid block number",
			 map_page_idx);

	/*
	 * Correctness-first trade-off:
	 *
	 * We serialize mapping entry updates with a mapping-page write latch.
	 * This is simpler and safer for now than a lock-free CAS publish path,
	 * but it reduces concurrency for PIDs that share the same mapping page.
	 */
	buf = _bwt_getbuf(rel, map_blkno, BWT_WRITE);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);

	entries[entry_idx].base_blkno = base_blkno;
	entries[entry_idx].delta_blkno = delta_blkno;
	MarkBufferDirty(buf);

	_bwt_relbuf(rel, buf, BWT_WRITE);
}

BWTreePid
_bwt_map_alloc_pid(Relation rel, BWTreeMetaPageData *metad,
				   Buffer metabuf,
				   BlockNumber base_blkno, BlockNumber delta_blkno)
{
	BWTreePid		pid;
	int				map_page_idx;
	int				entry_idx;
	BlockNumber		map_blkno;
	Buffer			buf;
	Page			page;
	BWTreeMapEntry *entries;

	if (metad->bwt_next_pid == InvalidBWTreePid)
		elog(ERROR, "bwtree: PID space exhausted");

	pid = metad->bwt_next_pid;
	map_page_idx = pid / BWTREE_MAP_ENTRIES_PER_PAGE;
	if (map_page_idx >= BWTREE_MAX_MAP_PAGES)
		elog(ERROR, "bwtree: mapping table is full at PID %u",
			 (unsigned int) pid);

	entry_idx = pid % BWTREE_MAP_ENTRIES_PER_PAGE;
	metad->bwt_next_pid = pid + 1;
	MarkBufferDirty(metabuf);

	map_blkno = _bwt_map_ensure_page(rel, metad, metabuf, map_page_idx);
	buf = _bwt_getbuf(rel, map_blkno, BWT_WRITE);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);

	entries[entry_idx].base_blkno = base_blkno;
	entries[entry_idx].delta_blkno = delta_blkno;
	MarkBufferDirty(buf);

	_bwt_relbuf(rel, buf, BWT_WRITE);
	return pid;
}
