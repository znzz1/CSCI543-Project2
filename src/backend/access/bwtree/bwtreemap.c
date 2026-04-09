#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufmgr.h"

static uint64 _bwt_map_pack_entry(BlockNumber base_blkno, BlockNumber delta_blkno);
static void _bwt_map_unpack_entry(uint64 packed,
								  BlockNumber *base_blkno,
								  BlockNumber *delta_blkno);
static volatile pg_atomic_uint64 *_bwt_map_entry_atomic_slot(BWTreeMapEntry *entry);

static uint64 _bwt_map_pack_entry(BlockNumber base_blkno, BlockNumber delta_blkno)
{
	return (((uint64) base_blkno) << 32) | (uint64) delta_blkno;
}

static void _bwt_map_unpack_entry(uint64 packed,
								  BlockNumber *base_blkno,
								  BlockNumber *delta_blkno)
{
	if (base_blkno != NULL)
		*base_blkno = (BlockNumber) (packed >> 32);
	if (delta_blkno != NULL)
		*delta_blkno = (BlockNumber) (packed & UINT64CONST(0xFFFFFFFF));
}

static volatile pg_atomic_uint64 *_bwt_map_entry_atomic_slot(BWTreeMapEntry *entry)
{
	StaticAssertDecl(sizeof(BWTreeMapEntry) == sizeof(uint64),
					 "BWTreeMapEntry must be 64-bit wide for CAS publish");

	return (volatile pg_atomic_uint64 *) entry;
}

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
	uint64			packed;

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

	/*
	 * Read-side is lock-free at slot granularity: mapping entry payload is
	 * loaded atomically as one 64-bit word.
	 */
	buf = _bwt_getbuf(rel, map_blkno, BWT_NOLOCK);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);
	packed = pg_atomic_read_u64(
		_bwt_map_entry_atomic_slot(&entries[entry_idx]));
	_bwt_map_unpack_entry(packed, base_blkno, delta_blkno);
	if (!BlockNumberIsValid(*base_blkno))
	{
		_bwt_relbuf(rel, buf, BWT_NOLOCK);
		elog(ERROR, "bwtree: PID %u has invalid base block in mapping entry",
			 (unsigned int) pid);
	}

	_bwt_relbuf(rel, buf, BWT_NOLOCK);
	return true;
}

bool
_bwt_map_cas(Relation rel, BWTreeMetaPageData *metad, BWTreePid pid,
			 BlockNumber expected_base_blkno,
			 BlockNumber expected_delta_blkno,
			 BlockNumber new_base_blkno,
			 BlockNumber new_delta_blkno,
			 BlockNumber *observed_base_blkno,
			 BlockNumber *observed_delta_blkno)
{
	int							map_page_idx;
	int							entry_idx;
	BlockNumber					map_blkno;
	Buffer						buf;
	Page						page;
	BWTreeMapEntry			   *entries;
	volatile pg_atomic_uint64  *slot;
	uint64						expected_packed;
	uint64						desired_packed;
	bool						ok;

	if (pid >= metad->bwt_next_pid)
		elog(ERROR, "bwtree: cannot CAS unmapped PID %u",
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
	 * Correctness-first CAS publish:
	 *
	 * Hold mapping-page content lock while doing CAS+dirty-mark so page
	 * mutation follows PostgreSQL buffer manager locking requirements.
	 */
	buf = _bwt_getbuf(rel, map_blkno, BWT_WRITE);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);
	slot = _bwt_map_entry_atomic_slot(&entries[entry_idx]);

	expected_packed = _bwt_map_pack_entry(expected_base_blkno,
										  expected_delta_blkno);
	desired_packed = _bwt_map_pack_entry(new_base_blkno, new_delta_blkno);
	ok = pg_atomic_compare_exchange_u64(slot, &expected_packed, desired_packed);
	if (ok)
		MarkBufferDirty(buf);
	else
		_bwt_map_unpack_entry(expected_packed,
							  observed_base_blkno,
							  observed_delta_blkno);

	_bwt_relbuf(rel, buf, BWT_WRITE);
	return ok;
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
	uint64			packed;

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
	packed = _bwt_map_pack_entry(base_blkno, delta_blkno);
	pg_atomic_write_u64(_bwt_map_entry_atomic_slot(&entries[entry_idx]),
						packed);
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
	uint64			packed;

	if (metad->bwt_next_pid == InvalidBWTreePid)
		elog(ERROR, "bwtree: PID space exhausted");

	pid = metad->bwt_next_pid;
	map_page_idx = pid / BWTREE_MAP_ENTRIES_PER_PAGE;
	if (map_page_idx >= BWTREE_MAX_MAP_PAGES)
		elog(ERROR, "bwtree: mapping table is full at PID %u",
			 (unsigned int) pid);

	entry_idx = pid % BWTREE_MAP_ENTRIES_PER_PAGE;
	map_blkno = _bwt_map_ensure_page(rel, metad, metabuf, map_page_idx);
	buf = _bwt_getbuf(rel, map_blkno, BWT_WRITE);
	page = BufferGetPage(buf);
	entries = BWTreeMapPageGetEntries(page);
	packed = _bwt_map_pack_entry(base_blkno, delta_blkno);
	pg_atomic_write_u64(_bwt_map_entry_atomic_slot(&entries[entry_idx]),
						packed);
	MarkBufferDirty(buf);
	_bwt_relbuf(rel, buf, BWT_WRITE);

	/*
	 * Publish PID allocation only after mapping entry is fully initialized.
	 * This avoids readers observing bwt_next_pid that points to an unready
	 * entry.
	 */
	metad->bwt_next_pid = pid + 1;
	MarkBufferDirty(metabuf);

	return pid;
}
