/*-------------------------------------------------------------------------
 *
 * bwtreedelta.c
 *    Delta-page operations for the Bw-tree.
 *
 *    Each modification (INSERT / DELETE) is recorded as a delta record
 *    on a delta page chain rather than modifying the base page in place.
 *    The chain head block is stored in the mapping table.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"
#include "storage/bufmgr.h"

/*
 * Install a delta record for the logical page identified by `pid`.
 *
 * Steps:
 *   1. Look up current delta_blkno from the mapping table.
 *   2. If a delta page exists and has room, append the record there.
 *   3. Otherwise allocate a new delta page, prepend it to the chain,
 *      and update the mapping table.
 */
void
_bwt_delta_install(Relation rel, BWTreeMetaPageData *metad,
				   BWTreePid pid, BWTreeDeltaType type,
				   IndexTuple itup, BWTreePid related_pid)
{
	BlockNumber base_blkno;
	BlockNumber delta_blkno;
	Size		rec_size;
	Size		itup_size;
	BWTreeDeltaRecordData *rec;
	char	   *rec_buf;
	bool		found;

	found = _bwt_map_lookup(rel, metad, pid, &base_blkno, &delta_blkno);
	if (!found)
		elog(ERROR, "bwtree: PID %u not found in mapping table", pid);

	/* build the delta record in a temporary buffer */
	itup_size = (itup != NULL) ? IndexTupleSize(itup) : 0;
	rec_size = SizeOfBWTreeDeltaRecord + itup_size;

	rec_buf = (char *) palloc0(rec_size);
	rec = (BWTreeDeltaRecordData *) rec_buf;
	rec->type = type;
	rec->related_pid = related_pid;
	rec->data_len = (uint16) itup_size;
	if (itup != NULL)
		memcpy(rec_buf + SizeOfBWTreeDeltaRecord, itup, itup_size);

	/* try to add to existing head delta page */
	if (delta_blkno != InvalidBlockNumber)
	{
		Buffer		dbuf;
		Page		dpage;

		dbuf = _bwt_getbuf(rel, delta_blkno, BWT_WRITE);
		dpage = BufferGetPage(dbuf);

		if (PageGetFreeSpace(dpage) >= MAXALIGN(rec_size) + sizeof(ItemIdData))
		{
			OffsetNumber off;

			off = PageAddItem(dpage, (Item) rec_buf, rec_size,
							  InvalidOffsetNumber, false, false);
			if (off == InvalidOffsetNumber)
				elog(ERROR, "bwtree: failed to add delta record to page");

			MarkBufferDirty(dbuf);
			_bwt_relbuf(rel, dbuf);
			pfree(rec_buf);
			return;
		}

		_bwt_relbuf(rel, dbuf);
	}

	/* need a new delta page */
	{
		Buffer		newbuf;
		Page		newpage;
		BWTreePageOpaque opaque;
		OffsetNumber off;

		newbuf = _bwt_allocbuf(rel);
		newpage = BufferGetPage(newbuf);

		_bwt_initpage(newpage, BWT_DELTA, pid, 0);

		opaque = BWTreePageGetOpaque(newpage);
		opaque->bwto_next = delta_blkno;   /* link to old chain head */

		off = PageAddItem(newpage, (Item) rec_buf, rec_size,
						  InvalidOffsetNumber, false, false);
		if (off == InvalidOffsetNumber)
			elog(ERROR, "bwtree: failed to add delta record to new page");

		MarkBufferDirty(newbuf);

		/* update mapping table: point delta_blkno to the new page */
		_bwt_map_update(rel, metad, pid,
						base_blkno, BufferGetBlockNumber(newbuf));

		_bwt_relbuf(rel, newbuf);
	}

	pfree(rec_buf);
}

/*
 * Apply all delta records from the delta chain onto a copy of the
 * base page.  Returns the number of net insertions (positive) or
 * deletions (negative) applied.
 *
 * The caller passes a writable page buffer (typically a palloc'd copy
 * of the base page).  INSERT deltas add items, DELETE deltas remove
 * matching items.
 *
 * Only INSERT / DELETE deltas are applied; structural deltas (SPLIT
 * etc.) are ignored for now.
 *
 * `maxoff` is updated to the new maximum offset on the page.
 */
int
_bwt_delta_apply(Relation rel, BlockNumber delta_blkno,
				 Page base_page, OffsetNumber *maxoff)
{
	int			net = 0;
	BlockNumber cur_blkno = delta_blkno;

	while (cur_blkno != InvalidBlockNumber)
	{
		Buffer		dbuf;
		Page		dpage;
		BWTreePageOpaque opaque;
		OffsetNumber doff;
		OffsetNumber dmaxoff;

		dbuf = _bwt_getbuf(rel, cur_blkno, BWT_READ);
		dpage = BufferGetPage(dbuf);
		opaque = BWTreePageGetOpaque(dpage);

		dmaxoff = PageGetMaxOffsetNumber(dpage);

		for (doff = FirstOffsetNumber; doff <= dmaxoff; doff++)
		{
			ItemId			did = PageGetItemId(dpage, doff);
			BWTreeDeltaRecordData *drec;
			IndexTuple		ditup;

			if (!ItemIdIsUsed(did))
				continue;

			drec = (BWTreeDeltaRecordData *) PageGetItem(dpage, did);
			ditup = BWTreeDeltaRecordGetTuple(drec);

			if (drec->type == BW_DELTA_INSERT && ditup != NULL)
			{
				Size itup_size = IndexTupleSize(ditup);

				if (PageGetFreeSpace(base_page) >=
					MAXALIGN(itup_size) + sizeof(ItemIdData))
				{
					PageAddItem(base_page, (Item) ditup, itup_size,
								InvalidOffsetNumber, false, false);
					net++;
				}
			}
			else if (drec->type == BW_DELTA_DELETE && ditup != NULL)
			{
				OffsetNumber boff;
				OffsetNumber bmaxoff = PageGetMaxOffsetNumber(base_page);

				for (boff = FirstOffsetNumber; boff <= bmaxoff; boff++)
				{
					ItemId		bid = PageGetItemId(base_page, boff);
					IndexTuple	bitup;

					if (!ItemIdIsUsed(bid))
						continue;

					bitup = (IndexTuple) PageGetItem(base_page, bid);

					if (IndexTupleSize(bitup) == IndexTupleSize(ditup) &&
						memcmp(bitup, ditup, IndexTupleSize(ditup)) == 0)
					{
						PageIndexTupleDelete(base_page, boff);
						net--;
						break;
					}
				}
			}
		}

		cur_blkno = opaque->bwto_next;
		_bwt_relbuf(rel, dbuf);
	}

	*maxoff = PageGetMaxOffsetNumber(base_page);
	return net;
}
