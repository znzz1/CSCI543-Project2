/*-------------------------------------------------------------------------
 *
 * bwtreepage.c
 *    Page and metapage scaffolding for the Bw-tree access method.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

void
bwtree_init_metapage(Page page)
{
	BWTreeMetaPageData *metadata;

	metadata = (BWTreeMetaPageData *) PageGetContents(page);
	metadata->bwt_magic = BWTREE_MAGIC;
	metadata->bwt_version = BWTREE_VERSION;
	metadata->bwt_root = InvalidBlockNumber;
	metadata->bwt_root_pid = 0;
}

void
bwtree_init_page(Page page, Size size)
{
	PageInit(page, size, 0);
}
