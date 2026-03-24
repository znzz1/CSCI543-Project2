/*-------------------------------------------------------------------------
 *
 * bwtreevalidate.c
 *    Opclass validation for the Bw-tree.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/bwtree.h"

bool
bwtreevalidate(Oid opclassoid)
{
	(void) opclassoid;
	return true;
}

void
bwtreeadjustmembers(Oid opfamilyoid, Oid opclassoid,
					List *operators, List *functions)
{
	(void) opfamilyoid;
	(void) opclassoid;
	(void) operators;
	(void) functions;
}
