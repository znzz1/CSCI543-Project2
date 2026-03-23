/*-------------------------------------------------------------------------
 *
 * bwtreevalidate.c
 *    Opclass validation scaffolding for the Bw-tree access method.
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
