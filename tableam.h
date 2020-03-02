/*-------------------------------------------------------------------------
 *
 * tableam.h
 *	  POSTGRES table access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/tableam.h
 *
 * NOTES
 *		See tableam.sgml for higher level documentation.
 *
 *-------------------------------------------------------------------------
 */
#ifndef VECTOR_ENGINE_TABLEAM_H
#define VECTOR_ENGINE_TABLEAM_H

#include "access/relscan.h"
#include "access/sdir.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/* ----------------------------------------------------------------------------
 * Functions in tableamapi.c
 * ----------------------------------------------------------------------------
 */

extern const TableAmRoutine *VGetHeapamTableAmRoutine(void);
#endif							/* TABLEAM_H */
