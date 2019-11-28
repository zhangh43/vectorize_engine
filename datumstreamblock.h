/*-------------------------------------------------------------------------
 *
 * datumstreamblock.h
 *
 * Portions Copyright (c) 201, EMC Inc.
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/utils/datumstreamblock.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef VECTOR_DATUMSTREAMBLOCK_H
#define VECTOR_DATUMSTREAMBLOCK_H

#include "catalog/pg_attribute.h"
#include "utils/guc.h"

#include "utils/datumstreamblock.h"

#include "vtype/vtype.h"
/* Stream access method */
inline static void
vDatumStreamBlockRead_Get(DatumStreamBlockRead * dsr, Datum *datum, bool *null, int minimalBatchSize)
{
	/*
	 * PERFORMANCE EXPERIMENT: Only do integrity and trace checking for DEBUG
	 * builds...
	 */
#ifdef USE_ASSERT_CHECKING
	if (strncmp(dsr->eyecatcher, DatumStreamBlockRead_Eyecatcher, DatumStreamBlockRead_EyecatcherLen) != 0)
		elog(FATAL, "DatumStreamBlockRead data structure not valid (eyecatcher)");
#endif

#ifdef USE_ASSERT_CHECKING
	if ((dsr->datumStreamVersion == DatumStreamVersion_Dense) ||
		(dsr->datumStreamVersion == DatumStreamVersion_Dense_Enhanced))
	{
		DatumStreamBlockRead_CheckDenseGetInvariant(dsr);
	}
#endif

	vtype	*column;
	int		row;
//	int	rowsLeftInBlock;
//	int rowsNeededInBatch;
	column = (vtype *)DatumGetPointer(*datum);
/*
	rowsLeftInBlock = dsr->logical_row_count - dsr->nth;

	rowsNeededInBatch = min(rowsLeftInBlock, BATCHSIZE - *rowsInPreviousBlock);
	// if (rowsNeededInBatch == rowsLeftInBlock && rowsNeededInBatch != BATCHSIZE)
	if (rowsLeftInBlock < BATCHSIZE - *rowsInPreviousBlock)
		*rowsInPreviousBlock = rowsLeftInBlock;
	*rowsInPreviousBlock = 0;
*/	
	/* fill null for batch */
	if (dsr->has_null)
	{
		for (row = 0; row < minimalBatchSize; row++)
		{
			DatumStreamBitMapRead_Next(&dsr->null_bitmap);
			Assert(DatumStreamBitMapRead_InRange(&dsr->null_bitmap));
			if(DatumStreamBitMapRead_CurrentIsOn(&dsr->null_bitmap))
			{
				column->isnull[row] = true;
			}
			else
				column->isnull[row] = false;
		}
	}

	for (row = 0; row < minimalBatchSize; row++)
	{
		if (!column->isnull[row])
		{
			/* advance work */
			if (dsr->physical_datum_index == 0)
			{
				/* no need to do anything here */
			}
			else
			{
				/*
				 * Advance the item pointer.
				 */
				if (dsr->typeInfo.datumlen == -1)
				{
					struct varlena *s = (struct varlena *) dsr->datump;

					Assert(dsr->datump >= dsr->datum_beginp);
					Assert(dsr->datump < dsr->datum_afterp);

					dsr->datump += VARSIZE_ANY(s);

					/*
					 * Skip any possible zero paddings AFTER PREVIOUS varlena data.
					 */
					if (*dsr->datump == 0)
					{
						dsr->datump = (uint8 *) att_align_nominal(dsr->datump, dsr->typeInfo.align);
					}
				}
				else if (dsr->typeInfo.datumlen == -2)
				{
					dsr->datump += strlen((char *) dsr->datump) + 1;
				}
				else
				{
					dsr->datump += dsr->typeInfo.datumlen;
				}
			}
			dsr->physical_datum_index++;
			/* fill vtype->values */

			if (dsr->typeInfo.datumlen == -1)
			{
#ifdef USE_ASSERT_CHECKING
				int32		varLen;
#endif
				Assert(dsr->delta_item == false);

				column->values[row] = PointerGetDatum(dsr->datump);
				Assert(VARATT_IS_SHORT(DatumGetPointer(*datum)) || !VARATT_IS_EXTERNAL(DatumGetPointer(*datum)));

				/*
				 * PERFORMANCE EXPERIMENT: Only do integrity and trace checking for
				 * DEBUG builds...
				 */
#ifdef USE_ASSERT_CHECKING
				varLen = VARSIZE_ANY(DatumGetPointer(*datum));

				if (varLen < 0 || varLen > dsr->physical_data_size)
				{
					ereport(ERROR,
							(errmsg("Datum stream block %s read variable-length item index %d length too large "
									"(nth %d, logical row count %d, "
									"item length %d, total physical data size %d, "
									"current datum pointer %p, after data pointer %p)",
									DatumStreamVersion_String(dsr->datumStreamVersion),
									dsr->physical_datum_index,
									dsr->nth,
									dsr->logical_row_count,
									varLen,
									dsr->physical_data_size,
									dsr->datump,
									dsr->datum_afterp),
							 errdetail_datumstreamblockread(dsr),
							 errcontext_datumstreamblockread(dsr)));
				}

				if (dsr->datump + varLen > dsr->datum_afterp)
				{
					ereport(ERROR,
							(errmsg("Datum stream block %s read variable-length item index %d length goes beyond end of block "
									"(nth %d, logical row count %d, "
									"item length %d, "
									"current datum pointer %p, after data pointer %p)",
									DatumStreamVersion_String(dsr->datumStreamVersion),
									dsr->physical_datum_index,
									dsr->nth,
									dsr->logical_row_count,
									varLen,
									dsr->datump,
									dsr->datum_afterp),
							 errdetail_datumstreamblockread(dsr),
							 errcontext_datumstreamblockread(dsr)));
				}

				if (Debug_datumstream_read_print_varlena_info)
				{
					DatumStreamBlockRead_PrintVarlenaInfo(
							dsr,
							dsr->datump);
				}

				if (Debug_appendonly_print_scan_tuple)
				{
					ereport(LOG,
							(errmsg("Datum stream block %s read is returning variable-length item #%d "
									"(nth %d, item begin %p, item offset " INT64_FORMAT ")",
									DatumStreamVersion_String(dsr->datumStreamVersion),
									dsr->physical_datum_index,
									dsr->nth,
									dsr->datump,
									(int64) (dsr->datump - dsr->datum_beginp)),
							 errdetail_datumstreamblockread(dsr),
							 errcontext_datumstreamblockread(dsr)));
				}
#endif
			}
			else if (!dsr->typeInfo.byval)
			{
				Assert(dsr->delta_item == false);

				column->values[row] = PointerGetDatum(dsr->datump);

				/*
				 * PERFORMANCE EXPERIMENT: Only do integrity and trace checking for
				 * DEBUG builds...
				 */
#ifdef USE_ASSERT_CHECKING
				if (Debug_appendonly_print_scan_tuple)
				{
					ereport(LOG,
							(errmsg("Datum stream block %s read is returning fixed-length item #%d "
									"(nth %d, item size %d, item begin %p, item offset " INT64_FORMAT ")",
									DatumStreamVersion_String(dsr->datumStreamVersion),
									dsr->physical_datum_index,
									dsr->nth,
									dsr->typeInfo.datumlen,
									dsr->datump,
									(int64) (dsr->datump - dsr->datum_beginp)),
							 errdetail_datumstreamblockread(dsr),
							 errcontext_datumstreamblockread(dsr)));
				}
#endif

			}
			else
			{
				if (!dsr->delta_item)
				{
					/*
					 * Performance is so critical we don't use a switch statement here.
					 */
					if (dsr->typeInfo.datumlen == 1)
					{
						column->values[row] = *(uint8 *) dsr->datump;
					}
					else if (dsr->typeInfo.datumlen == 2)
					{
						Assert(IsAligned(dsr->datump, 2));
						column->values[row] = *(uint16 *) dsr->datump;
					}
					else if (dsr->typeInfo.datumlen == 4)
					{
						Assert(IsAligned(dsr->datump, 4));
						column->values[row] = *(uint32 *) dsr->datump;
					}
					else if (dsr->typeInfo.datumlen == 8)
					{
						Assert(IsAligned(dsr->datump, 8) || IsAligned(dsr->datump, 4));
						column->values[row] = *(Datum *) dsr->datump;
					}
					else
					{
						column->values[row] = 0;
						Assert(false);
					}

					/*
					 * PERFORMANCE EXPERIMENT: Only do integrity and trace checking
					 * for DEBUG builds...
					 */
#ifdef USE_ASSERT_CHECKING
					if (Debug_appendonly_print_scan_tuple)
					{
						ereport(LOG,
								(errmsg("Datum stream block %s read is returning fixed-length item #%d "
										"(nth %d, item size %d, item begin %p, item offset " INT64_FORMAT ", integer " INT64_FORMAT ")",
										DatumStreamVersion_String(dsr->datumStreamVersion),
										dsr->physical_datum_index,
										dsr->nth,
										dsr->typeInfo.datumlen,
										dsr->datump,
										(int64) (dsr->datump - dsr->datum_beginp),
										(int64) * datum),
								 errdetail_datumstreamblockread(dsr),
								 errcontext_datumstreamblockread(dsr)));
					}
#endif
				}
				else
				{
					if (dsr->typeInfo.datumlen == 4)
					{
						column->values[row] = (uint32) dsr->delta_datum_p;
					}
					else if (dsr->typeInfo.datumlen == 8)
					{
						column->values[row] = dsr->delta_datum_p;
					}
					else
					{
						column->values[row] = 0;
						Assert(false);
					}
				}
			}
		}
	}

	dsr->nth += minimalBatchSize;
}
#endif
