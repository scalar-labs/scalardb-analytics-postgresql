#ifndef SCALARDB_FDW_H
#define SCALARDB_FDW_H

#include "c.h"
#include "postgres.h"

#include "condition.h"
#include "option.h"

/*
 * The plan state is set up in scalardbGetForeignRelSize and stashed away in
 * baserel->fdw_private and fetched in scalardbGetForeignPaths.
 */
typedef struct {
	ScalarDbFdwOptions options;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset *attrs_used;

	/* Conditions on the index keys or secondary indexes that are pushed to ScalarDB side */
	List *remote_conds;
	/* Conditions that are evaluated locally */
	List *local_conds;
	/* the clustering keys that are pushed to ScalarDB side */
	ScalarDbFdwClusteringKeyBoundary boundary;
	/* Type of Scan executed on the ScalarDB side. This must be consistent with the condtitions in remote_conds */
	ScalarDbFdwScanType scan_type;

	/* estimate of physical size */
	BlockNumber pages;
	/* estimate of number of data rows */
	double tuples;

	/* set of the column metadata of the table*/
	ScalarDbFdwColumnMetadata column_metadata;
} ScalarDbFdwPlanState;

#endif
