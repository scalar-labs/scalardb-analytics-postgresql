#ifndef SCALARDB_H
#define SCALARDB_H

#include "c.h"
#include "postgres.h"

#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "jni.h"
#include "nodes/execnodes.h"

#include "option.h"

/*
 * Represents a key condition of Scan operation in the executor phase.
 */
typedef struct {
	/* column name */
	char *name;
	/* condition value*/
	Datum value;
	/* type of value */
	Oid value_type;
} ScalarDbFdwScanCondition;

/*
 * Represents a clustering key boundary of Scan operation in the executor phase.
 */
typedef struct {
	/* column names. List of String */
	List *names;
	/* condition values for start boundary */
	Datum *start_values;
	/* a number of start_values */
	size_t num_start_values;
	/* types of values for start boundary. List of Oid */
	List *start_value_types;
	/* indicates whether start boundary is inclusive */
	bool start_inclusive;
	/* condition values for end boundary  */
	Datum *end_values;
	/* a number of end_values */
	size_t num_end_values;
	/* types of values for end boundary. List of Oid */
	List *end_value_types;
	/* indicates whether end boundary is inclusive */
	bool end_inclusive;
	/* indicates whether each condition is equal operation */
	List *is_equals;
} ScalarDbFdwScanBoundary;

extern void scalardb_initialize(ScalarDbFdwOptions *opts);

extern jobject scalardb_scan_all(char *namespace, char *table_name,
				 List *attnames);
extern jobject scalardb_scan(char *namespace, char *table_name, List *attnames,
			     ScalarDbFdwScanCondition *scan_conds,
			     size_t scan_conds_len,
			     ScalarDbFdwScanBoundary *boundary);
extern jobject scalardb_scan_with_index(char *namespace, char *table_name,
					List *attnames,
					ScalarDbFdwScanCondition *scan_conds,
					size_t scan_conds_len);

extern void scalardb_release_scan(jobject scan);

extern jobject scalardb_start_scan(jobject scan);

extern jobject scalardb_scanner_one(jobject scanner);
extern void scalardb_scanner_release_result(void);
extern void scalardb_scanner_close(jobject scanner);

extern int scalardb_list_size(jobject list);
extern jobject scalardb_list_iterator(jobject list);

extern bool scalardb_iterator_has_next(jobject iterator);
extern jobject scalardb_iterator_next(jobject iterator);

extern bool scalardb_optional_is_present(jobject optional);
extern jobject scalardb_optional_get(jobject optional);

extern bool scalardb_result_is_null(jobject result, char *attname);
extern bool scalardb_result_get_boolean(jobject result, char *attname);
extern int scalardb_result_get_int(jobject result, char *attname);
extern long scalardb_result_get_bigint(jobject result, char *attname);
extern float scalardb_result_get_float(jobject result, char *attname);
extern double scalardb_result_get_double(jobject result, char *attname);
extern text *scalardb_result_get_text(jobject result, char *attname);
extern bytea *scalardb_result_get_blob(jobject result, char *attname);
extern int scalardb_result_columns_size(jobject result);

extern void scalardb_get_paritition_key_names(char *namespace, char *table_name,
					      List **partition_key_names);
extern void scalardb_get_clustering_key_names(char *namespace, char *table_name,
					      List **clustering_key_names);
extern void scalardb_get_secondary_index_names(char *namespace,
					       char *table_name,
					       List **secondary_index_names);

extern char *scalardb_to_string(jobject scan);

#endif
