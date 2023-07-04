#ifndef SCALARDB_H
#define SCALARDB_H

#include "c.h"
#include "postgres.h"

#include "executor/tuptable.h"
#include "foreign/foreign.h"
#include "jni.h"
#include "nodes/execnodes.h"

#include "option.h"

extern void scalardb_initialize(ScalarDbFdwOptions *opts);

extern jobject scalardb_scan_all(char *namespace, char *table_name,
				 List *attnames);
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

extern char *scalardb_to_string(jobject scan);

#endif
