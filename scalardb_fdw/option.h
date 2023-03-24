#ifndef SCALARDB_FDW_OPTION_H
#define SCALARDB_FDW_OPTION_H

#include "postgres.h"

#include "nodes/pg_list.h"

typedef struct {
    char* config_file_path;
    char* max_heap_size;

    char* namespace;
    char* table_name;

    char* partition_key_column;
    List* clustering_key_columns;
    List* index_key_columns;
} ScalarDbFdwOptions;

void get_scalardb_fdw_options(Oid foreigntableid, ScalarDbFdwOptions* opts);

#endif
