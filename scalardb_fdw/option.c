#include "c.h"
#include "postgres.h"

#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/rel.h"

#include "option.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct OptionEntry {
	const char *optname;
	Oid optcontext; /* Oid of catalog in which option may appear */
};

/*
 * Valid options for scalardb_fdw.
 */
static struct OptionEntry valid_options[] = {
	{ "config_file_path", ForeignServerRelationId },
	{ "max_heap_size", ForeignServerRelationId },

	{ "namespace", ForeignTableRelationId },
	{ "table_name", ForeignTableRelationId },

	/* Sentinel */
	{ NULL, InvalidOid }
};

static bool is_valid_option(const char *option, Oid context);

PG_FUNCTION_INFO_V1(scalardb_fdw_validator);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses scalardb_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum scalardb_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);

	char *config_file_path = NULL;
	char *namespace = NULL;
	char *table_name = NULL;

	ListCell *cell;

	ereport(DEBUG3, errmsg("entering function %s", __func__));

	foreach(cell, options_list) {
		DefElem *def = (DefElem *)lfirst(cell);

		if (!is_valid_option(def->defname, catalog)) {
			const struct OptionEntry *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++) {
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s",
							 (buf.len > 0) ? ", " :
									 "",
							 opt->optname);
			}

			ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("invalid option \"%s\"", def->defname),
				 buf.len > 0 ?
					 errhint("Valid options in this context are: %s",
						 buf.data) :
					 errhint("There are no valid options in this context.")));
		}

		if (strcmp(def->defname, "config_file_path") == 0) {
			if (!has_privs_of_role(GetUserId(),
#if PG_VERSION_NUM >= 140000
					       ROLE_PG_READ_SERVER_FILES
#else
					       DEFAULT_ROLE_READ_SERVER_FILES
#endif
					       )) {
				ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("only superuser or a role with privileges of the "
						"pg_read_server_files role may specify the "
						"config_file_path")));
			}
			config_file_path = defGetString(def);
		} else if (strcmp(def->defname, "max_heap_size") == 0) {
			// TODO validate the value for the -Xmx format
		} else if (strcmp(def->defname, "namespace") == 0) {
			namespace = defGetString(def);
		} else if (strcmp(def->defname, "table_name") == 0) {
			table_name = defGetString(def);
		}
	}

	if (catalog == ForeignServerRelationId && config_file_path == NULL) {
		ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			 errmsg("config_file_path is required for a foreign server "
				"of scalardb_fdw")));
	}

	if (catalog == ForeignTableRelationId && namespace == NULL) {
		ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			 errmsg("namespace is required for a foreign table "
				"of scalardb_fdw")));
	}

	if (catalog == ForeignTableRelationId && table_name == NULL) {
		ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
			 errmsg("table_name is required for a foreign table "
				"of scalardb_fdw")));
	}

	PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool is_valid_option(const char *option, Oid context)
{
	const struct OptionEntry *opt;

	for (opt = valid_options; opt->optname; opt++) {
		if (context == opt->optcontext &&
		    strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch all options for the foreign table.
 *
 */
void get_scalardb_fdw_options(Oid foreigntableid, ScalarDbFdwOptions *opts)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	List *options;
	ListCell *cell;

	opts->config_file_path = NULL;
	opts->max_heap_size = NULL;
	opts->namespace = NULL;
	opts->table_name = NULL;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	foreach(cell, options) {
		DefElem *def = (DefElem *)lfirst(cell);

		if (strcmp(def->defname, "config_file_path") == 0) {
			opts->config_file_path = defGetString(def);
		} else if (strcmp(def->defname, "max_heap_size") == 0) {
			opts->max_heap_size = defGetString(def);
		} else if (strcmp(def->defname, "namespace") == 0) {
			opts->namespace = defGetString(def);
		} else if (strcmp(def->defname, "table_name") == 0) {
			opts->table_name = defGetString(def);
		}
	}
}
