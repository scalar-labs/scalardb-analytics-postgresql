CREATE FUNCTION scalardb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION scalardb_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER scalardb_fdw
  HANDLER scalardb_fdw_handler
  VALIDATOR scalardb_fdw_validator;

CREATE FUNCTION scalardb_fdw_get_jar_file_path()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

REVOKE ALL ON FUNCTION scalardb_fdw_get_jar_file_path() FROM PUBLIC;
