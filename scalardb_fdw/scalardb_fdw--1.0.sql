--
-- Copyright 2023 Scalar, Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
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
