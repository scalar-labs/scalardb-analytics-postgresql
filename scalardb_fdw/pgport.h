#ifndef SCALARDB_FDW_PGPORT_H
#define SCALARDB_FDW_PGPORT_H

#include "c.h"
#include "postgres.h"
#include "nodes/value.h"

#if PG_VERSION_NUM < 150000
typedef Value String;
#define boolVal(v) ((bool)intVal(v))

extern Value *makeBoolean(bool val);

#endif
#endif
