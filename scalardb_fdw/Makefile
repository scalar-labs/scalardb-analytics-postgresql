MODULE_big = scalardb_fdw
OBJS = scalardb_fdw.o option.o scalardb.o condition.o scalardb_fdw_util.o pgport.o

EXTENSION = scalardb_fdw
DATA = scalardb_fdw--1.0.sql

scalardb_version = 3.9.0

scalardb_jar = scalardb-$(scalardb_version)-all.jar
scalardb_jar_built = scalardb-utils/build/libs/$(scalardb_jar)
scalardb_jar_path = $(DESTDIR)$(datadir)/$(datamoduledir)/$(scalardb_jar)

DATA_built = $(scalardb_jar_built)

REGRESS = scalardb_fdw

OS = $(shell uname | tr '[:upper:]' '[:lower:]')

PG_CPPFLAGS = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(OS) -D'SCALARDB_JAR_PATH=$(scalardb_jar_path)'

libjvm_path = $(shell find -L $(JAVA_HOME) -name libjvm\.*)

SHLIB_LINK+=-L$(dir $(libjvm_path)) -ljvm

EXTRA_CLEAN = build

JAVA_SOURCES = $(wildcard src/main/java/*.java)

$(scalardb_jar_built): $(JAVA_SOURCES)
	./gradlew scalardb-utils:shadowJar

all: $(scalardb_jar_built)

scalardb-version:
	@echo $(scalardb_version)

format:
	clang-format -i *.c *.h

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

ifeq ($(MAJORVERSION),15)
	REGRESS := scalardb_fdw.pg$(MAJORVERSION)
endif
