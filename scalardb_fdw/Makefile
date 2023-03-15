MODULE_big = scalardb_fdw
OBJS = scalardb_fdw.o option.o scalardb.o

EXTENSION = scalardb_fdw
DATA = scalardb_fdw--1.0.sql

DATA_built = build/classes/java/main/ScalarDBUtils.class

REGRESS = scalardb_fdw

DATA_MODULE_DIR = $(DESTDIR)$(datadir)/$(datamoduledir)

OS = $(shell uname | tr '[:upper:]' '[:lower:]')

PG_CPPFLAGS = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(OS) -D'DATA_MODULE_DIR=$(DATA_MODULE_DIR)'

SHLIB_LINK+=-L$(JAVA_HOME)/lib/server -ljvm

EXTRA_CLEAN = build

JAVA_CLASSES = $(wildcard build/classes/java/main/*.class)

build/classes/java/main/%.class: src/main/java/%.java
	./gradlew classes

all: $(JAVA_CLASSES)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/scalardb_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifeq ($(MAJORVERSION),15)
	REGRESS := scalardb_fdw.pg$(MAJORVERSION)
endif
