#
# Copyright 2023 Scalar, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
MODULE_big = scalardb_fdw
OBJS = scalardb_fdw.o option.o scalardb.o condition.o column_metadata.o pgport.o cost.o pathkeys.o

EXTENSION = scalardb_fdw
DATA = scalardb_fdw--1.0.sql

scalardb_version = 3.11.0

scalardb_jar = scalardb-$(scalardb_version)-all.jar
scalardb_jar_built = scalardb-utils/build/libs/$(scalardb_jar)
scalardb_jar_path = $(DESTDIR)$(datadir)/$(datamoduledir)/$(scalardb_jar)

DATA_built = $(scalardb_jar_built)

REGRESS = scalardb_fdw

OS = $(shell uname | tr '[:upper:]' '[:lower:]')

PG_CPPFLAGS = -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/$(OS) -D'SCALARDB_JAR_PATH=$(scalardb_jar_path)'

libjvm_path = $(word 1, $(shell find -L $(JAVA_HOME) -name libjvm\.*))
libjvm_dir = $(dir $(libjvm_path))

ifeq ($(OS),darwin)
	SHLIB_LINK+=-L$(libjvm_dir) -ljvm -rpath $(libjvm_dir)
else
	SHLIB_LINK+=-L$(libjvm_dir) -ljvm
endif

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
