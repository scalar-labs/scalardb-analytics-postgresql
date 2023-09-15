FROM ghcr.io/scalar-labs/jdk8-postgres15:1.0.3

ENV JDBC_FDW_VERSION=0.3.0-forked \
    CASSANDRA_CPP_DRIVER_VERSION=2.16.2 \
    CASSANDRA_FDW_VERSION=ae5e8145e341f50c150a05d464a9fa4a5eb91179

COPY . /tmp/scalardb_fdw

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        wget \

        # Required to build postgresql extensions
        postgresql-server-dev-${PG_MAJOR} \
        build-essential \

        # Required for building cassandra cpp driver
        cmake \
        libuv1-dev \
        zlib1g-dev \

    # jdbc_fdw
    && cd /tmp \
    && ln -s $(find /usr/local/openjdk-8 -name libjvm.so) /usr/lib \
    && wget -q https://github.com/scalar-labs/jdbc_fdw/archive/refs/tags/v${JDBC_FDW_VERSION}.tar.gz \
    && tar zxf v${JDBC_FDW_VERSION}.tar.gz \
    && cd jdbc_fdw-${JDBC_FDW_VERSION} \
    && make USE_PGXS=1 install \

    # cassandra cpp driver
    && cd /tmp \
    && wget -q https://github.com/datastax/cpp-driver/archive/refs/tags/${CASSANDRA_CPP_DRIVER_VERSION}.tar.gz \
    && tar zxf ${CASSANDRA_CPP_DRIVER_VERSION}.tar.gz \
    && cd cpp-driver-${CASSANDRA_CPP_DRIVER_VERSION} \
    && mkdir build \
    && cd build \
    && cmake .. \
    && make install \
    && ldconfig \

    # cassandra_fdw
    && cd /tmp \
    && wget -q https://github.com/jaiminpan/cassandra2_fdw/archive/${CASSANDRA_FDW_VERSION}.tar.gz \
    && tar zxf ${CASSANDRA_FDW_VERSION}.tar.gz \
    && cd cassandra2_fdw-${CASSANDRA_FDW_VERSION} \
    && make USE_PGXS=1 install \

    # scalardb_fdw
    && cd /tmp/scalardb_fdw \
    && make USE_PGXS=1 install \

    # cleanup
    && cd /tmp \
    && rm -rf /tmp/* \
    && apt-get purge -y --auto-remove \
        wget \
        postgresql-server-dev-${PG_MAJOR} \
        build-essential \
        cmake \
        libuv1-dev \
        zlib1g-dev \
    && apt-get install -y --no-install-recommends \
        libuv1 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*
