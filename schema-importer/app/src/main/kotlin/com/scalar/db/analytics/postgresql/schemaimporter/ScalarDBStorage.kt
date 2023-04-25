package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.config.DatabaseConfig
import com.scalar.db.storage.multistorage.MultiStorageConfig

/** ScalarDBStorage */
sealed interface ScalarDBStorage {
    sealed interface SingleStorage : ScalarDBStorage {
        /** Foreign server name for this storage */
        val serverName: String
        /** Corresponding configuration for this storage */
        val config: DatabaseConfig
        val name: String
            get() = config.storage
    }

    class Jdbc(override val config: DatabaseConfig, override val serverName: String = "jdbc") :
        SingleStorage {
        val url: String
            get() =
                config.contactPoints.let {
                    if (it.size != 1) {
                        throw IllegalArgumentException("JDBC only supports a single contact point. Got ${it.joinToString(",")}")
                    }
                    it.first()
                }
    }

    class Cassandra(
        override val config: DatabaseConfig,
        override val serverName: String = "cassandra"
    ) : SingleStorage {
        val host: String
            get() = config.contactPoints.joinToString(",")

        val port: Int
            get() = config.contactPort
    }

    class DynamoDB(
        override val config: DatabaseConfig,
        override val serverName: String = "dynamodb"
    ) : SingleStorage

    class Cosmos(override val config: DatabaseConfig, override val serverName: String = "cosmos") : SingleStorage

    class MultiStorage(
        val storages: Map<String, SingleStorage>,
        private val namespaceStorageMap: Map<String, String>
    ) : ScalarDBStorage {
        fun getStorageForNamespace(namespace: String): SingleStorage {
            val name =
                namespaceStorageMap[namespace]
                    ?: throw IllegalArgumentException("No storage found for namespace $namespace")
            return storages[name]
                ?: throw IllegalArgumentException("No storage found for name $name")
        }

        companion object {
            fun fromConfig(config: DatabaseConfig): MultiStorage {
                val multiStorageConfig = MultiStorageConfig(config)
                val storages =
                    multiStorageConfig.databasePropertiesMap
                        .map { (nullableName, props) ->
                            val name = nullableName!!
                            val dbConfig = DatabaseConfig(props)
                            val serverName = "multi_storage_$name"
                            when (dbConfig.storage) {
                                "jdbc" -> name to Jdbc(dbConfig, serverName)
                                "cassandra" -> name to Cassandra(dbConfig, serverName)
                                "dynamo" -> name to DynamoDB(dbConfig, serverName)
                                "cosmos" -> name to Cosmos(dbConfig, serverName)
                                "multi-storage" ->
                                    throw IllegalArgumentException("MultiStorage cannot be nested")
                                else ->
                                    throw IllegalArgumentException(
                                        "${dbConfig.storage} is not supported yet"
                                    )
                            }
                        }
                        .toMap()
                return MultiStorage(storages, multiStorageConfig.namespaceStorageMap)
            }
        }
    }

    companion object {
        fun fromConfig(config: DatabaseConfig): ScalarDBStorage {
            return when (config.storage) {
                "jdbc" -> Jdbc(config)
                "cassandra" -> Cassandra(config)
                "dynamo" -> DynamoDB(config)
                "cosmos" -> Cosmos(config)
                "multi-storage" -> MultiStorage.fromConfig(config)
                else -> throw IllegalArgumentException("${config.storage} is not supported yet")
            }
        }
    }
}

const val SCALARDB_FDW: String = "scalardb_fdw"
const val JDBC_FDW: String = "jdbc_fdw"
const val CASSANDRA_FDW: String = "cassandra2_fdw"

fun useScalarDBFdw(storage: ScalarDBStorage.SingleStorage): Boolean =
    singleStorageToFdw(storage) == SCALARDB_FDW

fun singleStorageToFdw(storage: ScalarDBStorage.SingleStorage): String =
    when (storage) {
        is ScalarDBStorage.Jdbc -> JDBC_FDW
        is ScalarDBStorage.Cassandra -> CASSANDRA_FDW
        is ScalarDBStorage.DynamoDB -> SCALARDB_FDW
        is ScalarDBStorage.Cosmos -> SCALARDB_FDW
    }

fun storageToFdw(storage: ScalarDBStorage): Set<String> {
    return when (storage) {
        is ScalarDBStorage.SingleStorage -> setOf(singleStorageToFdw(storage))
        is ScalarDBStorage.MultiStorage -> storage.storages.values.map { singleStorageToFdw(it) }.toSet()
    }
}
