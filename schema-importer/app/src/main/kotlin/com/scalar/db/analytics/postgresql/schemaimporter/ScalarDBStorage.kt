package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.config.DatabaseConfig
import com.scalar.db.storage.multistorage.MultiStorageConfig

/** ScalarDBStorage */
sealed interface ScalarDBStorage {
    interface SingleStorage : ScalarDBStorage {
        /** Foreign server name for this storage */
        val serverName: String
        /** Whether to use native FDW or ScalarDB FDW */
        val useNativeFdw: Boolean
        /** Corresponding configuration for this storage */
        val config: DatabaseConfig
        val name: String
            get() = config.storage
    }

    class JDBC(override val config: DatabaseConfig, override val serverName: String = "jdbc") :
        SingleStorage {
        override val useNativeFdw: Boolean = true
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
        override val useNativeFdw: Boolean = true

        val host: String
            get() = config.contactPoints.joinToString(",")

        val port: Int
            get() = config.contactPort
    }

    class DynamoDB(
        override val config: DatabaseConfig,
        override val serverName: String = "dynamodb"
    ) : SingleStorage {
        override val useNativeFdw: Boolean = false
    }

    class Cosmos(override val config: DatabaseConfig, override val serverName: String = "cosmos") :
        SingleStorage {
        override val useNativeFdw: Boolean = false
    }

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
                        .map { (name, props) ->
                            val name = name!!
                            val dbConfig = DatabaseConfig(props)
                            val serverName = "multi_storage_$name"
                            when (dbConfig.storage) {
                                "jdbc" -> name to JDBC(dbConfig, serverName)
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
                "jdbc" -> JDBC(config)
                "cassandra" -> Cassandra(config)
                "dynamo" -> DynamoDB(config)
                "cosmos" -> Cosmos(config)
                "multi-storage" -> MultiStorage.fromConfig(config)
                else -> throw IllegalArgumentException("${config.storage} is not supported yet")
            }
        }
    }
}
