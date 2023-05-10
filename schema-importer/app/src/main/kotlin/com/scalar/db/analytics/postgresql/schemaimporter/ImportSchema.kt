package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.config.DatabaseConfig
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

fun importSchema(param: Parameter) {
    val config = DatabaseConfig(param.configPath)
    val storage = ScalarDBStorage.fromConfig(config)

    logger.info { "Start to import schema into ${param.url} with ${param.configPath}" }
    useDatabaseContext(param.url, param.user, param.password) { ctx ->
        useStorageAdmin(param.configPath) { admin ->
            CreateExtension(ctx, storage).run()
            CreateServers(ctx, storage, param.configPath, param.configPathOnPostgresHost).run()
            CreateSchema(ctx, param.namespaces).run()
            CreateUserMappings(ctx, storage).run()
            CreateForeignTables(ctx, param.namespaces, storage, admin).run()
            CreateViews(ctx, param.namespaces, admin).run()
        }
    }
    logger.info { "Finished importing schema" }
}
