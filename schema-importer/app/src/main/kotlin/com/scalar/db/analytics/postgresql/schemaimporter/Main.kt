/*
 * Copyright 2023 Scalar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.scalar.db.analytics.postgresql.schemaimporter

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.*
import com.github.ajalt.clikt.parameters.types.*
import java.nio.file.Path

class Main : CliktCommand(name = "scalardb-analytics-postgresql-schema-importer") {
    override fun run() = Unit
}

class Import : CliktCommand() {
    private val configPath: Path by
        option("--config", help = "Path to the ScalarDB configuration file").path().required()
    private val configPathOnPostgresHost: Path? by
        option(
            "--config-on-postgres-host",
            help =
            "Path to the ScalarDB configuration file on the PostgreSQL-running host." +
                " If this is not specified, the same value as --config will be used.",
        )
            .path()

    private val namespaces: Set<String> by
        option("-n", "--namespace", help = "Namespaces to import into the analytics instance")
            .multiple(required = true)
            .unique()

    private val host: String by option("--host", help = "PostgreSQL host").default("localhost")
    private val port: Int by option("--port", help = "PostgreSQL port").int().default(5432)
    private val database: String by
        option("--database", help = "PostgreSQL database").default("postgres")
    private val user: String by option("--user", help = "PostgreSQL user").default("postgres")
    private val password: String by option("--password", help = "PostgreSQL password").default("")

    private val debug: Boolean by
        option("--debug", help = "Enable debug mode").flag(default = false)

    private val url: String
        get() = "jdbc:postgresql://$host:$port/$database"

    override fun run() {
        if (debug) {
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG")
        }

        val param =
            Parameter(
                configPath = configPath,
                configPathOnPostgresHost = configPathOnPostgresHost,
                namespaces = namespaces,
                url = url,
                user = user,
                password = password,
            )
        importSchema(param)
    }
}

data class Parameter(
    val configPath: Path,
    val configPathOnPostgresHost: Path?,
    val namespaces: Set<String>,
    val url: String,
    val user: String,
    val password: String,
)

fun main(args: Array<String>) = Main().subcommands(Import()).main(args)
