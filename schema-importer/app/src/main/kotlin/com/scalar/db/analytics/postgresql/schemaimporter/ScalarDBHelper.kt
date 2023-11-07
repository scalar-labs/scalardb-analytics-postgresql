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

import com.scalar.db.api.DistributedStorageAdmin
import com.scalar.db.service.StorageFactory
import java.nio.file.Path
import java.sql.Statement

fun <T> useStorageAdmin(configPath: Path, f: (DistributedStorageAdmin) -> T): T {
    val sf = StorageFactory.create(configPath)
    val sa = sf.storageAdmin
    val ret =
        try {
            f(sa)
        } finally {
            sa.close()
        }
    return ret
}

fun findScalarDBFdwJarFile(statement: Statement): String {
    val rs = statement.executeQuery("select scalardb_fdw_get_jar_file_path() as path;")
    var stat = rs.next()
    if (!stat) {
        throw RuntimeException("Failed to find the jar file of scalardb_fdw")
    } else {
        val path = rs.getString("path")
        stat = rs.next()
        if (stat) {
            val secondPath = rs.getString("path")
            throw RuntimeException(
                "Found multiple rows for the jar file of scalardb_fdw: $path, $secondPath",
            )
        } else {
            return path
        }
    }
}
