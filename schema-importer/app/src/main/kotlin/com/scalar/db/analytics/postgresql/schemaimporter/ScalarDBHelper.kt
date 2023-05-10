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
                "Found multiple rows for the jar file of scalardb_fdw: $path, $secondPath"
            )
        } else {
            return path
        }
    }
}
