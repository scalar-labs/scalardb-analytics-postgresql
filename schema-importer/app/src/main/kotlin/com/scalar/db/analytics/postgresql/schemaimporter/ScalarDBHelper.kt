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

// TODO: Using the constant filename make this tightly coupled with the installation of scalardb_fdw.
//       We should find a better way to do this.
const val ScalarDBFdwJarFile: String = "scalardb-all.jar"

fun findScalarDBFdwJarFile(statement: Statement, filename: String = ScalarDBFdwJarFile): String =
    "${findPostgreExtensionDir(statement)}/$filename"

private fun findPostgreExtensionDir(statement: Statement): String {
    val rs = statement.executeQuery("select setting from pg_config where name = 'SHAREDIR';")
    var stat = rs.next()
    if (!stat) {
        throw RuntimeException("Failed to find the SHAREDIR of PostgreSQL")
    } else {
        val shareDir = rs.getString("setting")
        stat = rs.next()
        if (stat) {
            throw RuntimeException("Found multiple rows for SHAREDIR of PostgreSQL")
        } else {
            return "$shareDir/extension"
        }
    }
}
