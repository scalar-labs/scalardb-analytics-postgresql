package com.scalar.db.analytics.postgresql.schemaimporter

import com.scalar.db.api.DistributedStorageAdmin
import com.scalar.db.service.StorageFactory
import java.nio.file.Path

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
