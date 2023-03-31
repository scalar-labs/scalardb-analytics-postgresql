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

fun isUserColumn(c: String) =
    Constant.METADATA_COLUMNS.contains(c).not() && c.startsWith("before_").not()

object Constant {
    const val TX_STATE_COL = "tx_state"

    val METADATA_COLUMNS =
        setOf(
            "before_tx_id",
            "before_tx_prepared_at",
            "before_tx_state",
            "before_tx_version",
            "tx_committed_at",
            "tx_id",
            "tx_prepared_at",
            "tx_state",
            "tx_version",
        )
}
