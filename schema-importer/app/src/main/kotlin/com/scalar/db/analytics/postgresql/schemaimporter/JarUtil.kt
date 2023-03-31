package com.scalar.db.analytics.postgresql.schemaimporter

fun getRunningJarFile(): String = Main::class.java.protectionDomain.codeSource.location.toURI().path
