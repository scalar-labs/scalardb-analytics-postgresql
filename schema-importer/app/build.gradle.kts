plugins {
    id("org.jetbrains.kotlin.jvm") version "1.8.20"
    id("com.github.johnrengelman.shadow") version "7.1.2"

    application
}

group = "com.scalar-labs"

project.version = "0.0.1-SNAPSHOT"

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(8)
}

dependencies {
    implementation("com.scalar-labs:scalardb:3.8.0")
    implementation("com.github.ajalt.clikt:clikt:3.5.2")
    implementation("org.postgresql:postgresql:42.5.1")

    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:5.9.2")
    testImplementation("io.mockk:mockk:1.13.5")
}

application {
    mainClass.set("com.scalar.db.analytics.postgresql.schemaimporter.MainKt")
}

tasks.shadowJar {
    archiveBaseName.set("scalardb-analytics-postgresql-schema-importer")
    archiveClassifier.set("")
}

tasks.test {
    useJUnitPlatform()
}
