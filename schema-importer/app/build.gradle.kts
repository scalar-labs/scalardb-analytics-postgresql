plugins {
    id("org.jetbrains.kotlin.jvm") version "1.8.20"
    id("com.github.johnrengelman.shadow") version "7.1.2"
    id("com.diffplug.spotless") version "6.13.0"

    application
}

group = "com.scalar-labs"

project.version = "3.10.0"

repositories {
    mavenCentral()
}

kotlin {
    jvmToolchain(8)
}

dependencies {
    implementation("com.scalar-labs:scalardb:3.10.1")
    implementation("com.github.ajalt.clikt:clikt:3.5.2")
    implementation("org.postgresql:postgresql:42.5.1")
    implementation("io.github.microutils:kotlin-logging-jvm:3.0.5")
    implementation("org.slf4j:slf4j-api:2.0.7")
    implementation("org.slf4j:slf4j-simple:2.0.7")

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

spotless {
  kotlin {
    ktlint()
        .setEditorConfigPath("$rootDir/.editorconfig")
        .editorConfigOverride(mapOf(
            "max_line_length" to 130,
            "ij_kotlin_packages_to_use_import_on_demand" to
                "java.util.*,com.github.ajalt.clikt.parameters.**,io.mockk",
        ))

  }
}

tasks.check {
    dependsOn(tasks.spotlessCheck)
}
