plugins {
    id("java")
    id("org.jetbrains.intellij.platform") version "2.5.0"
}

group = "com.keboola"
version = "1.0.0"

repositories {
    mavenCentral()
    intellijPlatform {
        defaultRepositories()
    }
}

dependencies {
    intellijPlatform {
        local("/Applications/DataGrip.app")
        bundledPlugin("com.intellij.database")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

intellijPlatform {
    pluginConfiguration {
        id = "com.keboola.datagrip"
        name = "Keboola Database Support"
        version = project.version.toString()
        description = """
            Adds Keboola as a recognized database type in DataGrip.
            Uses JDBC metadata introspection for database/schema/table enumeration,
            supporting all Keboola Query Service backends (Snowflake, BigQuery, DuckDB).
        """.trimIndent()
        vendor {
            name = "Keboola"
            url = "https://www.keboola.com"
        }
        ideaVersion {
            sinceBuild = "253"
        }
    }
}

tasks {
    buildSearchableOptions {
        enabled = false
    }
}
