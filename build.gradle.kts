plugins {
    kotlin("jvm") version "1.9.23"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.hazelcast:hazelcast:5.4.0")
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass = "org.example.MainKt"
}