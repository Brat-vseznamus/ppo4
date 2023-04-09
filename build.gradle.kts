plugins {
    kotlin("jvm") version "1.8.0"
    id("org.jetbrains.kotlin.plugin.serialization") version "1.7.21"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation("io.reactivex:rxjava:1.3.8")
    implementation("io.reactivex:rxnetty-common:0.5.3")
    implementation("io.reactivex:rxnetty-http:0.5.3")
    implementation("io.netty:netty-codec-http:4.1.15.Final")
    implementation("org.mongodb:mongodb-driver-rx:1.5.0")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-runtime:1.0-M1-1.4.0-rc")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(8)
}

application {
    mainClass.set("MainKt")
}

