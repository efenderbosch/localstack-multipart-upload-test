import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.32"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation("software.amazon.awssdk:s3:2.16.+")
    implementation("org.slf4j:slf4j-api:1.7.+")
    runtimeOnly("ch.qos.logback:logback-classic:1.2.+")
    testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.+")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions {
        apiVersion = "1.4"
        languageVersion = "1.4"
        jvmTarget = "11"
    }
}
