plugins {
    kotlin("jvm") version "1.9.25"
}

repositories {
    mavenCentral()
}

dependencies {
    api(project(":core"))
    api("org.jmdns:jmdns:3.6.3")
}

kotlin {
    jvmToolchain(17)
}
