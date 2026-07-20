plugins {
    kotlin("jvm") version "2.3.21"
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
