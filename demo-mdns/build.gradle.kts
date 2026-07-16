plugins {
    kotlin("jvm") version "2.3.21"
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":core"))
    implementation("org.jmdns:jmdns:3.6.3")
}

kotlin {
    jvmToolchain(17)
}
