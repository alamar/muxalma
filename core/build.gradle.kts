plugins {
    id("java")
    id("application")
}

group = "pvt.muxalma"

repositories {
    mavenCentral()
}

dependencies {
    // Netty - основной движок
    implementation("io.netty:netty-all:4.1.107.Final")

    // Логирование
    implementation("org.slf4j:slf4j-simple:2.0.13")

    // Тестирование
    testImplementation(platform("org.junit:junit-bom:5.10.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.25.3")
}

application {
    mainClass = "pvt.muxalma.Main"
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.test {
    useJUnitPlatform()
}
