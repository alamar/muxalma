
repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":core"))
    implementation("com.h2database:h2:2.4.240")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}