plugins {
    id("java")
}

subprojects {
    group = rootProject.group
    version = rootProject.version

    apply(plugin = "java")
    apply(plugin = "maven-publish")

    configure<PublishingExtension> {
        publications {
            create<MavenPublication>("maven") {
                groupId = project.group.toString()
                version = project.version.toString()
                artifactId = project.name  // Uses module folder name as artifactId

                from(components["java"])
            }
        }
    }
}