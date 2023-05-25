# Releasing Twister

## Pushing to Maven Central

1. Make sure you have a GPG key and it is published to a key server. See [here](http://central.sonatype.org/pages/working-with-pgp-signatures.html) for more information.
2. Make sure you have a Sonatype JIRA account. See [here](http://central.sonatype.org/pages/ossrh-guide.html) for more information.
3. Make sure you have a `~/.m2/settings.xml` and `~/.m2/settings-security.xml` file.
4. Run `mvn clean deploy -P release` from the root of the project.