Download all
============

This module doesn't do anything but depend on all of the Nexus artifacts
published by the Apache Avro project.  It can be useful to download all of
the artifacts to the local repository in one step.

```bash
# Download all of the 1.10.x jars to the local maven repo.
mvn -Davro.version=1.10.0 dependency:go-offline
mvn -Davro.version=1.10.1 dependency:go-offline
# If this is a release being staged
mvn -Davro.version=1.10.2 -Papache-staging dependency:go-offline
```