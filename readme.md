The Avro Enchiridion
==============================================================================

![Java CI](https://github.com/RyanSkraba/avro-enchiridion/workflows/Java%20CI/badge.svg)

_[**Enchiridion**](https://en.wikipedia.org/wiki/Enchiridion): **A small manual or handbook.**_  It's a bit like a tech [cook book](https://www.oreilly.com/search/?query=cookbook), but a bigger, fancier, SEO-optimizabler word.

<!-- 2020/05/25: 920 O'Reilly results
     2020/06/05: 4758 O'Reilly results (but changed the search URL)
     2020/07/30: 5043 O'Reilly results
     2022/01/25: 5164 O'Reilly results -->

This project describes how to do many common Java tasks using the Avro serialization library.

The main project uses the latest release of [Avro 1.10.2](https://mvnrepository.com/artifact/org.apache.avro/avro/1.10.2), but includes modules that run the same tests on previous versions of Avro.

Java Topics
------------------------------------------------------------------------------

| I want to...                                | See...                 |
|---------------------------------------------|------------------------|
| Read/write one datum to a byte array        | [SerializeToBytesTest] |
| Read/write one datum to a ByteBuffer        | [SerializeToBytesTest] |
| Read/write one datum to an Avro JSON String | [SerializeToJsonTest]  |
| Read from/write to an Avro file             | [AvroFileTest]         |
| Append a record to an Avro file             | [AvroFileTest]         |

[SerializeToBytesTest]: core/src/test/java/com/skraba/avro/enchiridion/core/SerializeToBytesTest.java
[SerializeToJsonTest]: core/src/test/java/com/skraba/avro/enchiridion/core/SerializeToJsonTest.java
[AvroFileTest]: core/src/test/java/com/skraba/avro/enchiridion/core/file/AvroFileTest.java

### Logical Types ([spec][AvroSpecLogicalType])

| I want to...                                                               | See...                                                |
|----------------------------------------------------------------------------|-------------------------------------------------------|
| Use [BigDecimal][BigDecimal] datum with a [GenericData][GenericData] model | [DecimalPrecisionAndScaleTest] (see the static block) |
| Create my own logical type                                                 | TODO                                                  |

[AvroSpecLogicalType]: https://avro.apache.org/docs/current/spec.html#Logical+Types
[BigDecimal]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html
[GenericData]: https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericData.html
[DecimalPrecisionAndScaleTest]: core/src/test/java/com/skraba/avro/enchiridion/core/logical/DecimalPrecisionAndScaleTest.java

### Schema resolution ([spec][AvroSpecSchemaResolution])

| I want to...                       | See...                      |
|------------------------------------|-----------------------------|
| Evolve my schema by adding a field | [EvolveAddAFieldTest]       |
| ... by removing a field            | [EvolveRemoveAFieldTest]    |
| ... by renaming a field            | [EvolveRenameAFieldTest]    |
| ... by reordering fields           | [EvolveReorderFieldsTest]   |
| ... by widening a primitive        | [EvolveWidenPrimitivesTest] |
| ... by adding a union              | [EvolveUnionTest]           |
| ... by adding an enum symbol       |                             |
| ... by removing an enum symbol     |                             |

[AvroSpecSchemaResolution]: https://avro.apache.org/docs/current/spec.html#Schema+Resolution
[EvolveAddAFieldTest]: core/src/test/java/com/skraba/avro/enchiridion/core/evolution/EvolveAddAFieldTest.java
[EvolveRemoveAFieldTest]: core/src/test/java/com/skraba/avro/enchiridion/core/evolution/EvolveRemoveAFieldTest.java
[EvolveRenameAFieldTest]: core/src/test/java/com/skraba/avro/enchiridion/core/evolution/EvolveRenameAFieldTest.java
[EvolveReorderFieldsTest]: core/src/test/java/com/skraba/avro/enchiridion/core/evolution/EvolveReorderFieldsTest.java
[EvolveWidenPrimitivesTest]: core/src/test/java/com/skraba/avro/enchiridion/core/evolution/EvolveWidenPrimitivesTest.java
[EvolveUnionTest]: core/src/test/java/com/skraba/avro/enchiridion/core/evolution/EvolveUnionTest.java

Modules
------------------------------------------------------------------------------

| module                                                 | description                                                                                   |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| [avro-resources](avro-resources/readme.md)             | Reusable resources for testing Avro (Schema JSON strings, etc).                               |
| [core](core/readme.md)                                 | Unit tests and examples for the Avro Java SDK.                                                |
| [core17x](core17x/readme.md)                           | Helper project for running tests in Avro 1.7.x                                                |
| [core18x](core18x/readme.md)                           | Helper project for running tests in Avro 1.8.x                                                |
| [core19x](core19x/readme.md)                           | Helper project for running tests in Avro 1.9.x                                                |
| [core-master-snapshot](core-master-snapshot/readme.md) | Helper project for running tests in the latest SNAPSHOT releases from the Avro master branch. |
| [ipc](ipc/readme.md)                                   | Unit tests and examples for client/server communication with Avro protocols.                  |
| [plugin](plugin/readme.md)                             | Using the maven plugin to generate Avro classes.                                              |

Building an Apache Avro SNAPSHOT locally.
------------------------------------------------------------------------------

```bash
# Get the Avro source code.
git clone git@github.com:apache/avro.git

# Create an Avro docker image with all of the necessary tools. 
cd avro
./build.sh docker

# Run a container with your local .m2 mounted
docker run --rm -t -i \
  -v $PWD:$HOME/avro -w $HOME/avro \
  -v $HOME/.m2:$HOME/.m2 \
  -u $USER avro-build-$USER \
  ./build.sh test
```
