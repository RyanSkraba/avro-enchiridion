The Avro Enchiridion
====================

_[**Enchiridion**](https://en.wikipedia.org/wiki/Enchiridion): A small manual or handbook._  It's a bit like a tech [cook book](https://www.oreilly.com/search/?query=cookbook), but a bigger, fancier, SEO-optimizabler word.

<!-- 2020/05/25: 920 O'Reilly results
     2020/06/05: 4758 O'Reilly results (but changed the search URL) -->

This project describes how to do many common Java tasks using the Avro serialization library.

The main project uses the latest release of [Avro 1.10.0](https://mvnrepository.com/artifact/org.apache.avro/avro/1.10.0), but includes modules that run the same tests on previous versions of Avro.

Java Topics
-----------

| I want to...  | See... |
| ------------- | ------------- |
| Read/write one datum to a byte array | [SerializeToBytesTest][SerializeToBytesTest]
| Read/write one datum to a ByteBuffer |   
| Read/write one datum to an Avro JSON String |  
| Read from/write to an Avro file | [AvroFileTest][AvroFileTest]

[SerializeToBytesTest]: core/src/test/java/com/skraba/avro/enchiridion/core/SerializeToBytesTest.java
[AvroFileTest]: core/src/test/java/com/skraba/avro/enchiridion/core/file/AvroFileTest.java

### Logical Types

| I want to...  | See... |
| ------------- | ------------- |
| Use [BigDecimal][BigDecimal] datum with a [GenericData][GenericData] model | [DecimalPrecisionAndScaleTest] (static block)

[BigDecimal]: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/math/BigDecimal.html
[GenericData]: https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericData.html
[DecimalPrecisionAndScaleTest]: core/src/test/java/com/skraba/avro/enchiridion/core/logical/DecimalPrecisionAndScaleTest

Modules
-------

| module  | description
| ------------- | -------------
| [avro-resources](avro-resources/readme.md)  | Reusable resources for testing Avro (Schema JSON strings, etc).
| [core](core/readme.md)  | Unit tests and examples for the Avro Java SDK.
| [core17x](core17x/readme.md)  | Helper project for running tests in Avro 1.7.x 
| [core18x](core18x/readme.md)  | Helper project for running tests in Avro 1.8.x
| [core19x](core19x/readme.md)  | Helper project for running tests in Avro 1.9.x
| [core-master-snapshot](core-master-snapshot/readme.md)  | Helper project for running tests in the latest SNAPSHOT releases from the Avro master branch.


Building an Apache Avro SNAPSHOT locally.
-------------------------------------

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


