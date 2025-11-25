package com.skraba.avro.enchiridion.core.evolution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.testkit.AvroVersion;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaCompatibility;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test reading data with a schema that has evolved by widening a primitive.
 *
 * <p>The following promotions are permitted:
 *
 * <ol>
 *   <li><b>int</b> is promotable to long, float, or double
 *   <li><b>long</b> is promotable to float or double (although this can lose precision)
 *   <li><b>float</b> is promotable to double
 *   <li><b>string</b> is promotable to bytes
 *   <li><b>bytes</b> is promotable to string
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EvolveWidenPrimitivesTest {

  /** All of the schemas to check against each other. */
  protected Stream<Schema> getAllSchemasToCrossCheck() {
    return Stream.of(
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.LONG),
        Schema.create(Schema.Type.FLOAT),
        Schema.create(Schema.Type.DOUBLE),
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.BYTES),
        Schema.createFixed("fixed", null, null, 6));
  }

  /**
   * A Stream of all evolutions possible between the schemas in ALL, as well as whether or not the
   * evolution should be permitted.
   */
  private Stream<Arguments> getAllEvolutions() {
    return getAllSchemasToCrossCheck()
        .flatMap(
            fieldType1 -> {
              Schema v1 =
                  SchemaBuilder.record("A")
                      .fields()
                      .name("a1")
                      .type(fieldType1)
                      .noDefault()
                      .endRecord();
              return getAllSchemasToCrossCheck()
                  .flatMap(
                      fieldType2 -> {
                        Schema v2 =
                            SchemaBuilder.record("A")
                                .fields()
                                .name("a1")
                                .type(fieldType2)
                                .noDefault()
                                .endRecord();
                        return Stream.of(Arguments.of(isWidening(fieldType1, fieldType2), v1, v2));
                      });
            });
  }

  /**
   * @return if a schema change is a permitted widening.
   */
  private static boolean isWidening(Schema v1, Schema v2) {
    // Although converting to itself isn't technically a widening, consider that it works.
    if (v1.getType() == v2.getType()) return true;

    // Numeric primitives can be converted.
    if (v1.getType() == Schema.Type.INT
        && (v2.getType() == Schema.Type.LONG
            || v2.getType() == Schema.Type.FLOAT
            || v2.getType() == Schema.Type.DOUBLE)) {
      return true;
    }
    if (v1.getType() == Schema.Type.LONG
        && (v2.getType() == Schema.Type.FLOAT || v2.getType() == Schema.Type.DOUBLE)) {
      return true;
    }
    if (v1.getType() == Schema.Type.FLOAT && v2.getType() == Schema.Type.DOUBLE) {
      return true;
    }

    // It looks like STRING and BYTES weren't acceptable conversions in 1.7
    if (AvroVersion.avro_1_8.orAfter()) {
      if (v1.getType() == Schema.Type.STRING && v2.getType() == Schema.Type.BYTES) {
        return true;
      }
      return v1.getType() == Schema.Type.BYTES && v2.getType() == Schema.Type.STRING;
    }

    return false;
  }

  @ParameterizedTest
  @MethodSource("getAllEvolutions")
  void testSchemaCompatibility(boolean permitted, Schema v1, Schema v2) {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(v2, v1);
    if (permitted) {
      assertThat(
          compatibility.getType(), is(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE));
      if (AvroVersion.avro_1_9.orAfter()) {
        assertThat(
            compatibility.getResult(),
            is(SchemaCompatibility.SchemaCompatibilityResult.compatible()));
      }
    } else {
      assertThat(
          compatibility.getType(), is(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE));
      if (AvroVersion.avro_1_9.orAfter()) {
        assertThat(compatibility.getResult().getIncompatibilities(), hasSize(1));
        assertThat(
            compatibility.getResult().getIncompatibilities().get(0).getType(),
            is(SchemaCompatibility.SchemaIncompatibilityType.TYPE_MISMATCH));
      }
    }
  }
}
