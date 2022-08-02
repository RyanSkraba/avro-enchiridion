package com.skraba.avro.enchiridion.core.evolution;

import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.fromBytes;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.toBytes;
import static com.skraba.avro.enchiridion.core.evolution.BasicTest.*;
import static com.skraba.avro.enchiridion.core.evolution.EvolutionAsserts.assertSchemaCompatible;
import static com.skraba.avro.enchiridion.core.evolution.EvolutionAsserts.assertSchemaIncompatible;
import static org.assertj.core.api.Assertions.assertThat;

import com.skraba.avro.enchiridion.core.AvroVersion;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

/**
 * Test reading data with a schema that has evolved by changing to/from a union.
 *
 * <p>The schema goes through several iterations:
 *
 * <ol>
 *   <li><b>V1 (both fields required)</b> { id: LONG, name: STRING }
 *   <li><b>V2 (both fields nullable)</b> { id: NULL|LONG, name: NULL|STRING }
 *   <li><b>V3 (required id again)</b> { id: LONG, name: NULL|STRING|BYTES }
 *   <li><b>V4 (narrow union for name)</b> { id: LONG, name: STRING|BYTES }
 * </ol>
 */
public class EvolveUnionTest {

  /** The same as the original schema but with both fields changed to nullable. */
  private static final Schema SIMPLE_V2 =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .optionalLong("id")
          .optionalString("name")
          .endRecord();

  /** The third version makes id required again, and name can be null, string or bytes. */
  private static final Schema SIMPLE_V3 =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .name("name")
          .type()
          .unionOf()
          .nullType()
          .and()
          .stringType()
          .and()
          .bytesType()
          .endUnion()
          .noDefault()
          .endRecord();

  /** The fourth version drops null as a possibility for name. */
  private static final Schema SIMPLE_V4 =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .name("name")
          .type()
          .unionOf()
          .stringType()
          .and()
          .bytesType()
          .endUnion()
          .noDefault()
          .endRecord();

  @Test
  public void testCompatibility() {
    assertSchemaCompatible(SIMPLE_V1, SIMPLE_V2);
    assertSchemaIncompatible(SIMPLE_V2, SIMPLE_V3, "TYPE_MISMATCH");
    assertSchemaIncompatible(SIMPLE_V3, SIMPLE_V4, "MISSING_UNION_BRANCH");

    assertSchemaCompatible(SIMPLE_V4, SIMPLE_V3);
    if (AvroVersion.avro_1_8.orAfter("Bug with 1.7.x")) {
      assertSchemaCompatible(SIMPLE_V3, SIMPLE_V2);
    } else {
      assertSchemaIncompatible(SIMPLE_V3, SIMPLE_V2, "???");
    }
    // assertSchemaIncompatible(SIMPLE_V2, SIMPLE_V1, "TYPE_MISMATCH"); but two type mismatches
  }

  @Test
  public void testV1ToV2ConvertAFieldFromPrimitiveToUnion() {
    // Check that schema resolution is OK by reading with the new schema.
    GenericRecord recordV2 = fromBytes(SIMPLE_V1, SIMPLE_V2, BINARY_V1);

    // Ensure that the new field is read with the defaults.
    assertThat(recordV2.getSchema()).isEqualTo(SIMPLE_V2);
    assertThat(recordV2.getSchema().getFields()).hasSize(2);
    assertThat(recordV2.get("id")).isEqualTo(1L);
    assertThat(recordV2.get("name")).hasToString("one");
  }

  @Test
  public void testV2ToV3ConvertAFieldFromUnionToPrimitive() {
    // Check that schema resolution is OK by reading with the new schema.
    byte[] binaryV2 =
        toBytes(
            SIMPLE_V2,
            new GenericRecordBuilder(SIMPLE_V2).set("id", 2L).set("name", "two").build());
    GenericRecord recordV3 = fromBytes(SIMPLE_V2, SIMPLE_V3, binaryV2);

    // Ensure that the new field is read with the defaults.
    assertThat(recordV3.getSchema()).isEqualTo(SIMPLE_V3);
    assertThat(recordV3.getSchema().getFields()).hasSize(2);
    assertThat(recordV3.get("id")).isEqualTo(2L);
    assertThat(recordV3.get("name")).hasToString("two");
  }

  @Test
  public void testV3ToV4ConvertAFieldFromUnionToNarrowerUnion() {
    // Check that schema resolution is OK by reading with the new schema.
    byte[] binaryV3 =
        toBytes(
            SIMPLE_V3,
            new GenericRecordBuilder(SIMPLE_V3).set("id", 3L).set("name", "three").build());

    GenericRecord recordV4 = fromBytes(SIMPLE_V3, SIMPLE_V4, binaryV3);

    // Ensure that the new field is read with the defaults.
    assertThat(recordV4.getSchema()).isEqualTo(SIMPLE_V4);
    assertThat(recordV4.getSchema().getFields()).hasSize(2);
    assertThat(recordV4.get("id")).isEqualTo(3L);
    assertThat(recordV4.get("name")).hasToString("three");
  }
}
