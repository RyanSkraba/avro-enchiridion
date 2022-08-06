package com.skraba.avro.enchiridion.core.evolution;

import static org.assertj.core.api.Assertions.assertThat;

import com.skraba.avro.enchiridion.testkit.AvroVersion;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;

/** Basic tests for round-trip serialization with different reader and writer schemas. */
public class EvolutionAsserts {

  /** A simple, original schema. */
  public static void assertSchemaCompatible(Schema writer, Schema reader) {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
    assertThat(compatibility.getType())
        .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
    if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
      assertThat(compatibility.getResult())
          .isEqualTo(SchemaCompatibility.SchemaCompatibilityResult.compatible());
    }
  }

  public static void assertSchemaIncompatible(Schema writer, Schema reader, String reason) {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
    assertThat(compatibility.getType())
        .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE);
    if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
      assertThat(compatibility.getResult().getIncompatibilities())
          .hasSize(1)
          .anyMatch(
              incompatibility ->
                  incompatibility.getType()
                      == SchemaCompatibility.SchemaIncompatibilityType.valueOf(reason));
    }
  }

  public static void assertSchemaIncompatible(
      Schema writer, Schema reader, String reason1, String reason2) {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
    assertThat(compatibility.getType())
        .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE);
    if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
      assertThat(compatibility.getResult().getIncompatibilities())
          .hasSize(2)
          .satisfies(
              value -> {
                assertThat(value.get(0).getType()).hasToString(reason1);
                assertThat(value.get(1).getType()).hasToString(reason2);
              })
          .anyMatch(
              incompatibility ->
                  incompatibility.getType()
                      == SchemaCompatibility.SchemaIncompatibilityType.valueOf(reason1));
    }
  }
}
