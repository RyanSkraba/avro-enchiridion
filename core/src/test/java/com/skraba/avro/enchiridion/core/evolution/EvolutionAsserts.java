package com.skraba.avro.enchiridion.core.evolution;

import com.skraba.avro.enchiridion.core.AvroVersion;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.assertj.core.api.Assertions;

/** Basic tests for round-trip serialization with different reader and writer schemas. */
public class EvolutionAsserts {

  /** A simple, original schema. */
  public static void assertSchemaCompatible(Schema writer, Schema reader) {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
    Assertions.assertThat(compatibility.getType())
        .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE);
    if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
      Assertions.assertThat(compatibility.getResult())
          .isEqualTo(SchemaCompatibility.SchemaCompatibilityResult.compatible());
    }
  }

  public static void assertSchemaIncompatible(Schema writer, Schema reader, String reason) {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
    Assertions.assertThat(compatibility.getType())
        .isEqualTo(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE);
    if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
      Assertions.assertThat(compatibility.getResult().getIncompatibilities())
          .hasSize(1)
          .anyMatch(
              incompatibility ->
                  incompatibility.getType()
                      == SchemaCompatibility.SchemaIncompatibilityType.valueOf(reason));
    }
  }
}
