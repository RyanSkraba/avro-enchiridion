package com.skraba.avro.enchiridion.core.evolution;

import static com.skraba.avro.enchiridion.core.evolution.BasicTest.BINARY_V1;
import static com.skraba.avro.enchiridion.core.evolution.BasicTest.SIMPLE_V1;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.core.AvroVersion;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

/** Test reading data with a schema that has evolved by adding a field. */
public class EvolveAddAFieldTest {

  /** The same as the original schema but with a new field. */
  private static final Schema SIMPLE_V2 =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .requiredString("name")
          .name("rating")
          .type()
          .floatType()
          .floatDefault(2.5f) // New fields must have a default.
          .endRecord();

  /** The same as the original schema but with a new field. */
  private static final Schema SIMPLE_V2_MISSING_DEFAULT =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .requiredString("name")
          .name("rating")
          .type()
          .floatType()
          .noDefault() // It's an error to be missing the default.
          .endRecord();

  @Test
  public void testSchemaCompatibility() {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(SIMPLE_V2, SIMPLE_V1);
    assertThat(compatibility.getType(), is(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE));
    if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
      assertThat(
          compatibility.getResult(),
          is(SchemaCompatibility.SchemaCompatibilityResult.compatible()));
    }
  }

  @Test
  public void testAddAFieldToARecord() {

    // Check that schema resolution is OK by reading with the new schema.
    GenericRecord recordV2;
    try (ByteArrayInputStream bais = new ByteArrayInputStream(BINARY_V1)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      GenericDatumReader<GenericRecord> r =
          new GenericDatumReader<>(SIMPLE_V1, SIMPLE_V2, GenericData.get());
      recordV2 = r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }

    // Ensure that the new field is read with the defaults.
    assertThat(recordV2.getSchema(), is(SIMPLE_V2));
    assertThat(recordV2.getSchema().getFields(), hasSize(3));
    assertThat(recordV2.get("id"), is(1L));
    assertThat(recordV2.get("name").toString(), is("one"));
    assertThat(recordV2.get("rating"), is(2.5f));
  }

  @Test
  public void testAddAFieldToARecordWithoutADefault() {
    // SchemaCompatibility.getResult exists in Avro 1.9
    SchemaCompatibility.SchemaPairCompatibility check =
        SchemaCompatibility.checkReaderWriterCompatibility(SIMPLE_V2_MISSING_DEFAULT, SIMPLE_V1);
    if (AvroVersion.avro_1_9.orAfter("getResult appears in 1.9.x")) {
      assertThat(
          check.getResult().getCompatibility(),
          is(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE));
      // It lists one incompatibility (the missing default).
      assertThat(check.getResult().getIncompatibilities(), hasSize(1));
      assertThat(
          check.getResult().getIncompatibilities().get(0).getType(),
          is(SchemaCompatibility.SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE));
    } else {
      assertThat(check.getType(), is(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE));
    }
  }

  @Test
  public void testAddANullableFieldToARecordWithoutADefault() {
    Schema v1 = SchemaBuilder.record("A").fields().requiredString("a1").endRecord();
    Schema v2 =
        SchemaBuilder.record("A")
            .fields()
            .requiredString("a1")
            .optionalString("a2")
            .name("a3")
            .type()
            .unionOf()
            .nullType()
            .and()
            .stringType()
            .endUnion()
            .noDefault()
            .endRecord();

    SchemaCompatibility.SchemaPairCompatibility check =
        SchemaCompatibility.checkReaderWriterCompatibility(v2, v1);
    assertThat(check.getType(), is(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE));
  }
}
