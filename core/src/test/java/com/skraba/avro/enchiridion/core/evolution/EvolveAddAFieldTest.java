package com.skraba.avro.enchiridion.core.evolution;

import com.skraba.avro.enchiridion.core.AvroVersion;
import com.skraba.avro.enchiridion.core.SerializeToBytesTest;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/** Test reading data with a schema that has evolved by adding a field. */
public class EvolveAddAFieldTest {

  /** A simple, original schema. */
  public static final Schema SIMPLE_V1 =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .requiredString("name")
          .endRecord();

  /** An example record for {@link #SIMPLE_V1}. */
  public static final GenericRecord RECORD_V1 =
      new GenericRecordBuilder(SIMPLE_V1).set("id", 1L).set("name", "one").build();

  /** {@link #RECORD_V1} represented as binary. */
  public static final byte[] BINARY_V1 =
      SerializeToBytesTest.toBytes(GenericData.get(), SIMPLE_V1, RECORD_V1);

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
    if (AvroVersion.avro_1_9.orAfter()) {
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
    // TODO: Why doesn't this work in Avro 1.8?
    if (AvroVersion.avro_1_9.orAfter()) {
      // Check schema compatibility
      SchemaCompatibility.SchemaPairCompatibility check =
          SchemaCompatibility.checkReaderWriterCompatibility(SIMPLE_V2_MISSING_DEFAULT, SIMPLE_V1);
      assertThat(
          check.getResult().getCompatibility(),
          is(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE));
      // It lists one incompatibility (the missing default).
      assertThat(check.getResult().getIncompatibilities(), hasSize(1));
      assertThat(
          check.getResult().getIncompatibilities().get(0).getType(),
          is(SchemaCompatibility.SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE));
    }
  }
}
