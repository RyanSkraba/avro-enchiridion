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

/** Test reading data with a schema that has evolved by renaming a field. */
public class EvolveRenameAFieldTest {

  /** The same as the original schema but with one less field. */
  private static final Schema SIMPLE_V2 =
      SchemaBuilder.record("com.skraba.avro.enchiridion.simple.SimpleRecord")
          .fields()
          .requiredLong("id")
          .name("label")
          .aliases("name") // Old names will be looked up here.
          .type()
          .stringType()
          .noDefault()
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
  public void testRenameAFieldFromARecord() {
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
    assertThat(recordV2.getSchema().getFields(), hasSize(2));
    assertThat(recordV2.get("id"), is(1L));
    assertThat(recordV2.get("label").toString(), is("one"));
  }
}
