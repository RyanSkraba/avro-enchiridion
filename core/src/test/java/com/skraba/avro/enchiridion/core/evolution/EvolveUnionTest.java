package com.skraba.avro.enchiridion.core.evolution;

import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.toBytes;
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
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
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
  public void testV1ToV2SchemaCompatibility() {
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
  public void testV1ToV2ConvertAFieldFromPrimitiveToUnion() {
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
    assertThat(recordV2.get("name").toString(), is("one"));
  }

  @Test
  public void testV2ToV3SchemaCompatibility() {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(SIMPLE_V3, SIMPLE_V2);
    assertThat(
        compatibility.getType(), is(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE));
    assertThat(compatibility.getResult().getIncompatibilities(), hasSize(1));
    assertThat(
        compatibility.getResult().getIncompatibilities().get(0).getType(),
        is(SchemaCompatibility.SchemaIncompatibilityType.TYPE_MISMATCH));
  }

  @Test
  public void testV2ToV3ConvertAFieldFromUnionToPrimitive() {
    // Check that schema resolution is OK by reading with the new schema.
    byte[] binaryV2 =
        toBytes(
            SIMPLE_V2,
            new GenericRecordBuilder(SIMPLE_V2).set("id", 2L).set("name", "two").build());
    GenericRecord recordV3 = new GenericData.Record(SIMPLE_V2);
    try (ByteArrayInputStream bais = new ByteArrayInputStream(binaryV2)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      GenericDatumReader<GenericRecord> r =
          new GenericDatumReader<>(SIMPLE_V2, SIMPLE_V3, GenericData.get());
      recordV3 = r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }

    // Ensure that the new field is read with the defaults.
    assertThat(recordV3.getSchema(), is(SIMPLE_V3));
    assertThat(recordV3.getSchema().getFields(), hasSize(2));
    assertThat(recordV3.get("id"), is(2L));
    assertThat(recordV3.get("name").toString(), is("two"));
  }

  @Test
  public void testV3ToV4SchemaCompatibility() {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(SIMPLE_V4, SIMPLE_V3);
    assertThat(
        compatibility.getType(), is(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE));
    assertThat(compatibility.getResult().getIncompatibilities(), hasSize(1));
    assertThat(
        compatibility.getResult().getIncompatibilities().get(0).getType(),
        is(SchemaCompatibility.SchemaIncompatibilityType.MISSING_UNION_BRANCH));
  }

  @Test
  public void testV3ToV4ConvertAFieldFromUnionToNarrowerUnion() {
    // Check that schema resolution is OK by reading with the new schema.
    byte[] binaryV3 =
        toBytes(
            SIMPLE_V3,
            new GenericRecordBuilder(SIMPLE_V3).set("id", 3L).set("name", "three").build());
    GenericRecord recordV3 = new GenericData.Record(SIMPLE_V2);
    try (ByteArrayInputStream bais = new ByteArrayInputStream(binaryV3)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      GenericDatumReader<GenericRecord> r =
          new GenericDatumReader<>(SIMPLE_V3, SIMPLE_V4, GenericData.get());
      recordV3 = r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }

    // Ensure that the new field is read with the defaults.
    assertThat(recordV3.getSchema(), is(SIMPLE_V4));
    assertThat(recordV3.getSchema().getFields(), hasSize(2));
    assertThat(recordV3.get("id"), is(3L));
    assertThat(recordV3.get("name").toString(), is("three"));
  }
}
