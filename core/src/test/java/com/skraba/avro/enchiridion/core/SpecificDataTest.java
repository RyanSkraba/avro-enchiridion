package com.skraba.avro.enchiridion.core;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.skraba.avro.enchiridion.resources.AvroTestResources;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

/** Unit tests for working with the {@link SpecificData} class. */
public class SpecificDataTest {

  public static <T> byte[] toBytes(SpecificData model, Schema schema, T datum) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<T> w = new SpecificDatumWriter<>(schema, model);
      w.write(datum, encoder);
      encoder.flush();
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T fromBytes(SpecificData model, Schema schema, byte[] serialized) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      DatumReader<T> r = new SpecificDatumReader<>(schema, schema, model);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T roundTripBytes(SpecificData model, Schema schema, T datum) {
    return fromBytes(model, schema, toBytes(model, schema, datum));
  }

  @Test
  public void testStringDeepCopy() throws IOException {
    // Two identical schemas, btut one annotated with avro.java.string
    Schema sString = api().parse(AvroTestResources.SimpleRecord());
    Schema sUtf8 =
        api()
            .parse(
                AvroTestResources.SimpleRecord()
                    .replaceFirst(
                        "(\"type\" : \"string\")", "$1 ,\"avro.java.string\" : \"String\""));

    // Four records for all combinations of the two schemas and the two string types
    GenericRecord rString =
        new GenericRecordBuilder(sString).set("id", 1L).set("name", "One").build();
    GenericRecord rStringWithUtf8 =
        new GenericRecordBuilder(sString).set("id", 1L).set("name", new Utf8("One")).build();
    GenericRecord rUtf8 =
        new GenericRecordBuilder(sUtf8).set("id", 1L).set("name", new Utf8("One")).build();
    GenericRecord rUtf8WithStr =
        new GenericRecordBuilder(sUtf8).set("id", 1L).set("name", "One").build();

    Schema[] ss = {sString, sUtf8};
    GenericRecord[] rs = {rString, rStringWithUtf8, rUtf8, rUtf8WithStr};

    // All the records are valid compared to either schema.
    for (Schema s : ss) {
      for (GenericRecord r : rs) {
        assertTrue(SpecificData.get().validate(s, r));
      }
    }

    // Checking records for equality
    int recordsEqual = 0;
    int recordsNotEqual = 0;
    int deepCopyEqual = 0;
    int deepCopyNotEqual = 0;
    for (GenericRecord r : rs) {

      // A round trip returns an equal instance.
      assertThat(roundTripBytes(SpecificData.get(), r.getSchema(), r), is(r));

      for (GenericRecord r2 : rs) {

        // Two records are the same if they have the same schema, even if their string
        // field has a different type.
        if (r.getSchema().equals(r2.getSchema())) {
          assertThat(r, is(r2));
          recordsEqual++;
        } else {
          assertThat(r, not(r2));
          recordsNotEqual++;
        }

        // Any record can be serialized and deserialized into the other.
        assertThat(
            fromBytes(
                SpecificData.get(), r.getSchema(), toBytes(SpecificData.get(), r2.getSchema(), r2)),
            is(r));

        for (Schema s : ss) {
          if (s.equals(r2.getSchema())) {
            deepCopyEqual++;
            assertThat(SpecificData.get().deepCopy(s, r), is(r2));
          } else {
            deepCopyNotEqual++;
            assertThat(SpecificData.get().deepCopy(s, r), not(r2));
          }
        }
      }
    }

    assertThat(recordsEqual, is(8));
    assertThat(recordsNotEqual, is(8));
    assertThat(deepCopyEqual, is(16));
    assertThat(deepCopyNotEqual, is(16));
  }
}
