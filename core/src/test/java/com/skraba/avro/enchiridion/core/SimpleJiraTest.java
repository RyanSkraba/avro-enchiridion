package com.skraba.avro.enchiridion.core;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static com.skraba.avro.enchiridion.core.AvroUtil.qqify;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.junit.EnabledForAvroVersion;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

public class SimpleJiraTest {

  /** @see <a href="https://issues.apache.org/jira/browse/AVRO-1799">AVRO-1799</a> */
  @Test
  public void testAvro1799BytesTypeNotRewoundOnToString() throws IOException {
    // Create an input record with a required Schema.Type.BYTES field.
    Schema s = SchemaBuilder.record("A").fields().requiredBytes("a1").endRecord();
    IndexedRecord r = new GenericData.Record(s);
    r.put(0, ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x03}));

    // This is the code that shows the bug.
    String logMsg = "Processing:" + r;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GenericDatumWriter<IndexedRecord> writer = new GenericDatumWriter<>(s);
    writer.write(r, EncoderFactory.get().directBinaryEncoder(baos, null));
    baos.close();

    // Check that the record was logged AND fully serialized.
    assertThat(baos.toByteArray().length, is(4)); // length + 3 bytes data.
    if (AvroVersion.avro_1_9.orAfter()) {
      // This was actually fixed in 1.8.1
      assertThat(logMsg, is("Processing:{\"a1\": \"\\u0001\\u0002\\u0003\"}"));
    } else if (AvroVersion.avro_1_8.orAfter()) {
      assertThat(logMsg, is("Processing:{\"a1\": {\"bytes\": \"\\u0001\\u0002\\u0003\"}}"));
    } else {
      assertThat(logMsg, is("Processing:{\"a1\": {\"bytes\": \"\u0001\u0002\u0003\"}}"));
    }
  }

  /** @see <a href="https://issues.apache.org/jira/browse/AVRO-2837">AVRO-2837</a> */
  @Test
  public void testAvro1965NestedRecordsAndNamespaces() {

    // Build records nested three levels deep, with one int field.  Only the top level has a
    // namespace.
    Schema d = SchemaBuilder.builder().intType();
    Schema c = SchemaBuilder.record("c").fields().name("d").type(d).noDefault().endRecord();
    Schema b = SchemaBuilder.record("b").fields().name("c").type(c).noDefault().endRecord();
    Schema a = SchemaBuilder.record("default.a").fields().name("b").type(b).noDefault().endRecord();

    // The names on the original Schema are as expected: default.a, b, c
    assertThat(a.getFullName(), is("default.a"));
    assertThat(a.getField("b").schema().getFullName(), is("b"));
    assertThat(a.getField("b").schema().getField("c").schema().getFullName(), is("c"));

    // We should be able to reconstruct the schema from the JSON string.
    String aJsonString = a.toString();
    Schema aCopy = new Schema.Parser().parse(aJsonString);

    if (AvroVersion.avro_1_10.orAfter()) {
      // The copy should be identical with the same names.
      assertThat(aCopy, is(a));
    } else {
      // Before 1.10, the copy was not identical, with the names default.a, b, default.c
      assertThat(aCopy, not(a));
      assertThat(aCopy.getFullName(), is("default.a"));
      assertThat(aCopy.getField("b").schema().getFullName(), is("b"));
      assertThat(
          aCopy.getField("b").schema().getField("c").schema().getFullName(), is("default.c"));
    }
  }

  /** @see <a href="https://issues.apache.org/jira/browse/AVRO-2830">AVRO-2830</a> */
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_8,
      reason = "Logical types introduced in Avro 1.8")
  @Test
  public void testAvro2380UnionWithLogicalType() {

    // You can't have a primitive and a logical type on the same primitive in one union because
    // it's considered a duplicate.
    AvroRuntimeException ex =
        assertThrows(
            AvroRuntimeException.class,
            () -> {
              SchemaBuilder.record("Avro2380")
                  .fields()
                  .name("data")
                  .type()
                  .map()
                  .values()
                  .unionOf()
                  .nullType()
                  .and()
                  .longType()
                  .and()
                  .type(LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType()))
                  .endUnion()
                  .noDefault()
                  .endRecord();
            });
    assertThat(ex.getMessage(), is("Duplicate in union:long"));

    // The solution is to name a field by including it in a single column record.

    Schema tsMicros =
        SchemaBuilder.record("MyTimestamp")
            .fields()
            .name("ts")
            .type(LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType()))
            .noDefault()
            .endRecord();
    Schema artificiallyNameAField =
        SchemaBuilder.record("Avro2380")
            .fields()
            .name("data")
            .type()
            .map()
            .values()
            .unionOf()
            .nullType()
            .and()
            .longType()
            .and()
            .type(tsMicros)
            .endUnion()
            .noDefault()
            .endRecord();

    // Create a record with only long and null.
    Map<String, Object> data = new HashMap<>();
    data.put("one", 1L);
    data.put("two", null);
    data.put("three", 3L);
    GenericRecord r1 = new GenericData.Record(artificiallyNameAField);
    r1.put("data", data);

    byte[] serialized1 = toBytes(GenericData.get(), artificiallyNameAField, r1);

    // Overwrite one of the longs with a timestamp record.
    GenericRecord ts = new GenericData.Record(tsMicros);
    ts.put("ts", 1L);
    data.put("one", ts);

    byte[] serialized2 = toBytes(GenericData.get(), artificiallyNameAField, r1);

    // The two byte arrays should be exactly the same length and only have one difference (the
    // union type.
    assertThat(serialized1.length, is(serialized2.length));
    int count = 0;
    for (int i = 0; i < serialized1.length; i++) count += serialized1[i] != serialized2[i] ? 1 : 0;
    assertThat(count, is(1));
  }

  /** @see <a href="https://issues.apache.org/jira/browse/AVRO-2943">AVRO-2943</a> */
  @Test
  public void testAvro2943MapEquality() {
    Schema schema =
        SchemaBuilder.record("A")
            .fields()
            .name("a1")
            .type()
            .map()
            .values()
            .stringType()
            .noDefault()
            .endRecord();

    GenericRecord record1 = new GenericData.Record(schema);
    record1.put(0, new HashMap<CharSequence, String>());
    ((Map<CharSequence, String>) record1.get(0)).put("id", "one");
    GenericRecord record2 = new GenericData.Record(schema);
    record2.put(0, new HashMap<CharSequence, String>());
    ((Map<CharSequence, String>) record2.get(0)).put(new Utf8("id"), "one");

    AvroRuntimeException ex =
        assertThrows(
            AvroRuntimeException.class,
            () -> assertThat(GenericData.get().compare(record1, record2, schema), is(0)));
    assertThat(ex.getMessage(), is("Can't compare maps!"));
    assertThat(record1, not(record2));
    assertThat(record2, not(record1));
  }

  @Test
  public void testAvro3129NullPointer() {
    assertThrows(
        NullPointerException.class,
        () -> api().parse(qqify("{'type': 'enum', 'name': 'Suit', 'symbols' : [null] }")));
  }
}
