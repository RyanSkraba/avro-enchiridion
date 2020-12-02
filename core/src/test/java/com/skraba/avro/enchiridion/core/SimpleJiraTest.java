package com.skraba.avro.enchiridion.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

public class SimpleJiraTest {

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
}
