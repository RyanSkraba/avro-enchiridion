package com.skraba.avro.enchiridion.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
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
}
