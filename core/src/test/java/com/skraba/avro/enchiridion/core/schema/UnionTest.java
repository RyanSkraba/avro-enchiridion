package com.skraba.avro.enchiridion.core.schema;

import static com.skraba.avro.enchiridion.core.AvroUtil.qqify;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.core.AvroUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

/** Unit tests for untion types in Avro. */
public class UnionTest {

  @Test
  public void testBasicParser() {
    // A normal schema
    Schema s = AvroUtil.api().parse(qqify("['null','string']"));
    assertThat(s.toString(), is(qqify("['null','string']")));

    // You can't have duplicates
    AvroRuntimeException ex =
        assertThrows(
            AvroRuntimeException.class,
            () -> AvroUtil.api().parse(qqify("['null','string','string']")));
    assertThat(ex.getMessage(), containsString("Duplicate in union:string"));

    // And you can't nest schemas
    ex =
        assertThrows(
            AvroRuntimeException.class,
            () -> AvroUtil.api().parse(qqify("['null','string',['int']]")));

    assertThat(ex.getMessage(), containsString(qqify("Nested union: ['null','string',['int']]")));
  }

  @Test
  public void testBasicBuilder() {
    Schema is = SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
    AvroRuntimeException ex =
        assertThrows(
            AvroRuntimeException.class,
            () -> SchemaBuilder.unionOf().nullType().and().stringType().and().type(is).endUnion());

    assertThat(
        ex.getMessage(),
        containsString(qqify("Nested union: ['null','string',['null','string']]")));
  }
}