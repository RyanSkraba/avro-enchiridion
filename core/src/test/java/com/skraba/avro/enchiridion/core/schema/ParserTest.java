package com.skraba.avro.enchiridion.core.schema;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static com.skraba.avro.enchiridion.core.AvroUtil.qqify;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.fromBytes;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.toBytes;
import static com.skraba.avro.enchiridion.core.SerializeToJsonTest.fromJson;
import static com.skraba.avro.enchiridion.core.SerializeToJsonTest.toJson;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

public class ParserTest {

  @Test
  public void testBasicString() {
    Schema s = api().parse(qqify("'string'"));
    Schema s2 = api().parse(qqify("{'type': 'string'}"));
    assertThat(s2, is(s));

    byte[] binary = toBytes(s, "Hello world!");
    Utf8 roundTrip = fromBytes(GenericData.get(), s, binary);

    String json = toJson(GenericData.get(), s, "Hello world!");
    Utf8 roundTrip2 = fromJson(GenericData.get(), s, json);

    assertThat(roundTrip.toString(), is("Hello world!"));
    assertThat(roundTrip2.toString(), is("Hello world!"));
  }

  @Test
  public void testNotCaseInsensitive() {
    assertThrows(SchemaParseException.class, () -> api().parse(qqify("'String'")));
  }
}
