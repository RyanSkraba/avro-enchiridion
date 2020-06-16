package com.skraba.avro.enchiridion.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.skraba.avro.enchiridion.simple.SimpleRecord;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

public class SimpleRecordTest {

  @Test
  public void testSimpleRecord() {
    // This should be generated as a test resource.
    SimpleRecord sr = new SimpleRecord();
    assertThat(sr, instanceOf(GenericRecord.class));
    assertThat(sr.id, is(0L));
    assertThat(sr.name, nullValue());
  }

  @Test
  public void testSimpleRecordConstructor() {
    SimpleRecord sr = new SimpleRecord(123L, "abc");
    assertThat(sr.id, is(123L));
    assertThat(sr.name, is("abc"));
  }
}
