package com.skraba.avro.enchiridion.core.reflect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.junit.jupiter.api.Test;

/** Unit tests for working with the {@link ReflectData} class. */
public class ReflectDataTest {

  public static class SimpleRecord {
    public long id = 0;
    public String name;
  }

  public static class InstantRecord {
    public Instant moment;
  }

  public static class NestedStaticRecord {
    public long id = 0;

    public static class Inner {
      public long id = 0;
    }
  }

  @Test
  public void testSimpleRecord() {
    Schema reflected = ReflectData.get().getSchema(SimpleRecord.class);

    assertThat(
        reflected.getFullName(),
        is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest.SimpleRecord"));
    assertThat(reflected.getDoc(), nullValue());
    assertThat(reflected.isError(), is(false));
    assertThat(reflected.getFields(), hasSize(2));
    assertThat(reflected.getFields().get(0).name(), is("id"));
    assertThat(reflected.getFields().get(0).schema(), is(Schema.create(Schema.Type.LONG)));
    assertThat(reflected.getFields().get(1).name(), is("name"));
    assertThat(reflected.getFields().get(1).schema(), is(Schema.create(Schema.Type.STRING)));
  }

  @Test
  public void testInstantRecord() {
    Schema reflected = ReflectData.get().getSchema(InstantRecord.class);

    assertThat(
        reflected.getFullName(),
        is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest.InstantRecord"));
    assertThat(reflected.getDoc(), nullValue());
    assertThat(reflected.isError(), is(false));
    assertThat(reflected.getFields(), hasSize(1));
    assertThat(reflected.getFields().get(0).name(), is("moment"));
    assertThat(reflected.getFields().get(0).schema().getType(), is(Schema.Type.RECORD));
  }
}
