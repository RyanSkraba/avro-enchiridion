package com.skraba.avro.enchiridion.core.reflect;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.skraba.avro.enchiridion.core.AvroVersion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.jupiter.api.Test;

/** Unit tests for working with the {@link ReflectData} class. */
public class ReflectDataTest {

  public static <T> byte[] toBytes(T datum) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<T> w = new ReflectDatumWriter<>(ReflectData.get().getSchema(datum.getClass()));
      w.write(datum, encoder);
      encoder.flush();
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> byte[] toBytes(T datum, Class<T> c, ReflectData model) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<T> w = new ReflectDatumWriter<>(c, model);
      w.write(datum, encoder);
      encoder.flush();
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

  public static <T> T fromBytes(Schema schema, byte[] serialized) {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized)) {
      Decoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
      DatumReader<T> r = new ReflectDatumReader<>(schema, schema);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

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

  public enum NumbersEnum {
    ZERO {
      @Override
      public boolean isZero() {
        return true;
      }

      @Override
      public boolean isEven() {
        return true;
      }
    },

    ONE {},
    TWO {
      @Override
      public boolean isEven() {
        return true;
      }
    },
    THREE {};

    public boolean isZero() {
      return false;
    }

    public boolean isEven() {
      return false;
    }
  }

  public static class EnumRecord {
    public NumbersEnum count;
  }

  @Test
  public void testSimpleRecord() {
    Schema reflected = ReflectData.get().getSchema(SimpleRecord.class);

    if (AvroVersion.avro_1_9.orAfter("Removed invalid $ from reflected names"))
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest.SimpleRecord"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest$.SimpleRecord"));
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

    if (AvroVersion.avro_1_9.orAfter("Removed invalid $ from reflected names"))
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest.InstantRecord"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest$.InstantRecord"));
    assertThat(reflected.getDoc(), nullValue());
    assertThat(reflected.isError(), is(false));
    assertThat(reflected.getFields(), hasSize(1));
    assertThat(reflected.getFields().get(0).name(), is("moment"));
    assertThat(reflected.getFields().get(0).schema().getType(), is(Schema.Type.RECORD));
  }

  @Test
  public void testEnumRecord() {
    Schema reflected = ReflectData.get().getSchema(EnumRecord.class);

    if (AvroVersion.avro_1_9.orAfter("Removed invalid $ from reflected names"))
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest.EnumRecord"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest$.EnumRecord"));
    assertThat(reflected.getDoc(), nullValue());
    assertThat(reflected.isError(), is(false));
    assertThat(reflected.getFields(), hasSize(1));
    assertThat(reflected.getFields().get(0).name(), is("count"));
    assertThat(reflected.getFields().get(0).schema().getType(), is(Schema.Type.ENUM));

    EnumRecord r = new EnumRecord();
    r.count = NumbersEnum.TWO;

    byte[] serialized = toBytes(r);
    assertThat(serialized.length, is(1));
    assertThat(serialized[0], is((byte) 0x04));

    EnumRecord datum = fromBytes(reflected, serialized);
    assertThat(datum.count, is(NumbersEnum.TWO));

    serialized = toBytes(r, EnumRecord.class, ReflectData.get());
    assertThat(serialized.length, is(1));
    assertThat(serialized[0], is((byte) 0x04));
  }

  @Test
  public void testNumbersEnum() {
    Schema reflected = ReflectData.get().getSchema(NumbersEnum.class);

    if (AvroVersion.avro_1_9.orAfter("Removed invalid $ from reflected names"))
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest.NumbersEnum"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.reflect.ReflectDataTest$.NumbersEnum"));
    assertThat(reflected.getDoc(), nullValue());
    assertThat(reflected.getEnumSymbols(), hasSize(4));
    assertThat(reflected.getEnumSymbols(), contains("ZERO", "ONE", "TWO", "THREE"));
  }
}
