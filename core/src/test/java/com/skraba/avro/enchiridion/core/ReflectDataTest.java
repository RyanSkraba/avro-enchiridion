package com.skraba.avro.enchiridion.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.junit.EnabledForAvroVersion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.io.*;
import org.apache.avro.reflect.AvroSchema;
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

  @AvroSchema(
      "{\"type\":\"record\",\"name\":\"IssueImpl\",\"namespace\":\"com.skraba.avro.enchiridion.core.ReflectDataTest\","
          + "\"fields\":["
          + "{\"name\":\"number\",\"type\":\"int\"},"
          + "{\"name\":\"open\",\"type\":\"boolean\"},"
          + "{\"name\":\"project\",\"type\":\"string\"}]}")
  public interface Issue {
    boolean isOpen();

    int getNumber();

    String getProject();
  }

  public static class IssueImpl implements Issue {
    private final boolean open;
    private final int number;
    private final String project;

    public IssueImpl() {
      this.open = false;
      this.number = 0;
      this.project = "";
    }

    public IssueImpl(boolean open, int number, String project) {
      this.open = open;
      this.number = number;
      this.project = project;
    }

    public boolean isOpen() {
      return this.open;
    }

    public int getNumber() {
      return this.number;
    }

    public String getProject() {
      return this.project;
    }
  }

  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_10,
      reason = "Malformed data exception before 1.10")
  public void testInterface() {
    Schema reflected = ReflectData.get().getSchema(Issue.class);

    // This is how the AvroSchema annotation was constructed, but this assert might not be literally
    // true, depending on the order the fields were discovered during reflection.
    // assertThat(reflected, is(ReflectData.get().getSchema(IssueImpl.class)));

    if (AvroVersion.avro_1_9.orAfter("Removed invalid $ from reflected names"))
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.ReflectDataTest.IssueImpl"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.ReflectDataTest$.IssueImpl"));
    assertThat(reflected.getFields(), hasSize(3));

    Issue issue = new IssueImpl(true, 123, "AVRO");

    byte[] serialized = toBytes(issue);
    assertThat(serialized.length, is(8));

    Issue roundTrip = fromBytes(reflected, serialized);
    assertThat(roundTrip.isOpen(), is(true));
    assertThat(roundTrip.getProject(), is("AVRO"));
    assertThat(roundTrip.getNumber(), is(123));
  }

  @Test
  public void testSimpleRecord() {
    Schema reflected = ReflectData.get().getSchema(SimpleRecord.class);

    if (AvroVersion.avro_1_9.orAfter("Removed invalid $ from reflected names"))
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.ReflectDataTest.SimpleRecord"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.ReflectDataTest$.SimpleRecord"));
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
          is("com.skraba.avro.enchiridion.core.ReflectDataTest.InstantRecord"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.ReflectDataTest$.InstantRecord"));
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
          is("com.skraba.avro.enchiridion.core.ReflectDataTest.EnumRecord"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.ReflectDataTest$.EnumRecord"));
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

    // This is currently a bug AVRO-1851
    if (AvroVersion.avro_infinity.orAfter("Bug in parsing enums in a union")) {
      serialized = toBytes(r, EnumRecord.class, ReflectData.AllowNull.get());
      assertThat(serialized.length, is(1));
      assertThat(serialized[0], is((byte) 0x04));
    } else {
      AvroRuntimeException ex =
          assertThrows(
              AvroRuntimeException.class,
              () -> toBytes(r, EnumRecord.class, ReflectData.AllowNull.get()));
      assertThat(ex.getMessage(), containsString("Empty name"));
      if (AvroVersion.avro_1_9.orAfter("Exception subclass changed"))
        assertThat(ex, instanceOf(SchemaParseException.class));

      ex = assertThrows(AvroRuntimeException.class, () -> toBytes(NumbersEnum.ONE));
      assertThat(ex.getMessage(), containsString("Empty name"));
    }
  }

  @Test
  public void testNumbersEnum() {
    Schema reflected = ReflectData.get().getSchema(NumbersEnum.class);

    if (AvroVersion.avro_1_9.orAfter("Removed invalid $ from reflected names"))
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.ReflectDataTest.NumbersEnum"));
    else
      assertThat(
          reflected.getFullName(),
          is("com.skraba.avro.enchiridion.core.ReflectDataTest$.NumbersEnum"));
    assertThat(reflected.getDoc(), nullValue());
    assertThat(reflected.getEnumSymbols(), hasSize(4));
    assertThat(reflected.getEnumSymbols(), contains("ZERO", "ONE", "TWO", "THREE"));
  }
}
