package com.skraba.avro.enchiridion.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.simple.SimpleRecord;
import com.skraba.avro.enchiridion.simple.case$;
import com.skraba.avro.enchiridion.simple.static$;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

public class SimpleRecordTest {

  public static final SimpleRecord SIMPLE = new SimpleRecord(123L, "abc");

  public static <T> byte[] toBytes(SpecificData model, Schema schema, T datum) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
      DatumWriter<T> w = new SpecificDatumWriter<T>(schema, model);
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
      DatumReader<T> r = new SpecificDatumReader<T>(schema, schema, model);
      return r.read(null, decoder);
    } catch (IOException ioe) {
      throw new RuntimeException((ioe));
    }
  }

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

  @Test
  public void testSimpleRecordConstructorCloneViaBytes() {
    SimpleRecord clone =
        fromBytes(
            SIMPLE.getSpecificData(),
            SIMPLE.getSchema(),
            toBytes(SIMPLE.getSpecificData(), SIMPLE.getSchema(), SIMPLE));

    assertThat(clone, equalTo(SIMPLE));
  }

  @Test
  public void testSimpleRecordConstructorCloneViaMessage() throws IOException {
    ByteBuffer bb = SIMPLE.toByteBuffer();
    bb.rewind();

    SimpleRecord clone = SimpleRecord.fromByteBuffer(bb);

    assertThat(clone, equalTo(SIMPLE));
  }

  @Test
  public void testReservedKeywordNames() {
    case$ inner = new case$();
    assertThat(inner, instanceOf(GenericRecord.class));
    assertThat(inner.default$, is(0L));

    static$ wrapper = new static$();
    assertThat(wrapper, instanceOf(GenericRecord.class));
    assertThat(wrapper.switch$, nullValue());

    wrapper.setSwitch$(inner);

    // This is the bug behaviour.
    assertThrows(
        ClassCastException.class,
        () -> {
          assertThat(
              fromBytes(
                  wrapper.getSpecificData(),
                  wrapper.getSchema(),
                  toBytes(wrapper.getSpecificData(), wrapper.getSchema(), wrapper)),
              equalTo(wrapper));

          byte[] bytes = toBytes(wrapper.getSpecificData(), wrapper.getSchema(), wrapper);
          assertThat(bytes.length, is(1));
          static$ cloned = fromBytes(wrapper.getSpecificData(), wrapper.getSchema(), bytes);
          assertThat(cloned, equalTo(wrapper));
        });
  }
}
