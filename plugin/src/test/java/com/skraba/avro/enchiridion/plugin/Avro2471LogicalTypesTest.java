package com.skraba.avro.enchiridion.plugin;

import static com.skraba.avro.enchiridion.plugin.SimpleRecordTest.fromBytes;
import static com.skraba.avro.enchiridion.plugin.SimpleRecordTest.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.idl.TimestampMicrosOptional;
import com.skraba.avro.enchiridion.idl.TimestampMicrosRequired;
import com.skraba.avro.enchiridion.idl.TimestampMillisOptional;
import com.skraba.avro.enchiridion.idl.TimestampMillisRequired;
import java.io.IOException;
import java.time.Instant;
import org.apache.avro.data.TimeConversions;
import org.junit.jupiter.api.Test;

/** Unit tests related to AVRO-2471 and logical types. */
public class Avro2471LogicalTypesTest {
  public static final Instant TS = Instant.EPOCH;

  @Test
  public void testTimestampMillisRequired() throws IOException {
    TimestampMillisRequired f = TimestampMillisRequired.newBuilder().setTs(TS).build();
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(4));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  @Test
  public void testTimestampMillisOptional() throws IOException {
    TimestampMillisOptional f = TimestampMillisOptional.newBuilder().setTs(TS).build();
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(5));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  @Test
  public void testTimestampMicrosRequired() throws IOException {
    TimestampMicrosRequired f = TimestampMicrosRequired.newBuilder().setTs(TS).build();
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(4));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  @Test
  public void testTimestampMicrosOptional() throws IOException {
    TimestampMicrosOptional f = TimestampMicrosOptional.newBuilder().setTs(TS).build();
    f.getSpecificData().addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(5));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }
}
