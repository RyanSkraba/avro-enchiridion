package com.skraba.avro.enchiridion.plugin;

import static com.skraba.avro.enchiridion.plugin.SimpleRecordTest.fromBytes;
import static com.skraba.avro.enchiridion.plugin.SimpleRecordTest.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.idl.DecimalAll;
import com.skraba.avro.enchiridion.idl.LogicalTypesProtocol;
import com.skraba.avro.enchiridion.idl.TimestampAll;
import com.skraba.avro.enchiridion.idl.TimestampMicrosOptional;
import com.skraba.avro.enchiridion.idl.TimestampMicrosRequired;
import com.skraba.avro.enchiridion.idl.TimestampMillisOptional;
import com.skraba.avro.enchiridion.idl.TimestampMillisRequired;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.ipc.LocalTransceiver;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.junit.jupiter.api.Test;

/**
 * Unit tests related to generated plugin generated logical types.
 *
 * <ul>
 *   <li>https://issues.apache.org/jira/browse/AVRO-2471
 *   <li>https://issues.apache.org/jira/browse/AVRO-2872
 *   <li>https://issues.apache.org/jira/browse/AVRO-3102
 * </ul>
 */
public class LogicalTypesProtocolTest {

  public static final Instant TS = Instant.EPOCH;

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
  public void testTimestampMillisAll() throws IOException {
    // only with zeros and null
    TimestampAll f = TimestampAll.newBuilder().build();
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(7));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));

    // With actual values
    f =
        TimestampAll.newBuilder()
            .setTimeMs(TS)
            .setTimeMsOpt(TS)
            .setTimeUs(TS)
            .setTimeUsOpt(TS)
            .build();
    serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(9));
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  @Test
  public void testDecimal() throws IOException {
    // only with zeros and null
    DecimalAll f =
        DecimalAll.newBuilder()
            .setBytes52(new BigDecimal("1.23"))
            .setFixed52(new BigDecimal("1.23"))
            .build();
    byte[] serialized = toBytes(f.getSpecificData(), f.getSchema(), f);
    assertThat(serialized.length, equalTo(10));
    // Round-trip should reconstitute an equal instance.
    assertThat(fromBytes(f.getSpecificData(), f.getSchema(), serialized), is(f));
  }

  @Test
  public void testRequestResponse() throws IOException {
    // Create the server side that knows how to respond to messages
    Responder responder =
        new SpecificResponder(LogicalTypesProtocol.class, (LogicalTypesProtocol) in -> in);

    // Create the client side to make requests via the local transceiver.
    LocalTransceiver transceiver = new LocalTransceiver(responder);
    LogicalTypesProtocol requestor =
        SpecificRequestor.getClient(LogicalTypesProtocol.class, transceiver);

    // These IPC requests succeed.
    assertThat(requestor.echo(TimestampAll.newBuilder().build()), notNullValue());
    assertThat(requestor.echo(TimestampAll.newBuilder().setTimeUs(TS).build()), notNullValue());

    // But this one fails.
    assertThrows(
        AvroRuntimeException.class,
        () -> requestor.echo(TimestampAll.newBuilder().setTimeUsOpt(TS).build()));
  }
}
