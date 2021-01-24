package com.skraba.avro.enchiridion.core.logical;

import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.roundTripBytes;
import static com.skraba.avro.enchiridion.resources.AvroLogicalTypes$.MODULE$;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.core.AvroVersion;
import com.skraba.avro.enchiridion.junit.EnabledForAvroVersion;
import com.skraba.avro.enchiridion.resources.AvroLogicalTypes$;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

/** Unit tests for the Avro dates and times type. */
public class DateAndTimeTests {

  private static final Schema DATE_TIME_SCHEMA =
      AvroUtil.api().parse(MODULE$.DateLogicalTypeRecord());

  private final IndexedRecord BTTF =
      createRecordDateTimeTypes(
          "BackToTheFuture",
          ZonedDateTime.of(1985, 2, 14, 12, 34, 56, 123456789, ZoneOffset.ofHours(1)).toInstant());

  private final IndexedRecord EPOCH =
      createRecordDateTimeTypes(
          "epoch", ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());

  /**
   * @return A record with the {@link AvroLogicalTypes$#MODULE$#dateLogicalTypeRecord()} schema with
   *     the specified instant.
   */
  static GenericRecord createRecordDateTimeTypes(String name, Instant i) {
    OffsetDateTime dt = OffsetDateTime.ofInstant(i, ZoneOffset.UTC);
    GenericRecord r = new GenericData.Record(DATE_TIME_SCHEMA);
    r.put(0, name);
    // r.put(1, ChronoUnit.MILLIS.between(Instant.EPOCH, dt.toInstant()));
    r.put(1, i.toEpochMilli());
    r.put(2, ChronoUnit.MICROS.between(Instant.EPOCH, i));
    r.put(3, i.toEpochMilli());
    r.put(4, ChronoUnit.MICROS.between(Instant.EPOCH, i));
    r.put(5, (int) dt.getLong(ChronoField.EPOCH_DAY));
    r.put(6, (int) (dt.getLong(ChronoField.NANO_OF_DAY) / 1000000));
    r.put(7, dt.getLong(ChronoField.NANO_OF_DAY) / 1000);
    return r;
  }

  @Test
  public void testRecordsAsPrimitives() {
    // Deserializing and reading with the GenericData and date/time logical types return primitives.
    IndexedRecord epochRoundTrip = roundTripBytes(GenericData.get(), DATE_TIME_SCHEMA, EPOCH);
    assertThat(EPOCH, is(epochRoundTrip));
    IndexedRecord bttfRoundTrip = roundTripBytes(GenericData.get(), DATE_TIME_SCHEMA, BTTF);
    assertThat(BTTF, is(bttfRoundTrip));

    assertThat(epochRoundTrip.get(0), is(new Utf8("epoch")));
    assertThat(epochRoundTrip.get(1), is(0L));
    assertThat(epochRoundTrip.get(2), is(0L));
    assertThat(epochRoundTrip.get(3), is(0L));
    assertThat(epochRoundTrip.get(4), is(0L));
    assertThat(epochRoundTrip.get(5), is(0));
    assertThat(epochRoundTrip.get(6), is(0));
    assertThat(epochRoundTrip.get(7), is(0L));

    assertThat(bttfRoundTrip.get(0), is(new Utf8("BackToTheFuture")));
    assertThat(bttfRoundTrip.get(1), is(477228896123L));
    assertThat(bttfRoundTrip.get(2), is(477228896123456L));
    assertThat(bttfRoundTrip.get(3), is(477228896123L));
    assertThat(bttfRoundTrip.get(4), is(477228896123456L));
    assertThat(bttfRoundTrip.get(5), is(5523));
    assertThat(bttfRoundTrip.get(6), is(41696123));
    assertThat(bttfRoundTrip.get(7), is(41696123456L));
  }

  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_9,
      reason = "java.time classes aren't supported until Avro 1.9.x")
  public void testRecordsWithJavaTimeConversions() {
    GenericData withConversions = AvroUtil.api().withJavaTimeConversions();

    // Deserializing and reading with the GenericData and date/time logical types return primitives.
    IndexedRecord epochRoundTrip = roundTripBytes(withConversions, DATE_TIME_SCHEMA, EPOCH);
    assertThat(epochRoundTrip.toString(), not(EPOCH.toString()));
    IndexedRecord bttfRoundTrip = roundTripBytes(withConversions, DATE_TIME_SCHEMA, BTTF);
    assertThat(bttfRoundTrip.toString(), not(BTTF.toString()));

    // TODO: java.lang.ClassCastException: java.time.Instant cannot be cast to java.lang.Long
    // Remove the .toString above!

    assertThat(epochRoundTrip.get(0), is(new Utf8("epoch")));
    assertThat(epochRoundTrip.get(1), is(Instant.ofEpochMilli(0)));
    assertThat(epochRoundTrip.get(2), is(Instant.ofEpochMilli(0)));
    if (AvroVersion.avro_1_10.orAfter()) {
      // The local-timestamp-millis and local-timestamp-micros appeared in 1.10.x
      assertThat(epochRoundTrip.get(3), is(LocalDateTime.of(1970, 1, 1, 0, 0, 0)));
      assertThat(epochRoundTrip.get(4), is(LocalDateTime.of(1970, 1, 1, 0, 0, 0)));
    } else {
      assertThat(epochRoundTrip.get(3), is(0L));
      assertThat(epochRoundTrip.get(4), is(0L));
    }
    assertThat(epochRoundTrip.get(5), is(LocalDate.of(1970, 1, 1)));
    assertThat(epochRoundTrip.get(6), is(LocalTime.of(0, 0, 0)));
    assertThat(epochRoundTrip.get(7), is(LocalTime.of(0, 0, 0)));

    assertThat(bttfRoundTrip.get(0), is(new Utf8("BackToTheFuture")));
    assertThat(bttfRoundTrip.get(1), is(Instant.ofEpochMilli(477228896123L)));
    assertThat(bttfRoundTrip.get(2), is(Instant.ofEpochSecond(477228896L, 123456000L)));
    if (AvroVersion.avro_1_10.orAfter()) {
      assertThat(bttfRoundTrip.get(3), is(LocalDateTime.of(1985, 2, 14, 11, 34, 56, 123000000)));
      assertThat(bttfRoundTrip.get(4), is(LocalDateTime.of(1985, 2, 14, 11, 34, 56, 123456000)));
    } else {
      assertThat(bttfRoundTrip.get(3), is(477228896123L));
      assertThat(bttfRoundTrip.get(4), is(477228896123456L));
    }
    assertThat(bttfRoundTrip.get(5), is(LocalDate.of(1985, 2, 14)));
    assertThat(bttfRoundTrip.get(6), is(LocalTime.of(11, 34, 56, 123000000)));
    assertThat(bttfRoundTrip.get(7), is(LocalTime.of(11, 34, 56, 123456000)));
  }

  @Test
  @EnabledForAvroVersion(
      until = AvroVersion.avro_1_10,
      reason = "joda time classes are supported until Avro 1.10.x")
  public void testRecordsWithJodaConversions() {
    GenericData withConversions = AvroUtil.api().withJodaTimeConversions();

    // Deserializing and reading with the GenericData and date/time logical types return primitives.
    IndexedRecord epochRoundTrip = roundTripBytes(withConversions, DATE_TIME_SCHEMA, EPOCH);
    assertThat(epochRoundTrip.toString(), not(EPOCH.toString()));
    IndexedRecord bttfRoundTrip = roundTripBytes(withConversions, DATE_TIME_SCHEMA, BTTF);
    assertThat(bttfRoundTrip.toString(), not(BTTF.toString()));

    // In 1.8.x, these classes are automatically brought in as transitive dependencies
    // In 1.9.x, they need to be manually added to the classpath
    // In 1.10.x, joda-time is no longer used
    assertThat(epochRoundTrip.get(0).getClass().getName(), is("org.apache.avro.util.Utf8"));
    assertThat(epochRoundTrip.get(1).getClass().getName(), is("org.joda.time.DateTime"));
    assertThat(epochRoundTrip.get(2).getClass().getName(), is("org.joda.time.DateTime"));
    assertThat(epochRoundTrip.get(3).getClass().getName(), is("java.lang.Long"));
    assertThat(epochRoundTrip.get(4).getClass().getName(), is("java.lang.Long"));
    assertThat(epochRoundTrip.get(5).getClass().getName(), is("org.joda.time.LocalDate"));
    assertThat(epochRoundTrip.get(6).getClass().getName(), is("org.joda.time.LocalTime"));
    assertThat(epochRoundTrip.get(7).getClass().getName(), is("org.joda.time.LocalTime"));
  }

  @Test
  public void testGetLogicalTypeFromSchema() {
    Schema s = AvroUtil.api().parse(AvroLogicalTypes$.MODULE$.Date());
    LogicalType lt = LogicalTypes.fromSchema(s);

    assertThat(lt.getName(), is("date"));
    assertThat(lt, instanceOf(LogicalTypes.Date.class));

    Schema s2 = AvroUtil.api().parse(AvroLogicalTypes$.MODULE$.DateLogicalTypeRecord());
    LogicalType lt2 = LogicalTypes.fromSchema(s2.getFields().get(1).schema());
    assertThat(lt2.getName(), is("timestamp-millis"));
    assertThat(lt2, instanceOf(LogicalTypes.TimestampMillis.class));

    Schema s3 = AvroUtil.api().parse(AvroLogicalTypes$.MODULE$.DateLogicalTypeRecordInvalid());
    LogicalType lt3 = LogicalTypes.fromSchema(s3.getFields().get(1).schema());
    assertThat(lt3, nullValue());
  }
}
