package com.skraba.avro.enchiridion.core.logical;

import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.roundTripBytes;
import static com.skraba.avro.enchiridion.resources.AvroLogicalTypes$.MODULE$;
import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.avro.generic.GenericRecordBuilder;
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
          ZonedDateTime.of(2015, 10, 21, 16, 29, 56, 123456789, ZoneOffset.ofHours(-7))
              .toInstant());

  private final IndexedRecord EPOCH =
      createRecordDateTimeTypes(
          "epoch", ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant());

  /**
   * @return A record with the {@link AvroLogicalTypes$#MODULE$#DateLogicalTypeRecord()} schema with
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
    IndexedRecord epochRoundTrip = roundTripBytes(DATE_TIME_SCHEMA, EPOCH);
    assertThat(EPOCH).isEqualTo(epochRoundTrip);
    IndexedRecord bttfRoundTrip = roundTripBytes(DATE_TIME_SCHEMA, BTTF);
    assertThat(BTTF).isEqualTo(bttfRoundTrip);

    assertThat(epochRoundTrip.get(0)).isEqualTo(new Utf8("epoch"));
    assertThat(epochRoundTrip.get(1)).isEqualTo(0L);
    assertThat(epochRoundTrip.get(2)).isEqualTo(0L);
    assertThat(epochRoundTrip.get(3)).isEqualTo(0L);
    assertThat(epochRoundTrip.get(4)).isEqualTo(0L);
    assertThat(epochRoundTrip.get(5)).isEqualTo(0);
    assertThat(epochRoundTrip.get(6)).isEqualTo(0);
    assertThat(epochRoundTrip.get(7)).isEqualTo(0L);

    assertThat(bttfRoundTrip.get(0)).isEqualTo(new Utf8("BackToTheFuture"));
    assertThat(bttfRoundTrip.get(1)).isEqualTo(1445470196123L);
    assertThat(bttfRoundTrip.get(2)).isEqualTo(1445470196123456L);
    assertThat(bttfRoundTrip.get(3)).isEqualTo(1445470196123L);
    assertThat(bttfRoundTrip.get(4)).isEqualTo(1445470196123456L);
    assertThat(bttfRoundTrip.get(5)).isEqualTo(16729);
    assertThat(bttfRoundTrip.get(6)).isEqualTo(84596123);
    assertThat(bttfRoundTrip.get(7)).isEqualTo(84596123456L);
  }

  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_9,
      reason = "java.time classes aren't supported until Avro 1.9.x")
  public void testRecordsWithJavaTimeConversions() {
    GenericData withConversions = AvroUtil.api().withJavaTimeConversions();

    // Deserializing and reading with the GenericData and date/time logical types return primitives.
    IndexedRecord epochRoundTrip = roundTripBytes(withConversions, DATE_TIME_SCHEMA, EPOCH);
    assertThat(epochRoundTrip.toString()).isNotEqualTo(EPOCH.toString());
    IndexedRecord bttfRoundTrip = roundTripBytes(withConversions, DATE_TIME_SCHEMA, BTTF);
    assertThat(bttfRoundTrip.toString()).isNotEqualTo(BTTF.toString());

    // TODO: java.lang.ClassCastException: java.time.Instant cannot be cast to java.lang.Long
    // Remove the .toString above!

    assertThat(epochRoundTrip.get(0)).isEqualTo(new Utf8("epoch"));
    assertThat(epochRoundTrip.get(1)).isEqualTo(Instant.ofEpochMilli(0));
    assertThat(epochRoundTrip.get(2)).isEqualTo(Instant.ofEpochMilli(0));
    if (AvroVersion.avro_1_10.orAfter(
        "local-timestamp-millis and local-timestamp-micros appeared in 1.10.x")) {
      assertThat(epochRoundTrip.get(3)).isEqualTo(LocalDateTime.of(1970, 1, 1, 0, 0, 0));
      assertThat(epochRoundTrip.get(4)).isEqualTo(LocalDateTime.of(1970, 1, 1, 0, 0, 0));
    } else {
      assertThat(epochRoundTrip.get(3)).isEqualTo(0L);
      assertThat(epochRoundTrip.get(4)).isEqualTo(0L);
    }
    assertThat(epochRoundTrip.get(5)).isEqualTo(LocalDate.of(1970, 1, 1));
    assertThat(epochRoundTrip.get(6)).isEqualTo(LocalTime.of(0, 0, 0));
    assertThat(epochRoundTrip.get(7)).isEqualTo(LocalTime.of(0, 0, 0));

    assertThat(bttfRoundTrip.get(0)).isEqualTo(new Utf8("BackToTheFuture"));
    assertThat(bttfRoundTrip.get(1)).isEqualTo(Instant.ofEpochMilli(1445470196123L));
    assertThat(bttfRoundTrip.get(2)).isEqualTo(Instant.ofEpochSecond(1445470196, 123456000L));
    if (AvroVersion.avro_1_10.orAfter(
        "local-timestamp-millis and local-timestamp-micros appeared in 1.10.x")) {
      assertThat(bttfRoundTrip.get(3))
          .isEqualTo(LocalDateTime.of(2015, 10, 21, 23, 29, 56, 123000000));
      assertThat(bttfRoundTrip.get(4))
          .isEqualTo(LocalDateTime.of(2015, 10, 21, 23, 29, 56, 123456000));
    } else {
      assertThat(bttfRoundTrip.get(3)).isEqualTo(1445470196123L);
      assertThat(bttfRoundTrip.get(4)).isEqualTo(1445470196123456L);
    }
    assertThat(bttfRoundTrip.get(5)).isEqualTo(LocalDate.of(2015, 10, 21));
    assertThat(bttfRoundTrip.get(6)).isEqualTo(LocalTime.of(23, 29, 56, 123000000));
    assertThat(bttfRoundTrip.get(7)).isEqualTo(LocalTime.of(23, 29, 56, 123456000));
  }

  @Test
  @EnabledForAvroVersion(
      until = AvroVersion.avro_1_10,
      reason = "joda time classes are supported until Avro 1.10.x")
  public void testRecordsWithJodaConversions() {
    GenericData withConversions = AvroUtil.api().withJodaTimeConversions();

    // Deserializing and reading with the GenericData and date/time logical types return primitives.
    IndexedRecord epochRoundTrip = roundTripBytes(withConversions, DATE_TIME_SCHEMA, EPOCH);
    assertThat(epochRoundTrip.toString()).isNotEqualTo(EPOCH.toString());
    IndexedRecord bttfRoundTrip = roundTripBytes(withConversions, DATE_TIME_SCHEMA, BTTF);
    assertThat(bttfRoundTrip.toString()).isNotEqualTo(BTTF.toString());

    // In 1.8.x, these classes are automatically brought in as transitive dependencies
    // In 1.9.x, they need to be manually added to the classpath
    // In 1.10.x, joda-time is no longer used
    assertThat(epochRoundTrip.get(0).getClass().getName()).isEqualTo("org.apache.avro.util.Utf8");
    assertThat(epochRoundTrip.get(1).getClass().getName()).isEqualTo("org.joda.time.DateTime");
    assertThat(epochRoundTrip.get(2).getClass().getName()).isEqualTo("org.joda.time.DateTime");
    assertThat(epochRoundTrip.get(3).getClass().getName()).isEqualTo("java.lang.Long");
    assertThat(epochRoundTrip.get(4).getClass().getName()).isEqualTo("java.lang.Long");
    assertThat(epochRoundTrip.get(5).getClass().getName()).isEqualTo("org.joda.time.LocalDate");
    assertThat(epochRoundTrip.get(6).getClass().getName()).isEqualTo("org.joda.time.LocalTime");
    assertThat(epochRoundTrip.get(7).getClass().getName()).isEqualTo("org.joda.time.LocalTime");
  }

  @Test
  public void testGetLogicalTypeFromSchema() {
    Schema s = AvroUtil.api().parse(AvroLogicalTypes$.MODULE$.Date());
    LogicalType lt = LogicalTypes.fromSchema(s);

    assertThat(lt.getName()).isEqualTo("date");
    assertThat(lt).isInstanceOf(LogicalTypes.Date.class);

    Schema s2 = AvroUtil.api().parse(AvroLogicalTypes$.MODULE$.DateLogicalTypeRecord());
    LogicalType lt2 = LogicalTypes.fromSchema(s2.getFields().get(1).schema());
    assertThat(lt2.getName()).isEqualTo("timestamp-millis");
    assertThat(lt2).isInstanceOf(LogicalTypes.TimestampMillis.class);

    Schema s3 = AvroUtil.api().parse(AvroLogicalTypes$.MODULE$.DateLogicalTypeRecordInvalid());
    LogicalType lt3 = LogicalTypes.fromSchema(s3.getFields().get(1).schema());
    assertThat(lt3).isNull();
  }

  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_9,
      reason = "java.time classes aren't supported until Avro 1.9.x")
  public void testGetMaxDateRanges() {
    GenericData withConversions = AvroUtil.api().withJavaTimeConversions();
    GenericRecord maxO =
        new GenericRecordBuilder(DATE_TIME_SCHEMA)
            .set("name", "max")
            .set("datetime_ms", Long.MAX_VALUE)
            .set("datetime_us", Long.MAX_VALUE)
            .set("local_datetime_ms", Long.MAX_VALUE)
            .set("local_datetime_us", Long.MAX_VALUE)
            .set("date", Integer.MAX_VALUE)
            .set("time_ms", 24 * 60 * 60 * 1000 - 1)
            .set("time_us", 24 * 60 * 60 * 1000 * 1000L - 1)
            .build();
    IndexedRecord max = roundTripBytes(withConversions, DATE_TIME_SCHEMA, maxO);
    assertThat(max.toString()).isNotEqualTo(maxO.toString());

    assertThat(max.get(0)).isEqualTo(new Utf8("max"));
    assertThat(max.get(1)).isEqualTo(Instant.ofEpochMilli(Long.MAX_VALUE));
    // MAX_VALUE mod 1000000 and remainder * 1000
    assertThat(max.get(2)).isEqualTo(Instant.ofEpochSecond(9223372036854L, 775807000));
    if (AvroVersion.avro_1_10.orAfter(
        "local-timestamp-millis and local-timestamp-micros appeared in 1.10.x")) {
      assertThat(max.get(3)).isEqualTo(LocalDateTime.of(292278994, 8, 17, 7, 12, 55, 807000000));
      assertThat(max.get(4)).isEqualTo(LocalDateTime.of(294247, 1, 10, 4, 0, 54, 775807000));
    } else {
      assertThat(max.get(3)).isEqualTo(Long.MAX_VALUE);
      assertThat(max.get(4)).isEqualTo(Long.MAX_VALUE);
    }
    assertThat(max.get(5)).isEqualTo(LocalDate.of(5881580, 7, 11));
    assertThat(max.get(6)).isEqualTo(LocalTime.of(23, 59, 59, 999000000));
    assertThat(max.get(7)).isEqualTo(LocalTime.of(23, 59, 59, 999999000));
  }

  @Test
  @EnabledForAvroVersion(
      startingFrom = AvroVersion.avro_1_9,
      reason = "java.time classes aren't supported until Avro 1.9.x")
  public void testGetMinDateRanges() {
    GenericData withConversions = AvroUtil.api().withJavaTimeConversions();
    GenericRecord maxO =
        new GenericRecordBuilder(DATE_TIME_SCHEMA)
            .set("name", "max")
            .set("datetime_ms", Long.MIN_VALUE)
            .set("datetime_us", Long.MIN_VALUE)
            .set("local_datetime_ms", Long.MIN_VALUE)
            .set("local_datetime_us", Long.MIN_VALUE)
            .set("date", Integer.MIN_VALUE)
            .set("time_ms", 0)
            .set("time_us", 0L)
            .build();
    IndexedRecord max = roundTripBytes(withConversions, DATE_TIME_SCHEMA, maxO);
    assertThat(max.toString()).isNotEqualTo(maxO.toString());

    assertThat(max.get(0)).isEqualTo(new Utf8("max"));
    assertThat(max.get(1)).isEqualTo(Instant.ofEpochMilli(Long.MIN_VALUE));
    assertThat(max.get(2)).isEqualTo(Instant.ofEpochSecond(-9223372036855L, 224192000));
    if (AvroVersion.avro_1_10.orAfter(
        "local-timestamp-millis and local-timestamp-micros appeared in 1.10.x")) {
      assertThat(max.get(3)).isEqualTo(LocalDateTime.of(-292275055, 5, 16, 16, 47, 4, 192000000));
      assertThat(max.get(4)).isEqualTo(LocalDateTime.of(-290308, 12, 21, 19, 59, 5, 224192000));
    } else {
      assertThat(max.get(3)).isEqualTo(Long.MIN_VALUE);
      assertThat(max.get(4)).isEqualTo(Long.MIN_VALUE);
    }
    assertThat(max.get(5)).isEqualTo(LocalDate.of(-5877641, 6, 23));
    assertThat(max.get(6)).isEqualTo(LocalTime.of(0, 0, 0, 0));
    assertThat(max.get(7)).isEqualTo(LocalTime.of(0, 0, 0, 0));
  }
}
