package com.skraba.avro.enchiridion.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.simple.DateLogicalTypeOptionalRecord;
import com.skraba.avro.enchiridion.simple.DateLogicalTypeRecord;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/** Unit tests for the LogicalTypes. */
// TODO: Enabled for avro version 1.8 for logical types, verify no joda.
public class LogicalTypesSpecificRecordTest {

  @Test
  public void testDateLogicalTypeRecord() throws IOException {
    // A record with one of every date or time logical type.
    DateLogicalTypeRecord record = new DateLogicalTypeRecord();

    assertThat(record.getSchema().getFields(), hasSize(8));
    record.setName("zero");
    record.setDatetimeMs(Instant.ofEpochMilli(0));
    record.setDatetimeUs(Instant.ofEpochMilli(0).plusNanos(0));
    record.setLocalDatetimeMs(LocalDateTime.ofInstant(record.getDatetimeMs(), ZoneOffset.UTC));
    record.setLocalDatetimeUs(LocalDateTime.ofInstant(record.getDatetimeMs(), ZoneOffset.UTC));
    record.setDate(LocalDate.ofEpochDay(0));
    record.setTimeMs(LocalTime.ofNanoOfDay(0));
    record.setTimeUs(LocalTime.ofNanoOfDay(0));

    ByteBuffer bb = record.toByteBuffer();
    assertThat(bb.remaining(), is(22));
    assertThat(DateLogicalTypeRecord.fromByteBuffer(bb), is(record));
  }

  // This only works in 1.10.2
  @Test
  public void testDateLogicalTypeOptionalRecord() throws IOException {
    // A record with one of every date or time logical type, but nullable.
    DateLogicalTypeOptionalRecord record = new DateLogicalTypeOptionalRecord();

    assertThat(record.getSchema().getFields(), hasSize(8));
    record.setName("zero");
    record.setDatetimeMs(Instant.ofEpochMilli(0));
    record.setDatetimeUs(Instant.ofEpochMilli(0).plusNanos(0));
    record.setLocalDatetimeMs(LocalDateTime.ofInstant(record.getDatetimeMs(), ZoneOffset.UTC));
    record.setLocalDatetimeUs(LocalDateTime.ofInstant(record.getDatetimeMs(), ZoneOffset.UTC));
    record.setDate(LocalDate.ofEpochDay(0));
    record.setTimeMs(LocalTime.ofNanoOfDay(0));
    record.setTimeUs(LocalTime.ofNanoOfDay(0));

    ByteBuffer bb = record.toByteBuffer();
    assertThat(bb.remaining(), is(29));
  }
}
