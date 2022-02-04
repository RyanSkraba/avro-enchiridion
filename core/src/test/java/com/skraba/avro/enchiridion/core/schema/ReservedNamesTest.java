package com.skraba.avro.enchiridion.core.schema;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static com.skraba.avro.enchiridion.core.AvroUtil.qqify;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.fromBytes;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.toBytes;
import static com.skraba.avro.enchiridion.core.SerializeToJsonTest.fromJson;
import static com.skraba.avro.enchiridion.core.SerializeToJsonTest.toJson;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

public class ReservedNamesTest {

  public static final String BASIC =
      "{'type':'record','name':'enum','fields':[{'name':'a1','type':'long'}]}";
  public static final String LINKED_LIST =
      "{'type':'record','name':'LinkedList','fields':[{'name':'value','type':'int'},{'name':'next','type':['null','LinkedList']}]}";
  public static final String LINKED_LIST_VERBOSE =
      "{'type':'record','name':'LinkedList','fields':[{'name':'value','type':'int'},{'name':'next','type':['null',{'type':'LinkedList'}]}]}";
  public static final String NAMESPACED_INT =
      "{'type':'record','name':'ns.int','fields':[{'name':'a1','type':'long'}]}";

  @Test
  public void testRecordNamedEnum() {
    Schema s = api().parse(qqify(BASIC));

    GenericRecord datum = new GenericRecordBuilder(s).set("a1", 0L).build();

    byte[] binary = toBytes(GenericData.get(), s, datum);
    GenericRecord roundTrip = fromBytes(GenericData.get(), s, binary);

    String json = toJson(GenericData.get(), s, datum);
    GenericRecord roundTrip2 = fromJson(GenericData.get(), s, json);

    assertThat(roundTrip, is(datum));
    assertThat(roundTrip2, is(datum));
  }

  @Test
  public void testRecordNamedEnumWithNamespace() {
    Schema s = api().parse(qqify(BASIC.replaceFirst("'name'", "'namespace':'_','name'")));

    GenericRecord datum = new GenericRecordBuilder(s).set("a1", 0L).build();

    byte[] binary = toBytes(GenericData.get(), s, datum);
    GenericRecord roundTrip = fromBytes(GenericData.get(), s, binary);

    String json = toJson(GenericData.get(), s, datum);
    GenericRecord roundTrip2 = fromJson(GenericData.get(), s, json);

    assertThat(roundTrip, is(datum));
    assertThat(roundTrip2, is(datum));
  }

  @Test
  public void testLinkedListRecord() {
    Schema s = api().parse(qqify(LINKED_LIST));
    assertThat(api().parse(qqify(LINKED_LIST_VERBOSE)), is(s));

    GenericRecord datum =
        new GenericRecordBuilder(s)
            .set("value", 0)
            .set("next", new GenericRecordBuilder(s).set("value", 1).set("next", null).build())
            .build();

    byte[] binary = toBytes(GenericData.get(), s, datum);
    GenericRecord roundTrip = fromBytes(GenericData.get(), s, binary);

    String json = toJson(GenericData.get(), s, datum);
    GenericRecord roundTrip2 = fromJson(GenericData.get(), s, json);

    assertThat(roundTrip, is(datum));
    assertThat(roundTrip2, is(datum));
  }

  @Test
  public void testLinkedListRecordAsEnum() {
    Schema s = api().parse(qqify(LINKED_LIST.replaceAll("LinkedList", "enum")));
    assertThrows(
        SchemaParseException.class,
        () ->
            assertThat(
                api().parse(qqify(LINKED_LIST_VERBOSE.replaceAll("LinkedList", "enum"))), is(s)));

    GenericRecord datum =
        new GenericRecordBuilder(s)
            .set("value", 0)
            .set("next", new GenericRecordBuilder(s).set("value", 1).set("next", null).build())
            .build();

    byte[] binary = toBytes(GenericData.get(), s, datum);
    GenericRecord roundTrip = fromBytes(GenericData.get(), s, binary);

    String json = toJson(GenericData.get(), s, datum);
    GenericRecord roundTrip2 = fromJson(GenericData.get(), s, json);

    assertThat(roundTrip, is(datum));
    assertThat(roundTrip2, is(datum));
  }

  @Test
  public void testLinkedListRecordAsUnion() {
    Schema s = api().parse(qqify(LINKED_LIST.replaceAll("LinkedList", "union")));
    assertThat(api().parse(qqify(LINKED_LIST_VERBOSE.replaceAll("LinkedList", "union"))), is(s));

    GenericRecord datum =
        new GenericRecordBuilder(s)
            .set("value", 0)
            .set("next", new GenericRecordBuilder(s).set("value", 1).set("next", null).build())
            .build();

    byte[] binary = toBytes(GenericData.get(), s, datum);
    GenericRecord roundTrip = fromBytes(GenericData.get(), s, binary);

    String json = toJson(GenericData.get(), s, datum);
    GenericRecord roundTrip2 = fromJson(GenericData.get(), s, json);

    assertThat(roundTrip, is(datum));
    assertThat(roundTrip2, is(datum));
  }

  @Test
  public void testNamespacedInt() {
    Schema s = api().parse(qqify(NAMESPACED_INT));

    GenericRecord datum = new GenericRecordBuilder(s).set("a1", 0L).build();

    byte[] binary = toBytes(GenericData.get(), s, datum);
    GenericRecord roundTrip = fromBytes(GenericData.get(), s, binary);

    String json = toJson(GenericData.get(), s, datum);
    GenericRecord roundTrip2 = fromJson(GenericData.get(), s, json);

    assertThat(roundTrip, is(datum));
    assertThat(roundTrip2, is(datum));
  }

  @Test
  public void testBasicReferenceNamespacedInt() {
    Schema s =
        api()
            .parse(
                qqify(
                    LINKED_LIST
                        .replaceFirst("LinkedList", "_.int")
                        .replaceFirst("LinkedList", "_.int")));

    GenericRecord datum =
        new GenericRecordBuilder(s)
            .set("value", 0)
            .set("next", new GenericRecordBuilder(s).set("value", 1).set("next", null).build())
            .build();

    byte[] binary = toBytes(GenericData.get(), s, datum);
    GenericRecord roundTrip = fromBytes(GenericData.get(), s, binary);

    String json = toJson(GenericData.get(), s, datum);
    GenericRecord roundTrip2 = fromJson(GenericData.get(), s, json);

    assertThat(roundTrip, is(datum));
    assertThat(roundTrip2, is(datum));
  }
}
