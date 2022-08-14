package com.skraba.avro.enchiridion.core.evolution;

import static com.skraba.avro.enchiridion.core.AvroUtil.api;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.fromBytes;
import static com.skraba.avro.enchiridion.core.SerializeToBytesTest.toBytes;
import static com.skraba.avro.enchiridion.testkit.AvroAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

/** Test reading data with a schema that has evolved by changing to/from a union. */
class EvolveUnionTest {

  @Test
  void testConvertAFieldFromPrimitiveToUnion() {
    Schema v1 = api().createRecord("ns.A", "l");
    GenericRecord r1 = new GenericRecordBuilder(v1).set("a0", 123_456L).build();
    Schema v2 = api().createRecord("ns.A", "|l ");

    assertThat(v1).isRecord("ns.A").hasFieldsNamed("a0").compatibilityWith(v2).isOK();
    byte[] bin1 = toBytes(r1.getSchema(), r1);
    GenericRecord r2 = fromBytes(v1, v2, bin1);
    assertThat(r2).hasFieldEqualTo("a0", 123_456L);

    // And back works too, even though the schemas aren't guaranteed to be compatible
    assertThat(v2)
        .isRecord("ns.A")
        .hasFieldsNamed("a0")
        .compatibilityWith(v1)
        .isNotOK("TYPE_MISMATCH");
    byte[] bin2 = toBytes(r2.getSchema(), r2);
    GenericRecord r1x = fromBytes(v2, v1, bin2);
    assertThat(r1x).hasFieldEqualTo("a0", 123_456L);

    // But it doesn´t always work...
    GenericRecord r2null = new GenericRecordBuilder(v2).set("a0", null).build();
    byte[] bin2null = toBytes(r2null.getSchema(), r2null);
    assertThatThrownBy(() -> fromBytes(v2, v1, bin2null))
        .isInstanceOf(AvroTypeException.class)
        .hasMessage("Found null, expecting long");
  }

  @Test
  void testConvertAFieldFromUnionToWiderUnion() {
    Schema v1 = api().createRecord("ns.A", "| l");
    Schema v2 = api().createRecord("ns.A", "|l s");

    for (Object value : new Object[] {null, 123_456L}) {
      GenericRecord r1 = new GenericRecordBuilder(v1).set("a0", value).build();

      assertThat(v1)
          .isRecord()
          .hasFullName("ns.A")
          .hasFieldsNamed("a0")
          .compatibilityWith(v2)
          .isOK();
      byte[] bin1 = toBytes(r1.getSchema(), r1);
      GenericRecord r2 = fromBytes(v1, v2, bin1);
      assertThat(r2).hasFieldEqualTo("a0", value);

      // And back works too, even though the schemas aren't guaranteed to be compatible
      assertThat(v2)
          .isRecord()
          .hasFullName("ns.A")
          .hasFieldsNamed("a0")
          .compatibilityWith(v1)
          .isNotOK("MISSING_UNION_BRANCH");
      byte[] bin2 = toBytes(r2.getSchema(), r2);
      GenericRecord r1x = fromBytes(v2, v1, bin2);
      assertThat(r1x).hasFieldEqualTo("a0", value);
    }

    // But it doesn´t always work...
    GenericRecord r2string = new GenericRecordBuilder(v2).set("a0", "Broken").build();
    byte[] bin2string = toBytes(r2string.getSchema(), r2string);
    assertThatThrownBy(() -> fromBytes(v2, v1, bin2string))
        .isInstanceOf(AvroTypeException.class)
        .hasMessage("Found string, expecting union");
  }

  @Test
  void testConvertAFieldFromPrimitiveToUnionWithPromotion() {
    Schema v1 = api().createRecord("ns.A", "| l");
    Schema v2 = api().createRecord("ns.A", "| df");

    for (Object value : new Object[] {null, 123_456L}) {
      GenericRecord r1 = new GenericRecordBuilder(v1).set("a0", value).build();

      assertThat(v1)
          .isRecord()
          .hasName("A")
          .hasFullName("ns.A")
          .hasNamespace("ns")
          .hasFieldsNamed("a0")
          .compatibilityWith(v2)
          .isOK();
      byte[] bin1 = toBytes(r1.getSchema(), r1);
      GenericRecord r2 = fromBytes(v1, v2, bin1);
      assertThat(r2)
          .hasFieldEqualTo("a0", value instanceof Long ? ((Long) value).doubleValue() : value);

      // Back only works for null, double is not promotable to long.
      assertThat(v2)
          .isRecord()
          .hasName("A")
          .hasFullName("ns.A")
          .hasNamespace("ns")
          .hasFieldsNamed("a0")
          .compatibilityWith(v1)
          .isNotOK("MISSING_UNION_BRANCH", "MISSING_UNION_BRANCH");
      byte[] bin2 = toBytes(r2.getSchema(), r2);
      if (value == null) {
        GenericRecord r1x = fromBytes(v2, v1, bin2);
        assertThat(r1x).hasFieldEqualTo("a0", value);
      } else {
        assertThatThrownBy(() -> fromBytes(v2, v1, bin2))
            .isInstanceOf(AvroTypeException.class)
            .hasMessage("Found double, expecting union");
      }
    }
  }

  @Test
  void testConvertAFieldFromPrimitiveToUnionWithPromotionAndExactMatch() {
    Schema v1 = api().createRecord("ns.A", "| l");
    Schema v2 = api().createRecord("ns.A", "| dfl");

    for (Object value : new Object[] {null, 123_456L}) {
      GenericRecord r1 = new GenericRecordBuilder(v1).set("a0", value).build();

      assertThat(v1)
          .isRecord()
          .hasName("A")
          .hasFullName("ns.A")
          .hasNamespace("ns")
          .hasFieldsNamed("a0")
          .compatibilityWith(v2)
          .isOK();
      byte[] bin1 = toBytes(r1.getSchema(), r1);
      GenericRecord r2 = fromBytes(v1, v2, bin1);
      assertThat(r2).hasFieldEqualTo("a0", value);

      // And back works too, even though the schemas aren't guaranteed to be compatible
      assertThat(v2)
          .isRecord()
          .hasName("A")
          .hasFullName("ns.A")
          .hasNamespace("ns")
          .hasFieldsNamed("a0")
          .compatibilityWith(v1)
          .isNotOK("MISSING_UNION_BRANCH", "MISSING_UNION_BRANCH");
      byte[] bin2 = toBytes(r2.getSchema(), r2);
      GenericRecord r1x = fromBytes(v2, v1, bin2);
      assertThat(r1x).hasFieldEqualTo("a0", value);
    }
  }
}
