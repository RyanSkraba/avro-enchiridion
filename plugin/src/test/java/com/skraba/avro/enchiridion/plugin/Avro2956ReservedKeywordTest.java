package com.skraba.avro.enchiridion.plugin;

import static com.skraba.avro.enchiridion.plugin.SimpleRecordTest.fromBytes;
import static com.skraba.avro.enchiridion.plugin.SimpleRecordTest.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.iddl.goto$.case$;
import com.skraba.avro.enchiridion.idl.Avro2956ReservedKeywordWrapper;
import com.skraba.avro.enchiridion.idl.break$.static$;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
import com.skraba.avro.enchiridion.testkit.EnabledForAvroVersion;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.junit.jupiter.api.Test;

/** Unit tests related to AVRO-2956 */
public class Avro2956ReservedKeywordTest {

  @Test
  @EnabledForAvroVersion(until = AvroVersion.avro_1_12, reason = "Fixed in Avro 1.12.0")
  public void testBasic() {

    // Start with a simple serialized long.
    byte[] serialized = {0};

    // This is equivalent to this record.
    Avro2956ReservedKeywordWrapper record =
        Avro2956ReservedKeywordWrapper.newBuilder()
            .setInner(case$.newBuilder().setDefault$(0).build())
            .build();
    assertThat(toBytes(record.getSpecificData(), record.getSchema(), record), is(serialized));

    // Problem one: round-trip should reconstitute an equal instance, but this throws a
    // ClassCastException.
    ClassCastException ex =
        assertThrows(
            ClassCastException.class,
            () -> fromBytes(record.getSpecificData(), record.getSchema(), serialized));
    assertThat(
        ex.getMessage(),
        containsString("org.apache.avro.generic.GenericData$Record cannot be cast"));

    // This should be structurally identical to the above record.
    static$ record2 =
        static$.newBuilder().setSwitch$(case$.newBuilder().setDefault$(0).build()).build();
    assertThat(toBytes(record2.getSpecificData(), record.getSchema(), record), is(serialized));

    // Round-trip should reconstitute an equal instance, but this throws a ClassCastException.
    ex =
        assertThrows(
            ClassCastException.class,
            () -> {
              // Note that we've specified the expected return type.
              static$ unused =
                  fromBytes(record2.getSpecificData(), record2.getSchema(), serialized);
            });
    assertThat(
        ex.getMessage(),
        containsString("org.apache.avro.generic.GenericData$Record cannot be cast"));

    // Problem two: we can deserialize to GenericRecord and even compare it to the original.
    GenericRecord record3 = fromBytes(record2.getSpecificData(), record2.getSchema(), serialized);
    assertThat(record3, instanceOf(GenericData.Record.class));
    assertThat(SpecificData.get().compare(record2, record3, static$.getClassSchema()), is(0));

    // Problem three: but they aren't equal.
    assertThat(record2.equals(record3), is(false));
  }
}
