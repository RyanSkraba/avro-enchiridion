package com.skraba.avro.enchiridion.plugin;

import static com.skraba.avro.enchiridion.plugin.SimpleRecordTest.fromBytes;
import static com.skraba.avro.enchiridion.plugin.SimpleRecordTest.toBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.skraba.avro.enchiridion.idl.Avro2956ReservedKeywordWrapper;
import com.skraba.avro.enchiridion.idl.case$;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Unit tests related to AVRO-2956 */
public class Avro2956ReservedKeywordTest {

  @Test
  public void testBasic() throws IOException {
    Avro2956ReservedKeywordWrapper wrapper =
        Avro2956ReservedKeywordWrapper.newBuilder()
            .setInner(case$.newBuilder().setValue("AVRO-2956").build())
            .build();
    byte[] serialized = toBytes(wrapper.getSpecificData(), wrapper.getSchema(), wrapper);
    assertThat(serialized.length, equalTo(10));

    // Round-trip should reconstitute an equal instance, but this throws a ClassCastException
    ClassCastException ex =
        assertThrows(
            ClassCastException.class,
            () ->
                assertThat(
                    fromBytes(wrapper.getSpecificData(), wrapper.getSchema(), serialized),
                    is(wrapper)));
    assertThat(
        ex.getMessage(),
        containsString("org.apache.avro.generic.GenericData$Record cannot be cast"));
  }
}
