package com.skraba.avro.enchiridion.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.idl.Inner;
import com.skraba.avro.enchiridion.idl.Outer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

public class ImporterTest {

  public static final Outer OUTER = new Outer("Outer", 0, new Inner("Inner", 1));

  @Test
  public void testOuter() {
    // This should be generated as a test resource.
    assertThat(OUTER, instanceOf(GenericRecord.class));
    assertThat(OUTER.outer0, is("Outer"));
    assertThat(OUTER.outer1, is(0));
  }
}
