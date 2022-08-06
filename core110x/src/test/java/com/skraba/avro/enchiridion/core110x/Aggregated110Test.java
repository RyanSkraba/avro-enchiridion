package com.skraba.avro.enchiridion.core110x;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.core.Aggregated;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
import org.junit.jupiter.api.Test;

public class Aggregated110Test extends Aggregated {

  @Test
  public void testAvroVersion() {
    assertThat(AvroVersion.avro_1_11.before("Next major version"), is(true));
    assertThat(AvroVersion.avro_1_10.orAfter("This major version"), is(true));
    assertThat(AvroVersion.getInstalledAvro(), is(AvroVersion.avro_1_10));
  }
}
