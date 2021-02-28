package com.skraba.avro.enchiridion.coresnapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.core.Aggregated;
import com.skraba.avro.enchiridion.core.AvroVersion;
import org.junit.jupiter.api.Test;

public class AggregatedSnapshotTest extends Aggregated {
  @Test
  public void testAvroVersion() {
    assertThat(AvroVersion.avro_infinity.before("Next major version"), is(true));
    assertThat(AvroVersion.avro_1_11.orAfter("This major version"), is(true));
    assertThat(AvroVersion.getInstalledAvro(), is(AvroVersion.avro_1_11));
  }
}
