package com.skraba.avro.enchiridion.core19x;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.core.Aggregated;
import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

public class Aggregated19Test extends Aggregated {
  static {
    AvroUtil.api = ThreadLocal.withInitial(ApiCompatibility19x::new);
  }

  @Test
  public void testAvroVersion() {
    assertThat(AvroVersion.avro_1_10.before("Next major version"), is(true));
    assertThat(AvroVersion.avro_1_9.orAfter("This major version"), is(true));
    assertThat(AvroVersion.getInstalledAvro(), is(AvroVersion.avro_1_9));
  }

  /** Some of the methods tested need to be adapted to Avro 1.9 */
  private static class ApiCompatibility19x extends AvroUtil.ApiCompatibility {

    @Override
    public GenericData withJodaTimeConversions(GenericData... models) {
      return withConversions(
          new String[] {
            "org.apache.avro.data.JodaTimeConversions$DateConversion",
            "org.apache.avro.data.JodaTimeConversions$TimeConversion",
            "org.apache.avro.data.JodaTimeConversions$TimeMicrosConversion",
            "org.apache.avro.data.JodaTimeConversions$TimestampConversion",
            "org.apache.avro.data.JodaTimeConversions$TimestampMicrosConversion"
          },
          models);
    }

    @Override
    public GenericData withJavaTimeConversions(GenericData... models) {
      return withConversions(
          new String[] {
            "org.apache.avro.data.TimeConversions$DateConversion",
            "org.apache.avro.data.TimeConversions$TimeMicrosConversion",
            "org.apache.avro.data.TimeConversions$TimeMillisConversion",
            "org.apache.avro.data.TimeConversions$TimestampMicrosConversion",
            "org.apache.avro.data.TimeConversions$TimestampMillisConversion"
          },
          models);
    }
  }
}
