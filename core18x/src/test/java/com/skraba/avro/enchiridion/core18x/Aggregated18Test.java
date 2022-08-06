package com.skraba.avro.enchiridion.core18x;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.core.Aggregated;
import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

public class Aggregated18Test extends Aggregated {
  static {
    AvroUtil.api = ThreadLocal.withInitial(ApiCompatibility18x::new);
  }

  @Test
  public void testAvroVersion() {
    assertThat(AvroVersion.avro_1_9.before("Next major version"), is(true));
    assertThat(AvroVersion.avro_1_8.orAfter("This major version"), is(true));
    assertThat(AvroVersion.getInstalledAvro(), is(AvroVersion.avro_1_8));
  }

  /** Some of the methods tested need to be adapted to Avro 1.8 */
  private static class ApiCompatibility18x extends AvroUtil.ApiCompatibility {

    @Override
    public Schema.Field createField(Schema.Field field, Schema schema) {
      return createFieldOld(field, schema);
    }

    @Override
    public GenericData withJodaTimeConversions(GenericData... models) {
      return withConversions(
          new String[] {
            "org.apache.avro.data.TimeConversions$DateConversion",
            "org.apache.avro.data.TimeConversions$TimeConversion",
            "org.apache.avro.data.TimeConversions$TimeMicrosConversion",
            "org.apache.avro.data.TimeConversions$TimestampConversion",
            "org.apache.avro.data.TimeConversions$TimestampMicrosConversion"
          },
          models);
    }

    @Override
    public GenericData withJavaTimeConversions(GenericData... models) {
      throw new UnsupportedOperationException(
          "No java.time conversions in " + AvroVersion.getInstalledAvro());
    }
  }
}
