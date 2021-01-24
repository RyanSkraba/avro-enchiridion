package com.skraba.avro.enchiridion.core19x;

import com.skraba.avro.enchiridion.core.Aggregated;
import com.skraba.avro.enchiridion.core.AvroUtil;
import org.apache.avro.generic.GenericData;

public class Aggregated19Test extends Aggregated {
  static {
    AvroUtil.api = ThreadLocal.withInitial(ApiCompatibility19x::new);
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
