package com.skraba.avro.enchiridion.core18x;

import com.skraba.avro.enchiridion.core.Aggregated;
import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.core.AvroVersion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class Aggregated18Test extends Aggregated {
  static {
    AvroUtil.api = ThreadLocal.withInitial(ApiCompatibility18x::new);
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
