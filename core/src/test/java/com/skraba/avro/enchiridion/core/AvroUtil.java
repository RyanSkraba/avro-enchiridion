package com.skraba.avro.enchiridion.core;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import play.api.libs.json.JsObject;
import play.api.libs.json.Json;

/**
 * Collector of helper methods.
 *
 * <p>No significant logic here, just repetitive methods that can be expressed clearly in a single
 * line.
 */
public class AvroUtil {

  public static ThreadLocal<ApiCompatibility> api = ThreadLocal.withInitial(ApiCompatibility::new);

  /** Get an instance that wraps some Avro SDK methods that have evolved between versions. */
  public static ApiCompatibility api() {
    return api.get();
  }

  /** Print information about a Java BigDecimal. */
  public static String pbd(BigDecimal bd) {
    return "BigDecimal(" + bd.precision() + ":" + bd.scale() + ":" + bd.toString() + ")";
  }

  /** @return the input string as a string, quoting it if it's a {@link CharSequence} */
  public static String jsonify(Object defaultVal) {
    return defaultVal instanceof CharSequence
        ? "\"" + defaultVal + '"'
        : String.valueOf(defaultVal);
  }

  /** @return the input string with all single quotes replaced by double quotes */
  public static String qqify(String in) {
    return in.replace('\'', '"');
  }

  /**
   * This class provides a little indirection to methods that may or may not exist in different
   * versions of the Avro Schema API.
   */
  public static class ApiCompatibility {

    /**
     * See {@link org.apache.avro.Schema#createRecord(java.lang.String, java.lang.String,
     * java.lang.String, boolean, java.util.List)}
     */
    public Schema createRecord(
        String name, String namespace, String doc, boolean isError, List<Schema.Field> fields) {
      return Schema.createRecord(name, namespace, doc, isError, fields);
    }

    public Schema.Field createField(Schema.Field field, Schema schema) {
      return new Schema.Field(field, schema);
    }

    protected Schema.Field createFieldOld(Schema.Field field, Schema schema) {
      // A constructor was added to do this copy automatically in Avro 1.9.x
      Schema.Field f =
          createField(field.name(), schema, field.doc(), field.defaultVal(), field.order());
      for (Map.Entry<? extends String, ?> e : f.getObjectProps().entrySet())
        f.addProp(e.getKey(), e.getValue());
      for (String alias : field.aliases()) f.addAlias(alias);
      return f;
    }

    public Schema.Field createField(String name, Schema schema, String doc, Object defaultValue) {
      return createField(name, schema, doc, defaultValue, Schema.Field.Order.ASCENDING);
    }

    public Schema.Field createField(
        String name, Schema schema, String doc, Object defaultValue, Schema.Field.Order order) {
      return new Schema.Field(name, schema, doc, defaultValue, order);
    }

    public Schema parse(String jsonString) {
      return new Schema.Parser().parse(jsonString);
    }

    public Schema parse(JsObject json) {
      return new Schema.Parser().parse(Json.stringify(json));
    }

    /**
     * Adds {@link Conversion} classes to the given models, using reflection on their names.
     *
     * <p>This is roundabout, but useful so that different versions of Avro can reuse the same tests
     * without enforcing a compile-time dependency on the conversion class. Unfortunately, there's
     * been a bit of churn in the names of these classes since they were introduced in Avro 1.8.x.
     *
     * @param conversionClasses A list of class names to be instantiated and added to the models.
     * @param models A list of models to add the conversions to.
     * @return The first model in the list. If the list is empty or null, a new GenericData with the
     *     given conversions.
     */
    public GenericData withConversions(String[] conversionClasses, GenericData... models) {
      if (models == null || models.length == 0) models = new GenericData[] {new GenericData()};
      for (String cnv : conversionClasses)
        try {
          for (GenericData mdl : models)
            mdl.addLogicalTypeConversion((Conversion<?>) Class.forName(cnv).newInstance());
        } catch (IllegalAccessException | ClassNotFoundException | InstantiationException e) {
          // This shouldn't happen and is unrecoverable.
          throw new RuntimeException(e);
        }
      return models[0];
    }

    /**
     * Add the decimal conversions to the models.
     *
     * @param models A list of models to add the conversions to.
     * @return The first model in the list. If the list is empty or null, a new GenericData with the
     *     given conversions.
     */
    public GenericData withDecimalConversions(GenericData... models) {
      return withConversions(
          new String[] {"org.apache.avro.Conversions$DecimalConversion"}, models);
    }

    /**
     * Add any date/time conversions to the models that use joda-time classes.
     *
     * @param models A list of models to add the conversions to.
     * @return The first model in the list. If the list is empty or null, a new GenericData with the
     *     given conversions.
     */
    public GenericData withJodaTimeConversions(GenericData... models) {
      // This does nothing, since there are no longer these conversions in the system
      throw new UnsupportedOperationException("No joda-time in " + AvroVersion.getInstalledAvro());
    }

    /**
     * Add the decimal conversions to the models.
     *
     * @param models A list of models to add the conversions to.
     * @return The first model in the list. If the list is empty or null, a new GenericData with the
     *     given conversions.
     */
    public GenericData withJavaTimeConversions(GenericData... models) {
      return withConversions(
          new String[] {
            "org.apache.avro.data.TimeConversions$DateConversion",
            "org.apache.avro.data.TimeConversions$LocalTimestampMicrosConversion",
            "org.apache.avro.data.TimeConversions$LocalTimestampMillisConversion",
            "org.apache.avro.data.TimeConversions$TimeMicrosConversion",
            "org.apache.avro.data.TimeConversions$TimeMillisConversion",
            "org.apache.avro.data.TimeConversions$TimestampMicrosConversion",
            "org.apache.avro.data.TimeConversions$TimestampMillisConversion"
          },
          models);
    }
  }
}
