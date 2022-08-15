package com.skraba.avro.enchiridion.core;

import com.skraba.avro.enchiridion.resources.AvroTestResources;
import com.skraba.avro.enchiridion.testkit.AvroVersion;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
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

  public static ThreadLocal<SampleSchemaUtils> sample =
      ThreadLocal.withInitial(() -> new SampleSchemaUtils(api()));

  /** Get an instance that wraps some Avro SDK methods that have evolved between versions. */
  public static ApiCompatibility api() {
    return api.get();
  }

  /** Get an instance to help create sample schemas. */
  public static SampleSchemaUtils sample() {
    return sample.get();
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

    public Schema.Field createField(String name, Schema schema) {
      return new Schema.Field(name, schema);
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

    public Schema createUnion(Schema... types) {
      return Schema.createUnion(types);
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

    public GenericData withTimeConversions(GenericData... models) {
      if (AvroVersion.avro_1_9.orAfter("Use joda-time unless java time is available"))
        return withJavaTimeConversions(models);
      else return withJodaTimeConversions();
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

  /** Useful classes for generating sample schemas. */
  public static class SampleSchemaUtils {

    private final ApiCompatibility api;

    SampleSchemaUtils(ApiCompatibility api) {
      this.api = api;
    }

    /**
     * Creates a simple schema from a single character.
     *
     * <ul>
     *   <li><b>a</b>: A simple array of longs
     *   <li><b>b</b>: BOOLEAN
     *   <li><b>B</b>: BYTES
     *   <li><b>d</b>: DOUBLE
     *   <li><b>e</b>: A simple enum with three values
     *   <li><b>f</b>: FLOAT
     *   <li><b>F</b>: A simple fixed of length 5
     *   <li><b>i</b>: INT
     *   <li><b>l</b>: LONG
     *   <li><b>m</b>: A simple map of long
     *   <li><b>r</b>: A simple two column record
     *   <li><b>s</b>: STRING
     *   <li><b>u</b>: A union of null and long
     *   <li><b>Anything else</b>: NULL
     * </ul>
     *
     * @param spec The character to map to a schema.
     * @return
     */
    public Schema createSimple(char spec) {
      switch (spec) {
        case 'a':
          return Schema.createArray(createSimple('l'));
        case 'b':
          return Schema.create(Schema.Type.BOOLEAN);
        case 'B':
          return Schema.create(Schema.Type.BYTES);
        case 'd':
          return Schema.create(Schema.Type.DOUBLE);
        case 'e':
          return api().parse(AvroTestResources.SimpleEnum());
        case 'f':
          return Schema.create(Schema.Type.FLOAT);
        case 'F':
          return api().parse(AvroTestResources.SimpleFixed());
        case 'i':
          return Schema.create(Schema.Type.INT);
        case 'l':
          return Schema.create(Schema.Type.LONG);
        case 'm':
          return Schema.createMap(createSimple('l'));
        case 'r':
          return createRecord("A", "l");
        case 's':
          return Schema.create(Schema.Type.STRING);
        case 'u':
          return createUnion(" s");
        default:
          return Schema.create(Schema.Type.NULL);
      }
    }

    /**
     * @param unionSpec A string of characters to be mapped to a schema using {@link
     *     #createSimple(char)}.
     * @return A union of all the schemas mapped from the input spec. If only one character, returns
     *     the simple schema directly (not in a union).
     */
    public Schema createUnion(String unionSpec) {
      if (unionSpec.length() == 1) return createSimple(unionSpec.charAt(0));
      ArrayList<Schema> unionTypes = new ArrayList<>();
      for (char spec : unionSpec.toCharArray()) {
        unionTypes.add(createSimple(spec));
      }
      return Schema.createUnion(unionTypes);
    }

    /**
     * Create a record using the schemas discovered from the string.
     *
     * <p>The simplest case is that there are no
     *
     * @param recordName The name of the record to create
     * @param recordSpec A string of characters, each to be mapped to a schema using {@link
     *     #createSimple(char)}.
     * @param defaults When present (non-null and not none), create a default values for the field.
     * @return A record with one field per schema found in the spec.
     */
    public Schema createRecord(String recordName, String recordSpec, Optional<?>... defaults) {
      // The field prefix is the lower case version of the name
      String[] parts = recordName.split("\\.");
      String fieldNamePrefix = parts[parts.length - 1].toLowerCase();

      // Get the field specs from the record spec
      List<Schema> fieldSchemas =
          (recordSpec.contains("|")
                  ? Arrays.stream(
                      (recordSpec.startsWith("|") ? recordSpec.substring(1) : recordSpec)
                          .split("\\|"))
                  : recordSpec.codePoints().mapToObj(c -> String.valueOf((char) c)))
              .map(this::createUnion)
              .collect(Collectors.toList());

      ArrayList<Schema.Field> fields = new ArrayList<>();
      for (int i = 0; i < fieldSchemas.size(); i++) {
        Object fieldDefault = null;
        if (i < defaults.length && defaults[i] != null && defaults[i].isPresent())
          //noinspection OptionalGetWithoutIsPresent
          fieldDefault = defaults[i].get();
        fields.add(
            api()
                .createField(
                    fieldNamePrefix + i,
                    fieldSchemas.get(i),
                    null,
                    fieldDefault,
                    Schema.Field.Order.ASCENDING));
      }

      return api().createRecord(recordName, null, null, false, fields);
    }
  }
}
