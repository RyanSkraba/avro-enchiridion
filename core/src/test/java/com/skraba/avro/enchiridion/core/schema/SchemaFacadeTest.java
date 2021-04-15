package com.skraba.avro.enchiridion.core.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.skraba.avro.enchiridion.core.AvroUtil;
import com.skraba.avro.enchiridion.core.SerializeToBytesTest;
import com.skraba.avro.enchiridion.resources.AvroTestResources;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.RandomData;
import org.junit.jupiter.api.Test;

/**
 * Provide a facade over an existing data to make it appear that it has a different schema. From an
 * external point of view, the existing instances will be indistinguishable as if they were created
 * with the desired schema.
 *
 * <p>This is accomplished using a {@link SchemaFacade} that takes an instance and a schema, and
 * returns an instance of the same type, where all of the methods are delegated internally while
 * ensuring that any schema information remains consistently rewritten with the new schema.
 *
 * <p>For example, a {@link Schema.Type#ARRAY} takes a List<> datum. When rewriting the schema on
 * the list, all of the internal instances also have to have their schemas rewritten.
 *
 * <p>For simple types, or flattish records, this can be very efficient: creating one facade
 * instance over top of the existing data. The facade instances are "lazy" and created on every
 * access demand.
 *
 * <p>For deeply, deeply nested, complicated schemas, or data that is frequently read, it would be
 * worthwhile caching the facades internally.
 *
 * <p>The facades are not modifiable.
 */
public class SchemaFacadeTest {

  public static final Schema SIMPLE = AvroUtil.api().parse(AvroTestResources.SimpleRecord());

  public static final Schema SIMPLE_ANNOTATED =
      AvroUtil.api().parse(AvroTestResources.SimpleRecord());

  static {
    SIMPLE_ANNOTATED.addProp("ann1record", 1);
    SIMPLE_ANNOTATED.addProp("ann2record", "A");
    SIMPLE_ANNOTATED.getField("id").addProp("ann1id", "B");
    SIMPLE_ANNOTATED.getField("id").schema().addProp("ann1idSchema", "C");
    SIMPLE_ANNOTATED.getField("name").addProp("ann1name", "D");
    SIMPLE_ANNOTATED.getField("name").schema().addProp("ann1nameSchema", "E");
  }

  /**
   * Helper method to add a bunch of test annotations to a schema. This modifies the schema in
   * place, so be sure to use a deep copy if you need the original schema.
   */
  public static Schema addSomeAnnotations(
      Set<Schema> visited, Schema schema, String name, Object value) {

    if (!visited.contains(schema)) {
      schema.addProp(name, value);
      visited.add(schema);

      // Complex types that can be annotated should be wrapped.
      switch (schema.getType()) {
        case RECORD:
          for (Schema.Field f : schema.getFields()) {
            f.addProp(name, value);
            addSomeAnnotations(visited, f.schema(), name + "." + f.name(), value);
          }
          break;
        case ARRAY:
          addSomeAnnotations(visited, schema.getElementType(), name + ".element", value);
          break;
        case MAP:
          addSomeAnnotations(visited, schema.getValueType(), name + ".value", value);
          break;
        case UNION:
          for (int i = 0; i < schema.getTypes().size(); i++)
            addSomeAnnotations(visited, schema.getTypes().get(i), name + "[" + i + "]", value);
          break;
      }
    }
    return schema;
  }

  @Test
  public void wrapSimpleRecord() {
    // The original record.
    GenericRecord original =
        new GenericRecordBuilder(SIMPLE).set("id", 0L).set("name", "one").build();

    // Rewrap the record with an annotated schema.
    GenericRecord facade = SchemaFacade.of(original, SIMPLE_ANNOTATED);

    assertThat(facade.getSchema(), sameInstance(SIMPLE_ANNOTATED));

    assertTrue(GenericData.get().validate(SIMPLE_ANNOTATED, facade));
  }

  @Test
  public void wrapComplexRecord() {

    Schema original = AvroUtil.api().parse(AvroTestResources.Recipe());
    Schema annotated =
        addSomeAnnotations(
            new HashSet<>(), AvroUtil.api().parse(AvroTestResources.Recipe()), ".", true);

    for (Object record : new RandomData(original, 100, 0L)) {
      // Create a facade on the original record.
      GenericRecord facade = SchemaFacade.of((GenericRecord) record, annotated);

      // Verify that it has the expected schema at the top level, and that all of the data is valid
      assertThat(facade.getSchema(), sameInstance(annotated));
      assertTrue(GenericData.get().validate(annotated, facade));

      // Ensure that the facade is considered identical to a datum provided by a round trip.
      GenericRecord roundTrip =
          SerializeToBytesTest.roundTripBytes(GenericData.get(), annotated, facade);
      assertThat(GenericData.get().compare(roundTrip, facade, annotated), is(0));
    }
  }

  /** This is the factory for all of the facades and wrappers. */
  public static class SchemaFacade {

    /**
     * Wrap the given datum to conform to the given schema.
     *
     * @param datum The datum to wrap.
     * @param schema The desired schema for the datum.
     * @param <T> The type of datum being passed in.
     * @return Either the original or a facade of the datum presenting the desired schema.
     */
    public static <T> T of(T datum, Schema schema) {

      // Complex types that can be annotated should be wrapped.
      switch (schema.getType()) {
        case RECORD:
          if (datum instanceof GenericRecord)
            return (T) new GenericRecordFacade((GenericRecord) datum, schema);
          break;

        case ARRAY:
          if (datum instanceof List)
            return (T)
                new FunctionMappedList<T, T>(
                    (List) datum,
                    t -> SchemaFacade.of(t, schema.getElementType()),
                    SchemaFacade::unwrap);
          break;

        case MAP:
          if (datum instanceof Map)
            return (T)
                new FunctionMappedValueMap<String, T, T>(
                    (Map) datum,
                    t -> SchemaFacade.of(t, schema.getValueType()),
                    SchemaFacade::unwrap);
          break;

        case ENUM:
          if (datum instanceof GenericEnumSymbol)
            return (T) new GenericEnumFacade((GenericEnumSymbol) datum, schema);
          break;

        case FIXED:
          if (datum instanceof GenericFixed)
            return (T) new GenericFixedFacade((GenericFixed) datum, schema);

        case UNION:
          int branch = GenericData.get().resolveUnion(schema, datum);
          return of(datum, schema.getTypes().get(branch));

        default:
          break;
      }

      // Primitive and type mismatches can only be returned without modification.
      return datum;
    }

    public static <T> T unwrap(T facade) {
      if (facade instanceof WrapsDelegate) return ((WrapsDelegate<T>) facade).getDelegate();
      else return facade;
    }

    /** Internally, a wrapper or facade can get the original instance. */
    private interface WrapsDelegate<T> {
      T getDelegate();
    }

    /**
     * The facade over the {@link GenericRecord} returns the new schema, but also puts a facade over
     * all of its fields.
     */
    private static class GenericRecordFacade
        implements GenericRecord, WrapsDelegate<GenericRecord> {
      final GenericRecord dlg;
      final Schema recordSchema;

      public GenericRecordFacade(GenericRecord dlg, Schema recordSchema) {
        this.dlg = dlg;
        this.recordSchema = recordSchema;
      }

      @Override
      public void put(String key, Object v) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object get(String key) {
        return SchemaFacade.of(dlg.get(key), recordSchema.getField(key).schema());
      }

      @Override
      public void put(int i, Object v) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object get(int i) {
        return SchemaFacade.of(dlg.get(i), recordSchema.getFields().get(i).schema());
      }

      @Override
      public Schema getSchema() {
        return recordSchema;
      }

      @Override
      public GenericRecord getDelegate() {
        return dlg;
      }
    }

    /** The facade over the {@link GenericEnumSymbol} just returns the modified schema. */
    private static class GenericEnumFacade<E extends GenericEnumSymbol<E>>
        implements GenericEnumSymbol<E>, WrapsDelegate<GenericEnumSymbol<E>> {
      final GenericEnumSymbol<E> dlg;
      final Schema enumSchema;

      public GenericEnumFacade(GenericEnumSymbol<E> delegate, Schema enumSchema) {
        this.dlg = delegate;
        this.enumSchema = enumSchema;
      }

      @Override
      public Schema getSchema() {
        return enumSchema;
      }

      @Override
      public int compareTo(E o) {
        return dlg.compareTo(o);
      }

      @Override
      public GenericEnumSymbol<E> getDelegate() {
        return dlg;
      }
    }

    /** The facade over the {@link GenericFixed} just returns the modified schema. */
    private static class GenericFixedFacade implements GenericFixed, WrapsDelegate<GenericFixed> {
      final GenericFixed dlg;
      final Schema fixedSchema;

      public GenericFixedFacade(GenericFixed delegate, Schema fixedSchema) {
        this.dlg = delegate;
        this.fixedSchema = fixedSchema;
      }

      @Override
      public Schema getSchema() {
        return fixedSchema;
      }

      @Override
      public byte[] bytes() {
        return dlg.bytes();
      }

      @Override
      public GenericFixed getDelegate() {
        return dlg;
      }
    }

    /**
     * Provides an {@link Iterator} that wraps another, transparently applying a {@link Function} to
     * all of its values.
     *
     * @param <InT> The (hidden) type of the values in the wrapped iterator.
     * @param <OutT> The (visible) type of the values in this iterator.
     */
    public static class FunctionMappedIterator<InT, OutT>
        implements Iterator<OutT>, WrapsDelegate<Iterator<InT>> {

      private final Iterator<InT> dlg;

      private final Function<InT, OutT> fn;

      FunctionMappedIterator(Iterator<InT> dlg, Function<InT, OutT> fn) {
        this.dlg = dlg;
        this.fn = fn;
      }

      @Override
      public boolean hasNext() {
        return dlg.hasNext();
      }

      @Override
      public OutT next() {
        return fn.apply(dlg.next());
      }

      @Override
      public Iterator<InT> getDelegate() {
        return dlg;
      }
    }

    /**
     * Provides a unmodifiable {@link Set} that wraps another, transparently applying a {@link
     * Function} to all of its values.
     *
     * @param <InT> The (hidden) type of the values in the wrapped set.
     * @param <OutT> The (visible) type of the values in this set.
     */
    public static class FunctionMappedSet<InT, OutT> extends AbstractSet<OutT>
        implements WrapsDelegate<Set<InT>> {

      private final Set<InT> dlg;

      private final Function<InT, OutT> fn;

      private final Function<OutT, InT> fnOut;

      FunctionMappedSet(Set<InT> dlg, Function<InT, OutT> fn, Function<OutT, InT> fnOut) {
        this.dlg = dlg;
        this.fn = fn;
        this.fnOut = fnOut;
      }

      @Override
      public Iterator<OutT> iterator() {
        return new FunctionMappedIterator<>(dlg.iterator(), fn);
      }

      @Override
      public int size() {
        return dlg.size();
      }

      @Override
      @SuppressWarnings("unchecked")
      public boolean contains(Object o) {
        if (fnOut != null) return dlg.contains(fnOut.apply((OutT) o));
        else {
          for (OutT t : this) {
            if (t != null && t.equals(o)) return true;
            else if (t == null && o == null) return true;
          }
          return false;
        }
      }

      @Override
      public Set<InT> getDelegate() {
        return dlg;
      }
    }

    /**
     * Provides a {@link List} that wraps another, transparently applying a {@link Function} to all
     * of its values.
     *
     * @param <InT> The (hidden) type of the values in the wrapped list.
     * @param <OutT> The (visible) type of the values in this list.
     */
    public static class FunctionMappedList<InT, OutT> extends AbstractList<OutT>
        implements WrapsDelegate<List<InT>> {

      private final List<InT> dlg;

      private final Function<InT, OutT> fn;

      private final Function<OutT, InT> fnOut;

      FunctionMappedList(List<InT> dlg, Function<InT, OutT> fn, Function<OutT, InT> fnOut) {
        this.dlg = dlg == null ? new ArrayList<>(0) : dlg;
        this.fn = fn;
        this.fnOut = fnOut;
      }

      @Override
      public OutT get(int index) {
        return fn.apply(dlg.get(index));
      }

      @SuppressWarnings("unchecked")
      @Override
      public boolean contains(Object o) {
        if (fnOut != null) return dlg.contains(fnOut.apply((OutT) o));
        else {
          for (OutT t : this) {
            if (t != null && t.equals(o)) return true;
            else if (t == null && o == null) return true;
          }
          return false;
        }
      }

      @Override
      public int size() {
        return dlg.size();
      }

      @Override
      public List<InT> getDelegate() {
        return dlg;
      }
    }

    /**
     * Provides an unmodifiable {@link Map} that wraps another, transparently applying a {@link
     * Function} to all of its values.
     *
     * @param <KeyT> The type of the key in the map.
     * @param <InT> The (hidden) type of the values in the wrapped map.
     * @param <OutT> The (visible) type of the values in this map.
     */
    public static class FunctionMappedValueMap<KeyT, InT, OutT> extends AbstractMap<KeyT, OutT>
        implements WrapsDelegate<Map<KeyT, InT>> {

      private final Map<KeyT, InT> dlg;

      private final Function<InT, OutT> fn;

      private final Function<OutT, InT> fnOut;

      FunctionMappedValueMap(
          Map<KeyT, InT> dlg, Function<InT, OutT> fn, Function<OutT, InT> fnOut) {
        this.dlg = dlg;
        this.fn = fn;
        this.fnOut = fnOut;
      }

      @Override
      public boolean containsKey(Object key) {
        return dlg.containsKey(key);
      }

      @SuppressWarnings("unchecked")
      @Override
      public boolean containsValue(Object value) {
        return dlg.containsValue(fnOut.apply((OutT) value));
      }

      @Override
      public Set<KeyT> keySet() {
        return dlg.keySet();
      }

      @Override
      public OutT get(Object key) {
        return fn.apply(dlg.get(key));
      }

      @Override
      public Set<Map.Entry<KeyT, OutT>> entrySet() {
        Set<Map.Entry<KeyT, InT>> in = dlg.entrySet();
        return new FunctionMappedSet<>(
            in,
            t -> new FunctionMappedMapEntry<>(t, fn),
            t -> ((FunctionMappedMapEntry<KeyT, InT, OutT>) t).getDelegate());
      }

      @Override
      public Map<KeyT, InT> getDelegate() {
        return dlg;
      }
    }

    /**
     * Provides an unmodifiable {@link Map} that wraps another, transparently applying a {@link
     * Function} to all of its values.
     *
     * @param <KeyT> The type of the key in the map.
     * @param <InT> The (hidden) type of the values in the wrapped map.
     * @param <OutT> The (visible) type of the values in this map.
     */
    public static class FunctionMappedMapEntry<KeyT, InT, OutT>
        implements Map.Entry<KeyT, OutT>, WrapsDelegate<Map.Entry<KeyT, InT>> {

      private final Map.Entry<KeyT, InT> dlg;

      private final Function<InT, OutT> fn;

      public FunctionMappedMapEntry(Map.Entry<KeyT, InT> dlg, Function<InT, OutT> fn) {
        this.dlg = dlg;
        this.fn = fn;
      }

      @Override
      public KeyT getKey() {
        return dlg.getKey();
      }

      @Override
      public OutT getValue() {
        return fn.apply(dlg.getValue());
      }

      @Override
      public OutT setValue(OutT value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public int hashCode() {
        // The hashCode() for Map.Entry is defined as this:
        return (getKey() == null ? 0 : getKey().hashCode())
            ^ (getValue() == null ? 0 : getValue().hashCode());
      }

      @Override
      public boolean equals(Object other) {
        if (other instanceof Map.Entry<?, ?>) {
          Map.Entry<?, ?> e2 = (Map.Entry<?, ?>) other;
          return (getKey() == null ? e2.getKey() == null : getKey().equals(e2.getKey()))
              && (getValue() == null ? e2.getValue() == null : getValue().equals(e2.getValue()));
        }
        return false;
      }

      @Override
      public Map.Entry<KeyT, InT> getDelegate() {
        return dlg;
      }
    }
  }
}
