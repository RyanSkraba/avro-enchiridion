package com.skraba.avro.enchiridion.plugin;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.idl.CustomAnnotation;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

/** Unit tests related to using annotations in the IDL. */
public class CustomAnnotationTest {

  @Test
  public void testBasic() {
    Schema s = CustomAnnotation.getClassSchema();

    Map<String, Object> props = s.getField("a1").getObjectProps();
    assertThat(props.get("MyAnnotation"), is("three"));
    assertThat(props.get("MyAnnotationArray"), is(Arrays.asList("four", "five")));

    props = s.getField("a1").schema().getObjectProps();
    assertThat(props.get("MyAnnotation"), is("one"));
    assertThat(props.get("MyAnnotationArray"), is(Arrays.asList("two", "three")));
  }
}
