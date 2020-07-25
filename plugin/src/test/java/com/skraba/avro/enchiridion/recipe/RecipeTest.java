package com.skraba.avro.enchiridion.recipe;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import com.skraba.avro.enchiridion.simple.SimpleRecord;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for the {@link com.skraba.avro.enchiridion.recipe.Recipe} generated class. */
public class RecipeTest {

  @Test
  public void testSimpleBuilder() {
    Recipe r =
        Recipe.newBuilder()
            .setTitle("Chocolate Puffed Wheat Squares")
            .setIngredients(
                Arrays.asList(
                    new Ingredient(
                        "2L | 8 cups | 120g",
                        "puffed wheat cereal",
                        Collections.emptyList(),
                        Collections.emptyList()),
                    Ingredient.newBuilder()
                        .setQ("75 mL | 0.33 cups | 75g")
                        .setN("butter | margarine")
                        .build(),
                    new Ingredient(
                        "60 mL | 0.25 cups | 50g",
                        "packed brown sugar",
                        Collections.emptyList(),
                        Collections.emptyList()),
                    new Ingredient(
                        "125 mL | 0.5 cups | 120g",
                        "corn syrup",
                        Collections.emptyList(),
                        Collections.emptyList()),
                    new Ingredient(
                        "45 mL | 3 tbsp",
                        "unsweetened cocoa powder",
                        Collections.emptyList(),
                        Collections.emptyList()),
                    new Ingredient(
                        "5 mL | 1 tsp",
                        "vanilla extract",
                        Collections.emptyList(),
                        Collections.emptyList())))
            .setTodo(
                Arrays.asList(
                    "Combine all ingredients but puffed wheat in a saucepan.",
                    "Stir continually over medium heat until boils.",
                    "Boil for 1 minute, then remove from heat.",
                    "Press into buttered 23x23cm pan.",
                    "Pour mixture over puffed wheat and mix well.",
                    "Cool and cut into squares."))
            .build();

    // This should be generated as a test resource.
    assertThat(r, instanceOf(GenericRecord.class));
  }

  @Test
  public void testSimpleRecordConstructor() {
    SimpleRecord sr = new SimpleRecord(123L, "abc");
    assertThat(sr.id, is(123L));
    assertThat(sr.name, is("abc"));
  }
}
