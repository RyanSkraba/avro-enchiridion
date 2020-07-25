package com.skraba.avro.enchiridion.recipe;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for the {@link RecipeApi} API built on top of the {@link Recipe} record. */
public class RecipeApiTest {

  /** Test building a nested ingredient. */
  @Test
  public void testIngredientOption() {
    Ingredient i =
        RecipeApi.IngredientApi.of("75 mL | 0.33 cups | 75g", "butter")
            .withOption(null, "margarine")
            .build()
            .build();

    assertThat(i, instanceOf(GenericRecord.class));
    assertThat(i.q.toString(), is("75 mL | 0.33 cups | 75g"));
    assertThat(i.n.toString(), is("butter"));
    assertThat(i.note, empty());
    assertThat(i.option, hasSize(1));
    assertThat(i.option.get(0).q, nullValue());
    assertThat(i.option.get(0).n.toString(), is("margarine"));
    assertThat(i.option.get(0).note, empty());
    assertThat(i.option.get(0).option, empty());
  }

  /** Test building a nested ingredient. */
  @Test
  public void testBake() {
    // Without any info.
    Bake b = RecipeApi.BakeApi.of().build();
    assertThat(b, instanceOf(GenericRecord.class));
    assertThat(b.temp, nullValue());
    assertThat(b.time, nullValue());
    assertThat(b.note, nullValue());

    Bake b2 =
        RecipeApi.BakeApi.of()
            .withTemp("180 C")
            .withTime("20-25 minutes")
            .withNote("Check at 15 minutes")
            .build();
    assertThat(b2, instanceOf(GenericRecord.class));
    assertThat(b2.temp, is("180 C"));
    assertThat(b2.time, is("20-25 minutes"));
    assertThat(b2.note, is("Check at 15 minutes"));

    Bake b3 = RecipeApi.BakeApi.of().withTempCelsius("180").build();
    assertThat(b3, instanceOf(GenericRecord.class));
    assertThat(b3.temp, is("180°C"));
    assertThat(b3.time, nullValue());
    assertThat(b3.note, nullValue());
  }

  @Test
  public void testHelpfulBuilder() {

    Recipe r =
        RecipeApi.of("Chocolate Puffed Wheat Squares")
            .addIngredient("2L | 8 cups | 120g", "puffed wheat cereal")
            .build()
            .addIngredient("75 mL | 0.33 cups | 75g", "butter | margarine")
            .build()
            .addIngredient("60 mL | 0.25 cups | 50g", "packed brown sugar")
            .build()
            .addIngredient("125 mL | 0.5 cups | 120g", "corn syrup")
            .build()
            .addIngredient("45 mL | 3 tbsp", "unsweetened cocoa powder")
            .build()
            .addIngredient("5 mL | 1 tsp", "vanilla extract")
            .build()
            .addTodo("Combine all ingredients but puffed wheat in a saucepan.")
            .addTodo("Stir continually over medium heat until boils.")
            .addTodo("Boil for 1 minute, then remove from heat.")
            .addTodo("Press into buttered 23x23cm pan.")
            .addTodo("Pour mixture over puffed wheat and mix well.")
            .addTodo("Cool and cut into squares.")
            .build();

    // This should be generated as a test resource.
    assertThat(r, instanceOf(GenericRecord.class));
  }
}
