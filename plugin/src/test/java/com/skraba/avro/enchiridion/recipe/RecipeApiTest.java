package com.skraba.avro.enchiridion.recipe;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;

/** Unit tests for the {@link RecipeApi} API built on top of the {@link Recipe} record. */
public class RecipeApiTest {

  /**
   * @return A simple recipe that is a flat list of ingredients and steps.
   */
  public Recipe getPuffedWheatSquares() {
    return RecipeApi.of("Chocolate Puffed Wheat Squares")
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
        .addNote("")
        .build();
  }

  /**
   * @return A more complex recipe with multiple steps.
   */
  public Recipe getBestChocolateSheetCakeEver() {
    return RecipeApi.of("Best Chocolate Sheet Cake Ever")
        .withSource("http://bakingdom.com/2011/02/chocolate-sheet-cake.html")
        .withMakes("2 x 24cm round cake tins (shallow)")
        .addStepWithId("butter")
        .addSimpleIngredient("227g", "butter")
        .addTodo("Melt in saucepan.")
        .addTodo("Remove from heat.")
        .build() // end step butter
        .addStepWithId("liquid", "butter")
        .addSimpleIngredient("70g", "cocoa")
        .addSimpleIngredient("237g", "hot water")
        .addIngredient("440g", "sugar")
        .addNote("240g is alright for low-sugar, especially with sweet icing.")
        .build()
        .addTodo("Mix cocoa, then others.")
        .build() // end step liquid
        .addStepWithId("dry")
        .addSimpleIngredient("313g", "flour")
        .addSimpleIngredient("1 tsp", "baking soda")
        .addSimpleIngredient("0.25 tsp", "salt")
        .addTodo("Whisk together.")
        .build() // end step dry
        .addStepWithId("liquid2", "dry", "liquid")
        .addTodo("Whisk together.")
        .build() // end step liquid2
        .addStepWithId("buttermilk")
        .addSimpleIngredient("145g", "buttermilk")
        .addSimpleIngredient("2", "eggs")
        .addSimpleIngredient("1 tsp", "vanilla")
        .addTodo("Gently whisk.")
        .build() // end step buttermilk
        .addStepWithId("liquid3", "buttermilk", "liquid2")
        .addTodo("Slowly pour chocolate into eggs, whisking constantly.")
        .build() // end step liquid3
        .withBake()
        .withTempCelsius("180")
        .withTime("20-25 minutes")
        .build()
        .build();
  }

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

  /** Test building the baking instructions. */
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
  public void testSimpleRecipe() {
    Recipe r = getPuffedWheatSquares();
    assertThat(r, instanceOf(SpecificRecord.class));
  }

  @Test
  public void testComplexRecipe() throws IOException {
    Recipe r = getBestChocolateSheetCakeEver();
    assertThat(r, instanceOf(SpecificRecord.class));
    assertThat(r.getSteps(), hasSize(6));

    // Like all avro data, this can be written to a JSON string.
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      Encoder encoder = EncoderFactory.get().jsonEncoder(r.getSchema(), baos, false);
      DatumWriter<Recipe> w = new SpecificDatumWriter<>(r.getSchema());
      w.write(r, encoder);
      encoder.flush();

      String json = new String(baos.toByteArray(), StandardCharsets.UTF_8);
      assertThat(json, containsString("saucepan"));
    }
  }
}
