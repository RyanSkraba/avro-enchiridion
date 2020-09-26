package com.skraba.avro.enchiridion.recipe;

import java.util.ArrayList;

/**
 * Builds a slightly different style of fluent API on top of a {@link Recipe}.
 *
 * @param <T> The type of object that should be returned by the {@link #build()} method.
 */
public class RecipeApi<T> {

  /**
   * If building a {@link Recipe} nested inside another, this is the parent builder. Otherwise it is
   * the same instance as {@link #self}.
   */
  private final T parent;

  /** The {@link Recipe} currently being built. */
  private final Recipe self;

  private RecipeApi(T parent, Recipe self) {
    this.parent = parent;
    this.self = self;
  }

  /**
   * Start building a recipe with the required attribute
   *
   * @param title The title of the recipe.
   * @return The builder for the recipe.
   */
  public static RecipeApi<Recipe> of(String title) {
    Recipe self = Recipe.newBuilder().setTitle(title).build();
    return new RecipeApi<>(self, self);
  }

  public RecipeApi<T> withStepId(String stepId) {
    self.setStepId(stepId);
    return this;
  }

  public RecipeApi<T> addFromStepId(String fromStepId) {
    return addFromStepId(1.0, fromStepId);
  }

  public RecipeApi<T> addFromStepId(double fraction, String fromStepId) {
    self.getFromStepId()
        .add(FromStep.newBuilder().setFraction(fraction).setStepId(fromStepId).build());
    return this;
  }

  public RecipeApi<T> withSource(String source) {
    self.setSource(source);
    return this;
  }

  public RecipeApi<T> withMakes(String makes) {
    self.setMakes(makes);
    return this;
  }

  public RecipeApi<T> addNote(String note) {
    self.getNote().add(note);
    return this;
  }

  public RecipeApi<T> addSimpleIngredient(String q, String n) {
    return addIngredient(q, n).build();
  }

  public IngredientApi<RecipeApi<T>> addIngredient(String q, String n) {
    return new IngredientApi<>(this, new Ingredient(q, n, new ArrayList<>(), new ArrayList<>()));
  }

  public RecipeApi<T> addIngredient(Ingredient ingredient) {
    self.getIngredients().add(ingredient);
    return this;
  }

  public RecipeApi<RecipeApi<T>> addStepWithId(String stepId, String... fromStepIds) {
    Recipe.Builder self = Recipe.newBuilder().setStepId(stepId);
    if (fromStepIds.length > 0) {
      self.setFromStepId(new ArrayList<>());
      for (String id : fromStepIds) self.getFromStepId().add(new FromStep(1.0, id));
    }
    return new RecipeApi<>(this, self.build());
  }

  public RecipeApi<T> addStep(Recipe step) {
    self.getSteps().add(step);
    return this;
  }

  public RecipeApi<T> addTodo(String todo) {
    self.getTodo().add(todo);
    return this;
  }

  public BakeApi<RecipeApi<T>> withBake() {
    return new BakeApi<>(this, new Bake());
  }

  public T build() {
    if (parent instanceof RecipeApi) {
      ((RecipeApi<?>) parent).self.getSteps().add(self);
    }

    return parent;
  }

  /** Helper methods and builders for an {@link Ingredient}. */
  public static class IngredientApi<T> {

    /**
     * If building a {@link Ingredient} nested inside another, this is the parent builder. Otherwise
     * it is the same instance as {@link #self}.
     */
    private final T parent;

    /** The {@link Ingredient} currently being built. */
    private final Ingredient self;

    private IngredientApi(T parent, Ingredient self) {
      this.parent = parent;
      this.self = self;
    }

    public static IngredientApi<Ingredient> of(String q, String n) {
      Ingredient self = new Ingredient(q, n, new ArrayList<>(), new ArrayList<>());
      return new IngredientApi<>(self, self);
    }

    public IngredientApi<T> addOption(Ingredient option) {
      self.getOption().add(option);
      return this;
    }

    public IngredientApi<IngredientApi<T>> withOption(String q, String n) {
      return new IngredientApi<>(this, new Ingredient(q, n, new ArrayList<>(), new ArrayList<>()));
    }

    public IngredientApi<T> addNote(String note) {
      self.getNote().add(note);
      return this;
    }

    public T build() {
      if (parent instanceof RecipeApi.IngredientApi) {
        ((IngredientApi<?>) parent).addOption(self);
      }

      if (parent instanceof RecipeApi) {
        ((RecipeApi<?>) parent).addIngredient(self);
      }

      return parent;
    }
  }

  /** Helper methods and builders for an {@link Bake}. */
  public static class BakeApi<T> {

    /**
     * If building a {@link Bake} nested inside another, this is the parent builder. Otherwise it is
     * the same instance as {@link #self}.
     */
    private final T parent;

    /** The {@link Bake} currently being built. */
    private final Bake self;

    private BakeApi(T parent, Bake self) {
      this.parent = parent;
      this.self = self;
    }

    public static BakeApi<Bake> of() {
      Bake self = new Bake(null, null, null);
      return new BakeApi<>(self, self);
    }

    public BakeApi<T> withTemp(String temp) {
      self.setTemp(temp);
      return this;
    }

    public BakeApi<T> withTempCelsius(String temp) {
      self.setTemp(temp + "°C");
      return this;
    }

    public BakeApi<T> withTime(String time) {
      self.setTime(time);
      return this;
    }

    public BakeApi<T> withNote(String note) {
      self.setNote(note);
      return this;
    }

    public T build() {
      if (parent instanceof RecipeApi) {
        ((RecipeApi<?>) parent).self.setBake(self);
      }

      return parent;
    }
  }
}
