package com.skraba.avro.enchiridion.core.extra;

/** Example test class that we want to prevent from being instantiated or used. */
class Danger {
  /** Field that doesn't have an accessor */
  private String sensitive = "data";

  public Danger() {
    // This constructor does something dangerous, and we don't want it to be called.
    throw new SecurityException("rm -rf EVERYTHING! (" + sensitive + ")");
  }
}
