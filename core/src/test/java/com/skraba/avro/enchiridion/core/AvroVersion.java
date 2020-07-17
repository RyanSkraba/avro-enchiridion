package com.skraba.avro.enchiridion.core;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import org.apache.avro.Schema;

/**
 * A little utility for detecting the current Avro version.
 *
 * <pre>
 * if (AvroVersion.avro_1_8.orAfter()) {
 *   // Code to run if the version of Avro is 1.8.x or later.
 * } else {
 *   // Code to run if the version of Avro is 1.7.x or earlier.
 * }
 * </pre>
 */
public enum AvroVersion {
  avro_1_7,
  avro_1_8,
  avro_1_9,
  avro_1_10,
  avro_1_11,
  /** An avro version far, far off in the future. */
  never;

  /** Lazy auto-detected value of the installed Avro library. */
  private static final ThreadLocal<AvroVersion> installedAvro = new ThreadLocal<>();

  /** Accessor for the Avro library. */
  public static AvroVersion getInstalledAvro() {
    try {
      if (installedAvro.get() == null) {
        String avroJar =
            Paths.get(Schema.class.getProtectionDomain().getCodeSource().getLocation().toURI())
                .getFileName()
                .toString();
        for (AvroVersion v : AvroVersion.values()) {
          // The Avro jar always starts with avro-1.x.y
          String versionString = v.toString().replaceFirst("_", "-").replaceAll("_", ".");
          if (avroJar.startsWith(versionString)) {
            installedAvro.set(v);
            break;
          }
        }
      }
      return installedAvro.get();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Unexpected test deployment.  Where is your Avro jar?", e);
    }
  }

  public boolean orAfter() {
    return getInstalledAvro().compareTo(this) >= 0;
  }
}
