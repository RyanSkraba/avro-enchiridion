package com.skraba.avro.enchiridion.core.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.io.FileMatchers.aFileNamed;
import static org.hamcrest.io.FileMatchers.aFileWithSize;
import static org.hamcrest.io.FileMatchers.anExistingFile;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for Avro files and containers.
 *
 * <p>{@link DataFileWriter}
 */
public class AvroFileTest {

  /**
   * Writes exactly one datum to a file.
   *
   * @param f The file to create or write to.
   * @param model The GenericData model to use for interpreting the datum.
   * @param schema The schema corresponding to a datum.
   * @param datum The actual value to write.
   * @param <T> The type of the datum being written.
   * @throws IOException If there was an error communicating with the file.
   */
  public static <T> void toFile(File f, GenericData model, Schema schema, T datum)
      throws IOException {
    try (DataFileWriter<T> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema, model))) {
      // writer.setCodec(CodecFactory.snappyCodec());
      writer.create(schema, f);
      writer.append(datum);
    }
  }

  /**
   * Read exactly one datum from a file.
   *
   * @param f The file to read from.
   * @param model The GenericData model to use for interpreting the datum.
   * @param <T> The type of the datum being written.
   * @throws IOException If there was an error communicating with the file.
   */
  public static <T> T fromFile(File f, GenericData model) throws IOException {
    // Using a null reader/writer schema will read the
    try (DataFileReader<T> dataFileReader =
        new DataFileReader<>(f, new GenericDatumReader<>(null, null, model))) {
      return dataFileReader.next(null);
    }
  }

  @Test
  public void testRoundTripSerializeIntegerToFile(@TempDir Path tmpDir) throws IOException {
    Schema schema = SchemaBuilder.builder().intType();

    // From an int to the file.
    File f = tmpDir.resolve("integer.avro").toFile();
    toFile(f, GenericData.get(), schema, 1_234_567);

    assertThat(f, anExistingFile());
    assertThat(f, aFileNamed(equalToIgnoringCase("integer.avro")));
    assertThat(f, aFileWithSize(62L));

    Integer datum = fromFile(f, GenericData.get());
    assertThat(datum, is(1_234_567));
  }
}