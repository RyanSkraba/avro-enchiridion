package com.skraba.avro.enchiridion.resources

import play.api.libs.json.Json

import scala.reflect.io.Directory

/**
 * Reusable resources for Avro tests.
 */
object AvroTestResources {

  val Base: Directory = Directory(sys.env.getOrElse("AVRO_ENCHIRIDION_REPO_DIR", "/tmp/avro-enchiridion").toString)

  def SimpleRecordWithColumn(recordName: String, fieldName: String, fieldType: Any, fieldDefault: Any): String =
    s"""{
      |  "type" : "record",
      |  "name" : "$recordName",
      |  "fields" : [ {
      |    "name" : "$fieldName",
      |    "type" : "$fieldType",
      |    "type" : "$fieldDefault"
      |  } ]
      |}""".stripMargin

  val SimpleRecord: String =
    """{
      |  "type" : "record",
      |  "name" : "SimpleRecord",
      |  "namespace" : "com.skraba.avro.enchiridion.simple",
      |  "doc" : "Simple two column record",
      |  "fields" : [ {
      |    "name" : "id",
      |    "type" : "long"
      |  }, {
      |    "name" : "name",
      |    "type" : "string"
      |  } ]
      |}""".stripMargin


  val Recipe: String = Json.prettyPrint(Json.obj(
    "type" -> "record",
    "name" -> "Recipe",
    "namespace" -> "com.skraba.avro.enchiridion.recipe",
    "fields" -> Json.arr(
      Json.obj("name" -> "title", "type" -> Json.arr("null", "string")), // nullable
      Json.obj("name" -> "id", "type" -> Json.arr("null", "string")), // nullable
      Json.obj("name" -> "from_id", "type" -> Json.arr("null", "string")), // nullable
      Json.obj("name" -> "source", "type" -> Json.arr("null", "string")), // nullable
      Json.obj("name" -> "note", "type" -> Json.obj("type" -> "array", "items" -> "string")), // list of strings.
      Json.obj("name" -> "makes", "type" -> Json.arr("null", "string")), // nullable
      Json.obj("name" -> "ingredients", "type" -> Json.obj("type" -> "array", "items" ->
        Json.obj("type" -> "record", "name" -> "Ingredient",
          "fields" -> Json.arr(
            Json.obj("name" -> "q", "type" -> Json.arr("null", "string")),
            Json.obj("name" -> "n", "type" -> "string"),
            Json.obj("name" -> "option", "type" -> Json.arr("null", "Ingredient")),
            Json.obj("name" -> "note", "type" -> Json.arr("null", "string"))
          )
        )
      )), // list of strings.
      Json.obj("name" -> "todo", "type" -> Json.obj("type" -> "array", "items" -> "string")), // list of strings.
      Json.obj("name" -> "steps", "type" -> "com.skraba.avro.enchiridion.recipe.Recipe"),
      Json.obj("name" -> "bake", "type" ->
        Json.obj("type" -> "record", "name" -> "Bake",
          "fields" -> Json.arr(
            Json.obj("name" -> "temp", "type" -> Json.arr("null", "string")),
            Json.obj("name" -> "time", "type" -> Json.arr("null", "string")),
            Json.obj("name" -> "note", "type" -> Json.arr("null", "string"))
          )
        )
      ), // list of strings.
    )
  ))

  val Avro1965: String =
    """{
      |  "type" : "record",
      |  "name" : "a",
      |  "namespace" : "default",
      |  "fields" : [ {
      |    "name" : "b",
      |    "type" : {
      |      "type" : "record",
      |      "name" : "b",
      |      "namespace" : "",
      |      "fields" : [ {
      |        "name" : "c",
      |        "type" : {
      |          "type" : "record",
      |          "name" : "c",
      |          "fields" : [ {
      |            "name" : "d",
      |            "type" : "int"
      |          } ]
      |        }
      |      } ]
      |    }
      |  } ]
      |}""".stripMargin


  /** Write all of these schemas to the plugin directory. */
  def main(args: Array[String]) {
    val base: String = sys.env.getOrElse("AVRO_ENCHIRIDION_REPO_DIR", "/tmp/avro-enchiridion")
    val dst = Directory(base).resolve("plugin/src/test/avro/").createDirectory()

    dst.resolve("SimpleRecord.avsc").toFile.writeAll(Json.prettyPrint(Json.parse(SimpleRecord)))
    dst.resolve("Recipe.avsc").toFile.writeAll(Json.prettyPrint(Json.parse(Recipe)))
  }

}
