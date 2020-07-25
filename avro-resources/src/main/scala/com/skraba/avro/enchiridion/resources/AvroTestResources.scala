package com.skraba.avro.enchiridion.resources

import play.api.libs.json.{JsObject, JsString, JsValue, Json}

import scala.reflect.io.Directory

/**
  * Reusable resources for Avro tests.
  */
object AvroTestResources {

  val Base: Directory = Directory(
    sys.env
      .getOrElse("AVRO_ENCHIRIDION_REPO_DIR", "/tmp/avro-enchiridion")
      .toString
  )

  /**
    * @return a JSON object with name, doc and type attributes, useful in Avro field arrays.
    */
  private[this] def field(name: String,
                          doc: String,
                          fieldType: JsValue = JsString("string")): JsObject =
    Json.obj("name" -> name, "doc" -> doc, "type" -> fieldType)

  /**
    * @return a JSON object with name, doc and type attributes, useful in Avro field arrays.
    *         The type of the field is automatically unioned with "null".
    */
  private[this] def fieldOpt(
    name: String,
    doc: String,
    fieldType: JsValue = JsString("string")
  ): JsObject =
    Json.obj(
      "name" -> name,
      "doc" -> doc,
      "type" -> Json.arr("null", fieldType),
      "default" -> null
    )

  /**
    * @return a JSON object with name, doc and type attributes, useful in Avro field arrays.
    *         The type of the field is an array of with the given itemType.
    */
  private[this] def fieldArray(
    name: String,
    doc: String,
    itemType: JsValue = JsString("string")
  ): JsObject =
    Json.obj(
      "name" -> name,
      "doc" -> doc,
      "type" -> Json.obj("type" -> "array", "items" -> itemType),
      "default" -> Json.arr()
    )

  def SimpleRecordWithColumn(recordName: String,
                             fieldName: String,
                             fieldType: Any,
                             fieldDefault: Any): String =
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

  val Recipe: String = Json.prettyPrint(
    Json.obj(
      "type" -> "record",
      "name" -> "Recipe",
      "namespace" -> "com.skraba.avro.enchiridion.recipe",
      "doc" -> "A recipe containing ingredients and steps.",
      "fields" -> Json.arr(
        fieldOpt("title", "Recipe title"),
        fieldOpt("step_id", "A unique tag for this step"),
        fieldArray("from_step_id", "Recipe title"),
        fieldOpt("source", "Where it came from (person, website, book)"),
        fieldOpt("makes", "How much the recipe makes"),
        fieldArray("note", "Free text (information, hints)"),
        Json.obj(
          "name" -> "ingredients",
          "type" -> Json.obj(
            "type" -> "array",
            "items" ->
              Json.obj(
                "type" -> "record",
                "name" -> "Ingredient",
                "fields" -> Json.arr(
                  fieldOpt("q", "Quantity"),
                  fieldOpt("n", "Name"),
                  fieldArray("option", "Options", JsString("Ingredient")),
                  fieldArray("note", "Free text (information, hints)"),
                )
              )
          ),
          "default" -> Json.arr()
        ),
        fieldArray("todo", "Steps"),
        fieldArray(
          "steps",
          "Subrecipes",
          JsString("com.skraba.avro.enchiridion.recipe.Recipe")
        ),
        Json.obj(
          "name" -> "bake",
          "type" -> Json.arr(
            "null",
            Json.obj(
              "type" -> "record",
              "name" -> "Bake",
              "fields" -> Json.arr(
                fieldOpt("temp", "Temperature"),
                fieldOpt("time", "Time"),
                fieldOpt("note", "Note"),
              )
            )
          ),
          "default" -> null
        )
      )
    )
  )

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

  val Avro2299CanonicalMisplacedSize: String =
    """{
      |  "type" : "record",
      |  "name" : "Avro2299CanonicalMisplacedSize",
      |  "user-property" : "There is no size attribute in a record.",
      |  "size" : 100,
      |  "fields" : [ {
      |    "name" : "a1",
      |    "user-property" : "There is no size attribute in a field.",
      |    "size" : 200,
      |    "type" : {
      |      "name" : "MyEnum",
      |      "type" : "enum",
      |      "symbols" : ["one", "two", "three"],
      |      "user-property" : "There is no size attribute in an enum.",
      |      "size" : 300
      |    }
      |  }, {
      |    "name" : "a2",
      |    "type" : {
      |      "name" : "MyFixed",
      |      "type" : "fixed",
      |      "user-property" : "There is a size attribute in a fixed.",
      |      "size" : 123
      |    }
      |  } ]
      |}""".stripMargin

  /** Write all of these schemas to the plugin directory. */
  def main(args: Array[String]) {
    val base: String =
      sys.env.getOrElse("AVRO_ENCHIRIDION_REPO_DIR", "/tmp/avro-enchiridion")

    Directory(base)
      .resolve("plugin/src/test/avro/com/skraba/avro/enchiridion/simple")
      .createDirectory()
      .resolve("SimpleRecord.avsc")
      .toFile
      .writeAll(Json.prettyPrint(Json.parse(SimpleRecord)))

    Directory(base)
      .resolve("plugin/src/main/avro/com/skraba/avro/enchiridion/recipe")
      .createDirectory()
      .resolve("Recipe.avsc")
      .toFile
      .writeAll(Json.prettyPrint(Json.parse(Recipe)))
  }

}
