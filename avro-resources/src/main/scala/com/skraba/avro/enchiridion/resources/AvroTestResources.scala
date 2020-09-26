package com.skraba.avro.enchiridion.resources

import com.skraba.avro.enchiridion.resources.AvroLogicalTypes.DateLogicalTypeRecord
import play.api.libs.json.{JsObject, JsString, JsValue, Json}

import scala.reflect.io.{Directory, File, Path}

/**
  * Reusable resources for Avro tests.
  */
object AvroTestResources {

  val Base: Directory = Directory(
    Path(
      sys.env.getOrElse("AVRO_ENCHIRIDION_REPO_DIR", "/tmp/avro-enchiridion")
    )
  )

  /** The file of this source code. */
  private[this] lazy val ThisFile: File =
    Base
      .resolve(
        s"avro-resources/src/main/scala/${AvroTestResources.getClass.getName.replace('.', '/').replace("$", "")}.scala"
      )
      .toFile

  /**
    * @return a JSON object with name, doc and type attributes, useful in Avro field arrays.
    */
  private[this] def field(
      name: String,
      doc: String,
      fieldType: JsValue = JsString("string")
  ): JsObject =
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

  def SimpleRecordWithColumn(
      recordName: String,
      fieldName: String,
      fieldType: Any,
      fieldDefault: Any
  ): String =
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

  /** A schema containing itself. */
  val Recursive: String =
    """{
      |  "type" : "record",
      |  "name" : "A",
      |  "fields" : [ {
      |    "name" : "head",
      |    "type" : "int"
      |  }, {
      |    "name" : "left",
      |    "type" : [ "null", "A" ]
      |  }, {
      |    "name" : "right",
      |    "type" : [ "null", "A" ]
      |  } ]
      |}""".stripMargin

  /** A schema containing itself indirectly. */
  val RecursiveIndirect: String =
    """{
      |  "type" : "record",
      |  "name" : "A",
      |  "fields" : [ {
      |    "name" : "a1",
      |    "type" : "int"
      |  }, {
      |    "name" : "a2",
      |    "type" : {
      |      "type" : "record",
      |      "name" : "B",
      |      "fields" : [ {
      |        "name" : "b1",
      |        "type" : "int"
      |      }, {
      |        "name" : "b2",
      |        "type" : [ "null", "A" ],
      |        "default" : null
      |      } ]
      |    }
      |  } ]
      |}""".stripMargin

  /** A Schema of medium complexity. */
  val Recipe: String = Json.prettyPrint(
    Json.obj(
      "type" -> "record",
      "name" -> "Recipe",
      "namespace" -> "com.skraba.avro.enchiridion.recipe",
      "doc" -> "A recipe containing ingredients and steps",
      "fields" -> Json.arr(
        fieldOpt("title", "Name or title for this recipe"),
        fieldOpt("step_id", "A unique tag for this step"),
        fieldArray(
          "from_step_id",
          "The tag of the steps feeding into this step (if any)",
          Json.obj(
            "type" -> "record",
            "name" -> "FromStep",
            "doc" -> "",
            "fields" -> Json.arr(
              field(
                "fraction",
                "The fraction of the incoming step",
                JsString("double")
              ),
              field("step_id", "The unique tag for the incoming step")
            )
          )
        ),
        fieldOpt(
          "source",
          "Where the recipe came from (person, website, book)"
        ),
        fieldOpt("makes", "How much the recipe makes"),
        fieldArray("note", "Any free text (information, hints)"),
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
                  fieldArray(
                    "option",
                    "Replacement options for this ingredient",
                    JsString("Ingredient")
                  ),
                  fieldArray("note", "Any free text (information, hints)")
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
                fieldOpt("note", "Note")
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
      |      "symbols" : [ "one", "two", "three" ],
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

  /**
    * @return the given JSON string as pretty formatted scala code.
    */
  def prettifyCode(variableName: String, json: String): String = {
    Json
      .prettyPrint(Json.parse(json))
      .split("\n")
      .zipWithIndex
      .map { case (s, i) => if (i == 0) "    \"\"\"" + s else s"      |$s" }
      .mkString(start = "", sep = "\n", end = "\"\"\".stripMargin")
  }

  /** Write all of these schemas to the plugin directory. */
  def main(args: Array[String]) {

    // Rewrite some AVSC files with resources from this file.
    Base
      .resolve("plugin/src/test/avro/com/skraba/avro/enchiridion/simple")
      .createDirectory()
      .resolve("SimpleRecord.avsc")
      .toFile
      .writeAll(Json.prettyPrint(Json.parse(SimpleRecord)))

    Base
      .resolve("plugin/src/main/avro/com/skraba/avro/enchiridion/recipe")
      .createDirectory()
      .resolve("Recipe.avsc")
      .toFile
      .writeAll(Json.prettyPrint(Json.parse(Recipe)))

    Base
      .resolve("plugin/src/test/avro/com/skraba/avro/enchiridion/simple")
      .createDirectory()
      .resolve("DateLogicalTypeRecord.avsc")
      .toFile
      .writeAll(Json.prettyPrint(DateLogicalTypeRecord))

    // Rewrite this source itself with prettified JSON.
    val schemasToPrettify: Map[String, String] = Map(
      "SimpleRecord" -> SimpleRecord,
      "Recursive" -> Recursive,
      "RecursiveIndirect" -> RecursiveIndirect,
      "Avro1965" -> Avro1965,
      "Avro2299CanonicalMisplacedSize" -> Avro2299CanonicalMisplacedSize
    )
    ThisFile.writeAll(
      {
        for (
          scala <- ThisFile.safeSlurp().toArray;
          block <- scala.split("val")
        )
          yield {
            schemasToPrettify
              .find {
                case (k, _) => block.startsWith(s" $k:")
              }
              .map {
                case (k, v) =>
                  (s" $k: String =\n${prettifyCode(k, v)}" +: block
                    .split("\n\n")
                    .tail).mkString("\n\n")
              }
              .getOrElse(block)
          }
      }.mkString("val")
    )
  }

}
