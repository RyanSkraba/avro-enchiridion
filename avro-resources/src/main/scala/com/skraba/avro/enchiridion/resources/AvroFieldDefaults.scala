package com.skraba.avro.enchiridion.resources

import play.api.libs.json._

import scala.collection.immutable.ListMap

/**
  * Resources for testing default values in Avro schemas.
  */
object AvroFieldDefaults {

  /**
    * Holder for the attributes that can be used in testing default values for a field in a record.
    *
    * @param tag          a tag to identify the configuration, also used as the record name.
    * @param fieldType    the avro type to assign to the only field in the record.
    * @param fieldDefault the JSON representation of the field default for the record.
    */
  case class FieldDefaultCfg(
      tag: String,
      fieldType: JsValue,
      fieldDefault: JsValue
  ) {

    def unionNullFirst(tagSuffix: String = "UnionNullFirst"): FieldDefaultCfg =
      FieldDefaultCfg(
        tag + tagSuffix,
        Json.arr("null", fieldType),
        fieldDefault
      )

    def unionNullLast(tagSuffix: String = "UnionNullLast"): FieldDefaultCfg =
      FieldDefaultCfg(
        tag + tagSuffix,
        Json.arr(fieldType, "null"),
        fieldDefault
      )

    /**
      * @return A JSON object containing the namespace, name and aliases.
      */
    lazy val toJson: JsObject = Json.obj(
      "name" -> tag,
      "type" -> "record",
      "fields" -> Json.arr(
        "name" -> "a1",
        "type" -> fieldType,
        "default" -> fieldDefault
      )
    )
  }

  object FieldDefaultCfg {
    def simpleType(
        fieldType: String
    )(tag: String, fieldDefault: JsValue): FieldDefaultCfg =
      FieldDefaultCfg(tag, JsString(fieldType), fieldDefault)
  }

  val AllNumericDefaults: Iterable[FieldDefaultCfg] =
    NumericValues.AllJson.flatMap {
      case (tag, v: JsValue) =>
        List(
          FieldDefaultCfg.simpleType("double")(s"FieldDoubleDefault$tag", v),
          FieldDefaultCfg.simpleType("float")(s"FieldFloatDefault$tag", v),
          FieldDefaultCfg.simpleType("long")(s"FieldLongDefault$tag", v),
          FieldDefaultCfg.simpleType("int")(s"FieldIntDefault$tag", v)
        )
    }

  val (
    validNumeric: Seq[FieldDefaultCfg],
    invalidNumeric: Seq[FieldDefaultCfg]
  ) = AllNumericDefaults.partition {
    case FieldDefaultCfg(_, JsString("double"), _: JsNumber) => true
    case FieldDefaultCfg(_, JsString("float"), _: JsNumber)  => true
    case FieldDefaultCfg(_, JsString("long"), num: JsNumber)
        if num.value.isValidLong =>
      true
    case FieldDefaultCfg(_, JsString("int"), num: JsNumber)
        if num.value.isValidInt =>
      true
    case _ => false
  }

  /** A record with a partially specified default.  The fields missing from the default also have defaults. */
  val Avro2844: String =
    """{
      |  "name" : "Zoo",
      |  "type" : "record",
      |  "fields" : [ {
      |    "name" : "pet",
      |    "type" : {
      |      "name" : "Pet",
      |      "type" : "record",
      |      "fields" : [ {
      |        "name" : "name",
      |        "type" : "string",
      |        "default" : ""
      |      }, {
      |        "name" : "weight",
      |        "type" : "long",
      |        "default" : 0
      |      } ]
      |    },
      |    "default" : { }
      |  } ]
      |}""".stripMargin

  val Valid: Map[String, JsObject] = ListMap(validNumeric.collect {
    case cfg => cfg.tag -> cfg.toJson
  }: _*)

  val Invalid: Map[String, JsObject] = ListMap(invalidNumeric.collect {
    case cfg => cfg.tag -> cfg.toJson
  }: _*)

  /** Create the two files in the /tmp directory. */
  def main(args: Array[String]) {
    val dst = AvroTestResources.Base
      .resolve("avro-resources/src/test/resources/")
      .createDirectory()

    dst
      .resolve("field-defaults-good.txt")
      .toFile
      .writeAll(Valid.map {
        case (tag, json) => tag + ":" + Json.stringify(json) + "\n"
      }.toSeq: _*)
    dst
      .resolve("field-defaults-bad.txt")
      .toFile
      .writeAll(Invalid.map {
        case (tag, json) => tag + ":" + Json.stringify(json) + "\n"
      }.toSeq: _*)
  }

}
