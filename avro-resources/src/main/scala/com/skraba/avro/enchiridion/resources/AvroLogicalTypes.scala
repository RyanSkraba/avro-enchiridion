package com.skraba.avro.enchiridion.resources

import play.api.libs.json.{JsObject, Json}

/**
  * Generate valid and invalid schemas for checking schema logical types.
  *
  * @see https://avro.apache.org/docs/current/spec.html#Logical+Types
  */
object AvroLogicalTypes {

  lazy val TimestampMillis: JsObject =
    Json.obj("type" -> "long", "logicalType" -> "timestamp-millis")
  lazy val TimestampMicros: JsObject =
    Json.obj("type" -> "long", "logicalType" -> "timestamp-micros")
  lazy val Date: JsObject = Json.obj("type" -> "int", "logicalType" -> "date")
  lazy val TimeMillis: JsObject =
    Json.obj("type" -> "int", "logicalType" -> "time-millis")
  lazy val TimeMicros: JsObject =
    Json.obj("type" -> "long", "logicalType" -> "time-micros")

  /**
    * In a field, the logicalType attribute needs to be nested. None of these fields have logical types.
    *
    * ==WRONG==
    *
    * {{{
    * ...
    * { "name": "datetime_ms", "type": "long", "logicalType": "timestamp-millis" }
    * ...
    * }}}
    *
    * ==RIGHT==
    *
    * {{{
    * ...
    * { "name": "datetime_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}}
    * ...
    * }}}
    */
  lazy val DateLogicalTypeRecordInvalid: JsObject = Json.obj(
    "type" -> "record",
    "name" -> "DateLogicalTypeRecordInvalid",
    "fields" -> Json.arr(
      Json.obj("name" -> "name", "type" -> "string"),
      Json.obj("name" -> "datetime_ms") ++ TimestampMillis,
      Json.obj("name" -> "datetime_us") ++ TimestampMicros,
      Json.obj("name" -> "date") ++ Date,
      Json.obj("name" -> "time_ms") ++ TimeMillis,
      Json.obj("name" -> "time_us") ++ TimeMicros
    )
  )

  lazy val DateLogicalTypeRecord: JsObject = Json.obj(
    "type" -> "record",
    "name" -> "DateLogicalTypeRecord",
    "fields" -> Json.arr(
      Json.obj("name" -> "name", "type" -> "string"),
      Json.obj("name" -> "datetime_ms", "type" -> TimestampMillis),
      Json.obj("name" -> "datetime_us", "type" -> TimestampMicros),
      Json.obj("name" -> "date", "type" -> Date),
      Json.obj("name" -> "time_ms", "type" -> TimeMillis),
      Json.obj("name" -> "time_us", "type" -> TimeMicros)
    )
  )

  def decimalBytes(precision: Object, scale: Object): String =
    s"""{
       |  "type":"bytes",
       |  "logicalType":"decimal",
       |  "precision":$precision,
       |  "scale":$scale
       |}""".stripMargin

  def decimalFixed(precision: Object, scale: Object, size: Object): String =
    s"""{
       |  "fixed":"fixed",
       |  "name":"fixedP${precision}S$scale",
       |  "size":$size",
       |  "logicalType":"decimal",
       |  "precision":$precision,
       |  "scale":$scale
       |}""".stripMargin
}
