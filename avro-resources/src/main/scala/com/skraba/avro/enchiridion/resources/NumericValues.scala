package com.skraba.avro.enchiridion.resources

import play.api.libs.json.{JsNumber, JsString, JsValue}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

/** Lists of useful numeric values that can be applied to tests. */
object NumericValues {

  /**
   * Examples and edge cases for IEEE 754 64 bit floating point numbers.
   */
  val Doubles: Map[String, Double] = ListMap(
    "DoubleZero" -> 0d,
    "DoubleNegZero" -> -0d,
    "Double123" -> 123.45d,
    "DoubleMinPositiveValue" -> Double.MinPositiveValue,
    "DoubleNan" -> Double.NaN,
    "DoublePositiveInfinity" -> Double.PositiveInfinity,
    "DoubleNegativeInfinity" -> Double.NegativeInfinity,
    "DoubleMinValue" -> Double.MinValue,
    "DoubleMaxValue" -> Double.MaxValue
  )

  /**
   * Examples and edge cases for IEEE 754 32 bit floating point numbers.
   */
  val Floats: Map[String, Float] = ListMap(
    "FloatZero" -> 0f,
    "FloatNegZero" -> -0f,
    "Float123" -> 123.45f,
    "FloatMinPositiveValue" -> Float.MinPositiveValue,
    "FloatNan" -> Float.NaN,
    "FloatPositiveInfinity" -> Float.PositiveInfinity,
    "FloatNegativeInfinity" -> Float.NegativeInfinity,
    "FloatMinValue" -> Float.MinValue,
    "FloatMaxValue" -> Float.MaxValue
  )

  /**
   * Examples and edge cases for 64 bit integer numbers.
   */
  val Longs: Map[String, Long] = ListMap(
    "LongZero" -> 0L,
    "Long123" -> 12345L,
    "LongMinValue" -> Long.MinValue,
    "LongMaxValue" -> Long.MaxValue
  )

  /**
   * Examples and edge cases for 32 bit integer numbers.
   */
  val Ints: Map[String, Int] = ListMap(
    "IntZero" -> Int.box(0),
    "Int123" -> Int.box(12345),
    "IntMinValue" -> Int.MinValue,
    "IntMaxValue" -> Int.MaxValue
  )

  /**
   * Examples and edge cases for 16 bit integer numbers.
   */
  val Shorts: Map[String, Short] = ListMap(
    "ShortZero" -> Short.box(0),
    "Short123" -> Short.box(12345),
    "ShortMinValue" -> Short.MinValue,
    "ShortMaxValue" -> Short.MaxValue
  )

  /**
   * Examples and edge cases for 8 bit integer numbers.
   */
  val Bytes: Map[String, Byte] = ListMap(
    "ByteZero" -> 0,
    "Byte123" -> 123,
    "ByteMinValue" -> Byte.MinValue,
    "ByteMaxValue" -> Byte.MaxValue
  )

  /** Valid strings that should be used as text representations for non-finite floating point values according
   * to IEEE 754 5.12 */
  val Strings: Map[String, String] = ListMap(
    "StringInfinity" -> "Infinity",
    "StringInf" -> "Inf",
    "StringNaN" -> "NaN",
    "StringSNaN" -> "SNaN",
  ).flatMap {
    // Case insensitive.
    case (k, v) => List(k -> v, s"${k}Case" -> v.toLowerCase)
  }.flatMap {
    // Optional sign.
    case (k, v) => List(k -> v, s"${k}Pos" -> s"+$v", s"${k}Neg" -> s"-$v")
  }

  val All: Map[String, Any] = Doubles ++ Floats ++ Longs ++ Ints ++ Shorts ++ Bytes ++ Strings

  /** All of the numbers as JSON numbers, if possible.  JSON string for non-finite floating points. */
  val AllJson: Map[String, JsValue] = All.mapValues {
    case d: Double if java.lang.Double.isFinite(d) => JsNumber(d)
    case f: Float if java.lang.Float.isFinite(f) => JsNumber(BigDecimal(f))
    case l: Long => JsNumber(l)
    case i: Int => JsNumber(i)
    case s: Short => JsNumber(BigDecimal(s))
    case b: Byte => JsNumber(BigDecimal(b))
    case x => JsString(x.toString)
  }.map(_.swap).map(_.swap) // Double swap removes duplicate keys.

  val DoublesJava: Map[String, Number] = Doubles.mapValues(Double.box)
  val FloatsJava: Map[String, Number] = Floats.mapValues(Float.box)
  val LongsJava: Map[String, Number] = Longs.mapValues(Long.box)
  val IntsJava: Map[String, Number] = Ints.mapValues(Int.box)
  val ShortsJava: Map[String, Number] = Shorts.mapValues(Short.box)
  val BytesJava: Map[String, Number] = Bytes.mapValues(Byte.box)


  val AllJava: java.util.Map[String, Number] = (DoublesJava ++ FloatsJava ++ LongsJava ++ IntsJava ++ ShortsJava ++ BytesJava).asJava
}