package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import com.fasterxml.jackson.databind.JsonNode
import org.apache.iceberg.variants.{ShreddedObject, ValueArray, Variant, VariantMetadata, VariantValue, Variants}

import scala.jdk.CollectionConverters.*

/** Builds Iceberg [[Variant]] values out of Jackson JSON nodes.
  *
  * A column typed `ObjectType` maps to `Types.VariantType`, and Iceberg's parquet writer for such a column is a
  * `ParquetValueWriter[Variant]`: it casts the cell value to [[Variant]] outright. Handing it the Avro object the JSON
  * decoder produces (a `GenericRecord`, `Map` or `GenericArray`) therefore fails with a `ClassCastException` at write
  * time, so nested payload nodes are converted here instead.
  *
  * The conversion runs against the raw JSON rather than the decoded Avro value on purpose: the Avro representation has
  * already lost information the variant encoding can carry (an Avro `map` forces one value type across all keys, and
  * union branches are resolved), whereas the JSON node still holds the document as the producer sent it.
  *
  * Variant metadata is a dictionary of every object key in the document, so the whole tree is walked once to collect
  * the names before the value tree is built.
  */
object JsonVariantConverter:

  /** Converts a JSON node into a [[Variant]]. A `null` or missing node yields a variant holding the null primitive,
    * which is how an absent nested document is recorded.
    */
  def toVariant(node: JsonNode): Variant =
    val metadata = Variants.metadata(collectFieldNames(node).asJava)
    Variant.of(metadata, toValue(node, metadata))

  /** Every object key in the document, in encounter order. Iceberg sorts and de-duplicates these when it builds the
    * metadata dictionary, so no ordering is imposed here.
    */
  private def collectFieldNames(node: JsonNode): Set[String] =
    if node == null then Set.empty
    else if node.isObject then
      node.fields().asScala.foldLeft(Set.empty[String]) { (names, entry) =>
        names + entry.getKey ++ collectFieldNames(entry.getValue)
      }
    else if node.isArray then node.elements().asScala.foldLeft(Set.empty[String])(_ ++ collectFieldNames(_))
    else Set.empty

  private def toValue(node: JsonNode, metadata: VariantMetadata): VariantValue =
    if node == null || node.isNull || node.isMissingNode then Variants.ofNull()
    else if node.isObject then toObject(node, metadata)
    else if node.isArray then toArray(node, metadata)
    else toPrimitive(node)

  private def toObject(node: JsonNode, metadata: VariantMetadata): ShreddedObject =
    val obj: ShreddedObject = Variants.`object`(metadata)
    node.fields().asScala.foreach(entry => obj.put(entry.getKey, toValue(entry.getValue, metadata)))
    obj

  private def toArray(node: JsonNode, metadata: VariantMetadata): ValueArray =
    val arr: ValueArray = Variants.array()
    node.elements().asScala.foreach(element => arr.add(toValue(element, metadata)))
    arr

  /** Numbers keep the narrowest physical type that holds them, so an integral value does not become a double and gain a
    * spurious fractional part when the variant is read back. Jackson reports `BigInteger`/`BigDecimal` for literals
    * outside the primitive ranges; those fall back to the decimal and string encodings respectively.
    */
  private def toPrimitive(node: JsonNode): VariantValue =
    if node.isTextual then Variants.of(node.textValue())
    else if node.isBoolean then Variants.of(node.booleanValue())
    else if node.isBinary then Variants.of(java.nio.ByteBuffer.wrap(node.binaryValue()))
    else if node.isIntegralNumber then integralToVariant(node)
    else if node.isFloatingPointNumber then floatingToVariant(node)
    else Variants.of(node.toString)

  private def integralToVariant(node: JsonNode): VariantValue =
    if node.canConvertToInt then
      val value = node.intValue()
      if value.isValidByte then Variants.of(value.toByte)
      else if value.isValidShort then Variants.of(value.toShort)
      else Variants.of(value)
    else if node.canConvertToLong then Variants.of(node.longValue())
    else Variants.of(node.decimalValue())

  private def floatingToVariant(node: JsonNode): VariantValue =
    if node.isFloat then Variants.of(node.floatValue())
    else if node.isDouble then Variants.of(node.doubleValue())
    else Variants.of(node.decimalValue())
