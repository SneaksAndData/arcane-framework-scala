package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import org.apache.iceberg.io
import org.apache.parquet.io.{InputFile, SeekableInputStream}

/**
 * Wrapper class for org.apache.iceberg.io.InputFile that maps it to org.apache.parquet.io.InputFile
 * @param file Iceberg InputFile object
 */
class ParquetInputFile(file: org.apache.iceberg.io.InputFile) extends InputFile:
  override def getLength: Long = file.getLength

  override def newStream(): SeekableInputStream = ParquetInputStreamAdapter(file.newStream())

given Conversion[org.apache.iceberg.io.InputFile, org.apache.parquet.io.InputFile] with
  override def apply(icebergFile: io.InputFile): InputFile = ParquetInputFile(icebergFile)
  