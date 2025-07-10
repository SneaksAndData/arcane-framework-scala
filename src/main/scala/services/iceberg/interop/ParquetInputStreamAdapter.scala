package com.sneaksanddata.arcane.framework
package services.iceberg.interop

import org.apache.iceberg.io.SeekableInputStream
import org.apache.parquet.io.DelegatingSeekableInputStream

/** Wrapper class for org.apache.iceberg.io.SeekableInputStream that maps it to
  * org.apache.parquet.io.DelegatingSeekableInputStream
  *
  * @param stream
  *   Iceberg SeekableInputStream from file.newStream()
  */
class ParquetInputStreamAdapter(stream: SeekableInputStream) extends DelegatingSeekableInputStream(stream):
  override def getPos: Long = stream.getPos

  override def seek(newPos: Long): Unit = stream.seek(newPos)
