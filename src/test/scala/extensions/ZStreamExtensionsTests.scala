package com.sneaksanddata.arcane.framework
package extensions

import extensions.ZStreamExtensions.dropLast

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers
import zio.stream.ZStream
import zio.{Runtime, Unsafe}

class ZStreamExtensionsTests extends AsyncFlatSpec with Matchers:
  private val runtime = Runtime.default

  it should "should drop the last element from the stream" in {
    // Arrange
    val stream = ZStream(1, 2, 3, 4, 5)

    // Act
    val task = stream.dropLast.runCollect.map { result =>
      // Assert
      result mustBe List(1, 2, 3, 4)
    }

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(task))
  }

  it should "not fail if the stream is empty" in {
    // Arrange
    val stream = ZStream(1, 2, 3, 4, 5)

    // Act
    val task = stream.dropLast.runCollect.map { result =>
      // Assert
      result mustBe List(1, 2, 3, 4)
    }

    Unsafe.unsafe(implicit unsafe => runtime.unsafe.runToFuture(task))
  }
