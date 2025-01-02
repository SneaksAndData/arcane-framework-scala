package com.sneaksanddata.arcane.framework
package utils

import services.app.base.StreamLifetimeService

class TestStreamLifetimeService(maxQueries: Int, callback: Int => Any) extends StreamLifetimeService:
  var counter = 0
  override def cancelled: Boolean =
    callback(counter)
    counter += 1
    counter > maxQueries

  override def cancel(): Unit = ()

  override def start(): Unit = ()

object TestStreamLifetimeService:

  def withMaxQueries(maxQueries: Int) = new TestStreamLifetimeService(maxQueries, _ => ())

  def apply(maxQueries: Int) = new TestStreamLifetimeService(maxQueries, _ => ())

  def apply(maxQueries: Int, callback: Int => Any) = new TestStreamLifetimeService(maxQueries, callback)

