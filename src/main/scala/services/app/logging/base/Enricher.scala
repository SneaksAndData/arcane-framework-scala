package com.sneaksanddata.arcane.framework
package services.app.logging.base

import services.app.logging.{EnvironmentEnricher, StaticEnricher}

import scala.annotation.targetName

trait Enricher:
  /**
   * Enriches the logger with the key-value pair.
   *
   * @param target the setter method of the logging framework to invoke with the key-value pair.
   */
  def enrichLoggerWith(target: (String, String) => Unit): Unit

  /**
   * Appends the other enricher to this enricher.
   *
   * @param other the other enricher to append.
   * @return the new enricher.
   */
  @targetName("append")
  def ++(other: Enricher): Enricher


/**
 * The companion object for the Enricher class.
 */
object Enricher:
  /**
   * Creates a new instance of the Enricher class.
   *
   * @param key the key of the key-value pair.
   * @param value the value of the key-value pair.
   * @return a new instance of the Enricher class.
   */
  def apply(key: String, value: String): Enricher = new StaticEnricher(key, value)

  /**
   * Creates a new instance of the Enricher class.
   *
   * @param key the key of the key-value pair.
   * @param default the default value of the key-value pair.
   * @return a new instance of the Enricher class.
   */
  def fromEnvironment(key: String, default: String = ""): Enricher = new EnvironmentEnricher(key, default)