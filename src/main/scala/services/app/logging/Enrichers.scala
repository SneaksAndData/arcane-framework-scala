package com.sneaksanddata.arcane.framework
package services.app.logging

import services.app.logging.base.Enricher

import scala.annotation.targetName

/**
 * A static enricher that enriches the logger with a key-value pair.
 *
 * @param key the key of the key-value pair.
 * @param value the value of the key-value pair.
 */
class StaticEnricher(key: String, value: String) extends Enricher:

  /**
   * Enriches the logger with the key-value pair.
   *
   * @param target the setter method of the logging framework to invoke with the key-value pair.
   */
  override def enrichLoggerWith(target: (String, String) => Unit): Unit =
    target(key, value)

  /**
   * Appends the other enricher to this enricher.
   *
   * @param other the other enricher to append.
   * @return the new enricher.
   */
  @targetName("append")
  def ++(other: Enricher): Enricher = SequenceEnricher(Seq(this, other))


/**
 * A sequence enricher that enriches the logger with a sequence of enrichers.
 * @param enrichers the sequence of enrichers.
 */
class SequenceEnricher(enrichers: Seq[Enricher]) extends Enricher:
  override def enrichLoggerWith(target: (String, String) => Unit): Unit =
    enrichers.foreach(_.enrichLoggerWith(target))

  /**
   * Appends the other enricher to this enricher.
   *
   * @param other the other enricher to append.
   * @return the new enricher.
   */
  @targetName("append")
  def ++(other: Enricher): Enricher = SequenceEnricher(this.enrichers ++ Seq(other))


/**
 * An enricher that enriches the logger with an environment variable.
 * @param key the key of the environment variable.
 * @param default the default value of the environment variable.
 */
class EnvironmentEnricher(key: String, default: String = "") extends Enricher:

  /**
   * Enriches the logger with the key-value pair.
   *
   * @param target the setter method of the logging framework to invoke with the key-value pair.
   */
  override def enrichLoggerWith(target: (String, String) => Unit): Unit =
    target(key, sys.env.getOrElse(key, default))

  /**
   * Appends the other enricher to this enricher.
   *
   * @param other the other enricher to append.
   * @return the new enricher.
   */
  @targetName("append")
  def ++(other: Enricher): Enricher = SequenceEnricher(Seq(this, other))
