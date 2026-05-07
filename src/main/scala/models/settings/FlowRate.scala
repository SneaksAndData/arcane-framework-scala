package com.sneaksanddata.arcane.framework
package models.settings

import java.time.Duration

/** A fixed processing rate for a stream as elements/interval
  */
case class FlowRate(elements: Int, interval: Duration)
