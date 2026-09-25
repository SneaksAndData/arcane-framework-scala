package com.sneaksanddata.arcane.framework
package models.settings

trait CompositeSetting[T]:
  def resolve: T
