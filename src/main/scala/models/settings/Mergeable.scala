package com.sneaksanddata.arcane.framework
package models.settings

/** A trait that defines a mergeable type, which can be merged with another instance of an associated type to produce a
  * new instance of a result type. Used in backfill overrides process.
  */
trait Mergeable:
  /** The type of the instance that can be merged with this instance. */
  type MergeableFrom

  /** The type of the result produced by merging this instance with an instance of `MergeableFrom`. */
  type MergeResult

  /** Merges this instance with an instance of `MergeableFrom` to produce a new instance of `MergeResult`.
    *
    * This default implementation keeps the current instance unchanged so that lightweight test stubs and non-mergeable
    * mocks can still satisfy the contract without redefining the method. Concrete settings classes are expected to
    * override this method with real merge logic.
    *
    * @param overrides
    *   The instance of `MergeableFrom` to merge with this instance.
    * @return
    *   A new instance of `MergeResult` that represents the result of the merge.
    */
  def merge(overrides: Option[MergeableFrom]): MergeResult = this.asInstanceOf[MergeResult]
