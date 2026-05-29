package com.sneaksanddata.arcane.framework
package services.base

import zio.Task
import zio.stream.ZStream

trait ShardProvider:
  type Shard
  
  def deleteShards(): Task[Unit]
  def getShards: ZStream[Any, Throwable, Shard]
