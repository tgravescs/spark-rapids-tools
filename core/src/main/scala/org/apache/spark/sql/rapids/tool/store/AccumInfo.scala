/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids.tool.store

import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.status.KVUtils
import org.apache.spark.util.kvstore.{KVIndex, KVStoreView}

class TaskAccumValueWrapper(
    @KVIndex val accumId: Long,
    val stageId: Int,
    val taskId: Long,
    val accumValue: Long) {

  // TODO - add stage attempt id
  @JsonIgnore @KVIndex
  def id: Array[Long] = Array(accumId, stageId, taskId)
}


/*

/**
 * Maintains the accumulator information for a single accumulator
 * This maintains following information:
 * 1. Task updates for the accumulator - a map of all taskIds and their update values
 * 2. Stage values for the accumulator - a map of all stageIds and their total values
 * 3. AccumMetaRef for the accumulator - a reference to the Meta information
 * @param infoRef - AccumMetaRef for the accumulator
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class AccumInfo(val infoRef: AccumMetaRef, val appId: String) extends Serializable with Logging {

  @KVIndexParam
  val id: Long = infoRef.id
  // TODO: use sorted maps for stageIDs and taskIds?
//  @JsonProperty("taskUpdatesMap")
  //val taskUpdatesMap: mutable.HashMap[Long, Long] =
   // new mutable.HashMap[Long, Long]()
//  @JsonProperty("stageValuesMap")
  @JsonDeserialize(contentAs=classOf[mutable.HashMap[Int, Long]])
  val stageValuesMap: mutable.HashMap[Int, Long] =
    new mutable.HashMap[Int, Long]()


  /**
   * Add accumulable to a stage while:
   * 1- handling StageCompleted event.
   * 2- handling taskEnd event, we want to make sure that there is an entry that maps the stage to
   *    the accumulable. The reason is that we want to keep track of the stageIDs in case their
   *    stageCompleted has never been triggered. It is common case for incomplete eventlogs.
   * @param stageId the stageId pulled from the StageCompleted/TaskEnd event
   * @param accumulableInfo the accumulableInfo from the StageCompleted/TaskEnd event
   * @param update optional long that represents the task update value in case the call was
   *               triggered by a taskEnd event and the map between stage-Acc has not been
   *               established yet.
   */
  @JsonIgnore
  def addAccumToStage(stageId: Int,
      accumulableInfo: AccumulableInfo,
      update: Option[Long] = None): Unit = {
    val parsedValue = accumulableInfo.value.flatMap(parseAccumFieldToLong)
    // in case there is an out of order event, the value showing up later could be
    // lower-than the previous value. In that case we should take the maximum.
    val existingValue = stageValuesMap.getOrElse(stageId, 0L)
    val incomingValue = parsedValue match {
      case Some(v) => v
      case _ => update.getOrElse(0L)
    }
    stageValuesMap.put(stageId, Math.max(existingValue, incomingValue))
  }

  /**
   * Add accumulable to a task while handling TaskEnd event.
   * This can propagate a call to addAccToStage if the stageId does not exist in the stageValuesMap
   * @param stageId the stageId pulled from the TaskEnd event
   * @param taskId the taskId pulled from the TaskEnd event
   * @param accumulableInfo the accumulableInfo from the TaskEnd event
   */
  @JsonIgnore
  def addAccumToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    // update is the task value, value is the total accum for all tasks
    val parsedUpdateValue = accumulableInfo.update.flatMap(parseAccumFieldToLong)
    // we need to update the stageMap if the stageId does not exist in the map
    val updateStageFlag = !stageValuesMap.contains(stageId)
    // This is for cases where same task updates the same accum multiple times
    val taskValueWrapper = new TaskAccumValueWrapper(accumulableInfo.id,
      stageId, taskId, parsedUpdateValue.getOrElse(0L))
    MemoryManager.write(appId, taskValueWrapper)
    /*val existingUpdateValue = taskUpdatesMap.getOrElse(taskId, 0L)
    parsedUpdateValue match {
      case Some(v) =>
        taskUpdatesMap.put(taskId, v + existingUpdateValue)
      case None =>
        taskUpdatesMap.put(taskId, existingUpdateValue)
    } */
    // update the stage value map if necessary
    if (updateStageFlag) {
      addAccumToStage(stageId, accumulableInfo, parsedUpdateValue)
    }
  }

  @JsonIgnore
  def getStageIds: Set[Int] = {
    stageValuesMap.keySet.toSet
  }

  @JsonIgnore
  def getMinStageId: Int = {
    stageValuesMap.keys.min
  }

  @JsonIgnore
  def calculateAccStats(): StatisticsMetrics = {
    val sortedTaskUpdates = KVUtils.viewToSeq(
      MemoryManager.view(appId, classOf[TaskAccumValueWrapper]).index("accumId").first(id).last(id))
      .map(_.accumValue).sorted

    // val sortedTaskUpdates = taskUpdatesMap.values.toSeq.sorted
    if (sortedTaskUpdates.isEmpty) {
      // do not check stage values because the stats is only meant for task updates
      StatisticsMetrics.ZERO_RECORD
    } else {
      val min = sortedTaskUpdates.head
      val max = sortedTaskUpdates.last
      val sum = sortedTaskUpdates.sum
      val median = if (sortedTaskUpdates.size % 2 == 0) {
        val mid = sortedTaskUpdates.size / 2
        (sortedTaskUpdates(mid) + sortedTaskUpdates(mid - 1)) / 2
      } else {
        sortedTaskUpdates(sortedTaskUpdates.size / 2)
      }
      StatisticsMetrics(min, median, max, sum)
    }
  }
  @JsonIgnore
  def getMaxStageValue: Option[Long] = {
    if (stageValuesMap.values.isEmpty) {
      None
    } else {
      logWarning("stge max value contains: " + stageValuesMap.values.mkString(","))
      logWarning("stge max value contains: " + stageValuesMap.values.head.getClass)
      logWarning("stge keys contains: " + stageValuesMap.keys.mkString(","))
      logWarning("stge keys contains: " + stageValuesMap.keys.head.getClass)

      /*stageValuesMap.foreach { case (k, v) =>
        logWarning(s"key is : $k value is $v value instance ${v.getClass}")
      }*/
      val foo = stageValuesMap.values
      foo.foreach { x =>
        logWarning("x class " + x.getClass)
      }
      logWarning("max is " + foo + " instance: " + foo.getClass)
      Some(stageValuesMap.values.max)
    }
  }
}

 */

object AccumInfo {
  def getViewFromSeq[T](view: KVStoreView[T]): Seq[T] = {
    KVUtils.viewToSeq(view)
  }
}

