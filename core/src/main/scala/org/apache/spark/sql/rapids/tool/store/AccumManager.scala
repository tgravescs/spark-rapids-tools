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

import scala.collection.Map
import scala.collection.mutable

import com.nvidia.spark.rapids.tool.analysis.StatisticsMetrics

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.sql.rapids.tool.util.EventUtils.parseAccumFieldToLong
import org.apache.spark.status.KVUtils

/**
 * A class that manages task/stage accumulables -
 * maintains a map of accumulable id to AccumInfo
 */
class AccumManager(appId: String) extends Logging {
  val accumRefMap: mutable.HashMap[Long, AccumMetaRef] = {
    new mutable.HashMap[Long, AccumMetaRef]()
  }
  val accumIdStageMap: mutable.HashMap[Long, mutable.HashMap[Int, Long]] = {
    new mutable.HashMap[Long, mutable.HashMap[Int, Long]]()
  }

  if (appId.isEmpty) {
    throw new Exception("Error can't have an empty appId for the AccumManager!")
  }

  /*
  private def getOrCreateAccumInfo(id: Long, name: Option[String]): AccumMetaRef = {
    accumRefMap.getOrElseUpdate(id, AccumMetaRef(id, name))


//    val newAccumInfo = accumInfoMap.getOrElseUpdate(id, new AccumInfo(AccumMetaRef(id, name)))
    /*
    try {
      val existingElement = MemoryManager.read(appId, classOf[AccumInfo], id)
      existingElement
    } catch {
      case _:NoSuchElementException =>
      val newAccumInfo = new AccumInfo(AccumMetaRef(id, name), appId)
      MemoryManager.write(appId, newAccumInfo)
      newAccumInfo
    }

     */
//    println("Before writing accumulable")
//    try{
//      println(s"The stored accum -> TaskUpdateMap - ${newAccumInfo.taskUpdatesMap}" +
//        s" StageValuesMap - ${newAccumInfo.stageValuesMap} " +
//        s" AccumMetaRef - ${newAccumInfo.infoRef.id} ${newAccumInfo.infoRef.name}")
//      MemoryManager.write(newAccumInfo)
//      val read_value = MemoryManager.read(classOf[AccumInfo], id)
//      println(s"The read_value -> TaskUpdateMap - ${read_value.taskUpdatesMap} " +
//        s"StageValuesMap - ${read_value.stageValuesMap} " +
//        s" AccumMetaRef - ${read_value.infoRef.id} ${read_value.infoRef.name}")
//      Thread.sleep(1000)
//    }
//    catch{
//      case e: Exception => println(e.toString)
//    }
//    println("After writing accumulable")
//    newAccumInfo
  }

   */

  // TODO - don't retkurn actual map?
  def getAccumStagesInfo: mutable.HashMap[Long, mutable.HashMap[Int, Long]] = {
    accumIdStageMap
  }

  def addAccToStage(stageId: Int, accumulableInfo: AccumulableInfo): Unit = {
    logWarning("add  stages id for: " + stageId)
    accumRefMap.getOrElseUpdate(accumulableInfo.id,
      AccumMetaRef(accumulableInfo.id, accumulableInfo.name))
    val stagesAccumMap = accumIdStageMap.getOrElseUpdate(stageId, new mutable.HashMap[Int, Long])
    val parsedValue = accumulableInfo.value.flatMap(parseAccumFieldToLong)
    val existingValue = stagesAccumMap.getOrElse(stageId, 0L)
    val incomingValue = parsedValue match {
      case Some(v) => v
      case _ => 0L
    }
    stagesAccumMap.put(stageId, Math.max(existingValue, incomingValue))
  }

  def addAccToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    logWarning("add task stages id for: " + stageId + " task: " + taskId)
    // val accumInfoRef = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    accumRefMap.getOrElseUpdate(accumulableInfo.id,
      AccumMetaRef(accumulableInfo.id, accumulableInfo.name))
    val parsedUpdateValue = accumulableInfo.update.flatMap(parseAccumFieldToLong)
    // This is for cases where same task updates the same accum multiple times
    val taskValueWrapper = new TaskAccumValueWrapper(accumulableInfo.id,
      stageId, taskId, parsedUpdateValue.getOrElse(0L))
    MemoryManager.write(appId, taskValueWrapper)
  }

  def getAccStageIds(id: Long): Set[Int] = {
//    accumInfoMap.get(id).map(_.getStageIds).getOrElse(Set.empty)
    accumIdStageMap.getOrElse(id, mutable.HashMap.empty).keySet.toSet
    /*
    try {
      logWarning("get acc stages id for: " + id)
      Option(MemoryManager.read(appId, classOf[AccumInfo], id))
        .map(_.getStageIds).getOrElse(Set.empty)
    } catch {
      case _: NoSuchElementException =>
       Set.empty
    }

     */
  }

  def getAccumSingleStage: Map[Long, Int] = {
    accumIdStageMap.map { case (k, v) =>
      (k, v.keys.min)
    }
   /* val kvStoreIterator = KVUtils.viewToSeq(MemoryManager.view(appId, classOf[AccumInfo]))
    kvStoreIterator.map(
      accumInfo => (accumInfo.infoRef.id, accumInfo.getMinStageId)).toMap

    */
//    toIterate.map { case (id, accInfo) =>
//      (id, accInfo.getMinStageId)
//    }.toMap
  }

  def removeAccumInfo(id: Long): Unit = {
    /*
    try {
      val removedElement = MemoryManager.read(appId, classOf[AccumInfo], id)
      MemoryManager.delete(appId, classOf[AccumInfo], id)
      Option(removedElement)
    } catch {
      case _: NoSuchElementException =>
        None
    }

     */
    accumIdStageMap.remove(id)
  }


  def calculateAccStats(id: Long): StatisticsMetrics = {
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

  def getMaxStageValue(id: Long): Option[Long] = {
    accumIdStageMap.get(id).map(_.values.max)
    /*
    try {
      logWarning("get max stage value for id: " + id)
      val foo = MemoryManager.read(appId, classOf[AccumInfo], id)
      logWarning("get max stage value for foo: " + foo)
      val resget = if (foo.getMaxStageValue.isDefined) {
        foo.getMaxStageValue.get
      } else {
        return None
      }
      logWarning("get max stage value for tom foo: " + resget +
        " instance: " + resget.isInstanceOf[Long])
      Option(foo).map(_.getMaxStageValue.get)

    } catch {
      case _: NoSuchElementException =>
        None
    }

     */
//    accumInfoMap.get(id).map(_.getMaxStageValue.get)
  }
}
