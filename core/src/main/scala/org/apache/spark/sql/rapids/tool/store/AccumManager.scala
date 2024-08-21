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

import scala.collection.{mutable, Map}

import com.nvidia.spark.rapids.tool.analysis.StatisticsMetrics

import org.apache.spark.scheduler.AccumulableInfo

/**
 * A class that manages task/stage accumulables -
 * maintains a map of accumulable id to AccumInfo
 */
class AccumManager {
  val accumInfoMap: mutable.HashMap[Long, AccumInfo] = {
    new mutable.HashMap[Long, AccumInfo]()
  }

//  private val memoryManager = new MemoryManager()
//  memoryManager.initialize()

  private def getOrCreateAccumInfo(id: Long, name: Option[String]): AccumInfo = {
    val newAccumInfo = accumInfoMap.getOrElseUpdate(id, new AccumInfo(AccumMetaRef(id, name)))
    try{
      val existingElement = MemoryManager.read(classOf[AccumInfo], id)
      return existingElement
    }
    catch {
      case e: NoSuchElementException =>
        println(e.toString)
        MemoryManager.write(new AccumInfo(AccumMetaRef(id, name)))
    }
//    println(s"Existing object -> $existingObject")
    println("Before writing accumulable")
    try{
      println(s"The stored accum -> TaskUpdateMap - ${newAccumInfo.taskUpdatesMap}" +
        s" StageValuesMap - ${newAccumInfo.stageValuesMap} " +
        s" AccumMetaRef - ${newAccumInfo.infoRef.id} ${newAccumInfo.infoRef.name}")
      MemoryManager.write(newAccumInfo)
      val read_value = MemoryManager.read(classOf[AccumInfo], id)
      println(s"The read_value -> TaskUpdateMap - ${read_value.taskUpdatesMap} " +
        s"StageValuesMap - ${read_value.stageValuesMap} " +
        s" AccumMetaRef - ${read_value.infoRef.id} ${read_value.infoRef.name}")
      Thread.sleep(1000)
    }
    catch{
      case e: Exception => println(e.toString)
    }
    println("After writing accumulable")
    newAccumInfo
  }

  def addAccToStage(stageId: Int, accumulableInfo: AccumulableInfo): Unit = {
    val accumInfoRef = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    accumInfoRef.addAccumToStage(stageId, accumulableInfo)
  }

  def addAccToTask(stageId: Int, taskId: Long, accumulableInfo: AccumulableInfo): Unit = {
    val accumInfoRef = getOrCreateAccumInfo(accumulableInfo.id, accumulableInfo.name)
    accumInfoRef.addAccumToTask(stageId, taskId, accumulableInfo)
  }

  def getAccStageIds(id: Long): Set[Int] = {
    accumInfoMap.get(id).map(_.getStageIds).getOrElse(Set.empty)
  }

  def getAccumSingleStage: Map[Long, Int] = {
    accumInfoMap.map { case (id, accInfo) =>
      (id, accInfo.getMinStageId)
    }.toMap
  }

  def removeAccumInfo(id: Long): Option[AccumInfo] = {
    accumInfoMap.remove(id)
  }

  def calculateAccStats(id: Long): Option[StatisticsMetrics] = {
    accumInfoMap.get(id).map(_.calculateAccStats())
  }

  def getMaxStageValue(id: Long): Option[Long] = {
    accumInfoMap.get(id).map(_.getMaxStageValue.get)
  }
}
