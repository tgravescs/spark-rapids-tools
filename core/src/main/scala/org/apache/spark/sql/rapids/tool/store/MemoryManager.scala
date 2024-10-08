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

import java.io.File

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.status.KVUtils
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{KVStore, KVStoreView}

object MemoryManager extends Logging {

  private val sparkConf = new SparkConf()
  private var listing: KVStore = null
  private var finalStoreLoc: String = ""

  def initialize(outputPath: String): Unit = {
    finalStoreLoc = outputPath + "/appSummaries"
    // force ROCKSDB
    sparkConf.set("spark.history.store.hybridStore.diskBackend", "ROCKSDB")
    logInfo(s"ROCKSDB location is: $finalStoreLoc")
    listing = KVUtils.createKVStore(Option(new File(finalStoreLoc)), live = false, sparkConf)
  }

  def write[T](obj: T): Unit = {
      listing.write(obj)
  }

  def read[T](klass: Class[T], key: Any): T = {
      listing.read(klass, key)
  }

  def delete[T](klass: Class[T], key: Any): Unit = {
    listing.delete(klass, key)
  }

  def view[T](klass: Class[T]): KVStoreView[T] = {
    listing.view(klass)
  }

  def viewToSeq[T](klass: Class[T]): Seq[T] = {
    KVUtils.viewToSeq(view(klass))
  }

  def deleteDB(): Unit = {
    if (!finalStoreLoc.isEmpty) {
      try {
        Utils.deleteRecursively(new File(finalStoreLoc))
      } catch {
        case NonFatal(e) =>
          logError(s"Error removing ROCKSDB database at: $finalStoreLoc", e)
      }
    }
  }
}
