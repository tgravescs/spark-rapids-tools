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
import java.net.URI
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo
import org.apache.spark.status.KVUtils
import org.apache.spark.util.Utils
import org.apache.spark.util.kvstore.{KVStore, KVStoreView}

/**
 * Key Value store base class.  Currently hardcoded to use the Spark
 * ROCKSDB implementation of org.apache.spark.util.kvstore.KVStore.
 *
 * This class could be extended to have logic on decided which backend
 * store is most efficient. For instance, memory or disk store.
 */
object ToolKVStore extends Logging {

  private val sparkConf = new SparkConf()
  private var appSummaries: KVStore = null
  private var finalStoreLoc: String = ""
  private val DB_DIR_NAME = "qual_store_path"
  private val APP_SUMMARY_DB_DIR = "appSummaries"
  private val allApps = new ConcurrentHashMap[String, QualificationSummaryInfo]()


  def initialize(localStorePath: String): Unit = {
    // make sure to remove any scheme like file:// but what if they specified hdfs output?
    // create a store.path type config
    logWarning("localStorePath before: " + localStorePath)
    finalStoreLoc = new URI(localStorePath + "/" + DB_DIR_NAME).getPath
    val appSummaryDbLoc = new URI(finalStoreLoc + "/" + APP_SUMMARY_DB_DIR).getPath
    logWarning("appSummaryDbLoc after: " + appSummaryDbLoc)

    // force ROCKSDB
    sparkConf.set("spark.history.store.hybridStore.diskBackend", "ROCKSDB")
    logInfo(s"ROCKSDB location is: $finalStoreLoc")
    appSummaries = KVUtils.createKVStore(Option(new File(finalStoreLoc)), live = false, sparkConf)
  }

  def write[T](obj: T): Unit = {
    appSummaries.write(obj)
  }

  def read[T](klass: Class[T], key: Any): T = {
    appSummaries.read(klass, key)
  }

  def delete[T](klass: Class[T], key: Any): Unit = {
    appSummaries.delete(klass, key)
  }

  def view[T](klass: Class[T]): KVStoreView[T] = {
    appSummaries.view(klass)
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
