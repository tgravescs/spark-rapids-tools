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
  private var appSummaries : KVStore = null
  private var finalStoreDir: String = ""
  private var finalStoreLoc: String = ""
  private val appSpecific : scala.collection.mutable.Map[String, KVStore] =
    scala.collection.mutable.Map[String, KVStore]()

  val APP_SUMMARY = "APP_SUMMARIES"


  def initialize(outputPath: String): Unit = {
    // force ROCKSDB for now
    finalStoreDir = outputPath
    finalStoreLoc = outputPath + "/appSummaries"
    sparkConf.set("spark.history.store.hybridStore.diskBackend", "ROCKSDB")
    logInfo(s"ROCKSDB location is: $finalStoreLoc")
    appSummaries = KVUtils.createKVStore(Option(new File(finalStoreLoc)), live = false, sparkConf)
  }

  private def getSpecificStore(target: String): KVStore = {
    if (target == APP_SUMMARY) {
      appSummaries
    } else {
      if (appSpecific.contains(target)) {
        appSpecific.get(target).get
      } else {
        val appDir = finalStoreDir + s"/$target"
        logInfo(s"ROCKSDB location is: $appDir")
        val appKvstore =
          KVUtils.createKVStore(Option(new File(appDir)), live = false, sparkConf)
        appSpecific.put(target, appKvstore)
        appKvstore
      }
    }
  }

  def write[T](storeId: String, obj: T): Unit = {
    val store = getSpecificStore(storeId)
    store.write(obj)
  }

  def read[T](storeId: String, klass: Class[T], key: Any): T = {
    val store = getSpecificStore(storeId)
    store.read(klass, key)
  }

  def delete[T](storeId: String, klass: Class[T], key: Any): Unit = {
    val store = getSpecificStore(storeId)
    store.delete(klass, key)
  }

  def view[T](storeId: String, klass: Class[T]): KVStoreView[T] = {
    val store = getSpecificStore(storeId)
    store.view(klass)
  }

  // TODO - presumably pull into memory, maybe just use the iterators and
  // see if that doesn't cause huge memory increase
  def viewToSeq[T](storeId: String, klass: Class[T]): Seq[T] = {
    KVUtils.viewToSeq(view(storeId, klass))
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
