package org.apache.spark.sql.rapids.tool.store

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.status.KVUtils
import org.apache.spark.util.kvstore.KVStore

object MemoryManager {

  private val sparkConf = new SparkConf()
  private var listing : KVStore = null

  def initialize(): Unit = {
    sparkConf.set("spark.history.store.hybridStore.diskBackend", "ROCKSDB")
    val storePath = new File("file:/tmp/storePath")
    listing = KVUtils.createKVStore(Option(storePath), live = true, sparkConf)
  }

  def write[T](obj: T): Unit = {
    listing.synchronized{
      listing.write(obj)
    }
  }

  def read[T](klass: Class[T], key: Any): T = {
    listing.synchronized{
      listing.read(klass, key)
    }
  }
}
