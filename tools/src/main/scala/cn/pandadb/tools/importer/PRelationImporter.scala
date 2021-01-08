package cn.pandadb.tools.importer

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import cn.pandadb.kernel.PDBMetaData
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, RocksDBStorage}
import cn.pandadb.kernel.store.StoredRelationWithProperty
import cn.pandadb.kernel.util.serializer.RelationSerializer
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.{FlushOptions, RocksDB, WriteBatch, WriteOptions}

import scala.util.Random

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 11:01 2020/12/4
 * @Modified By:
 */
// TODO 1.relationLabel 2.relationType String->Int

/**
 * protocol: :relId(long), :fromId(long), :toId(long), :edgetype(string), propName1:type, ...
 */
class PRelationImporter(dbPath: String, headFile: File, edgeFile: File, soloDB: RocksDB) extends Importer with Logging{

  override val importerFileReader = new ImporterFileReader(edgeFile, 500000)
  override var propSortArr: Array[Int] = null
  override val headMap: Map[Int, String] = _setEdgeHead()

  override val service: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  service.scheduleWithFixedDelay(importerFileReader.fillQueue, 0, 50, TimeUnit.MILLISECONDS)
  service.scheduleAtFixedRate(closer, 1, 1, TimeUnit.SECONDS)
//  val soloDB = RocksDBStorage.getInitDB(s"${dbPath}")
//  val relationDB = RocksDBStorage.getInitDB(s"${dbPath}/rels")
//  val inRelationDB = RocksDBStorage.getInitDB(s"${dbPath}/inEdge")
//  val outRelationDB = RocksDBStorage.getInitDB(s"${dbPath}/outEdge")
//  val relationTypeDB = RocksDBStorage.getInitDB(s"${dbPath}/relLabelIndex")

  var globalCount: AtomicLong = new AtomicLong(0)
  val estEdgeCount: Long = estLineCount(edgeFile)

  val writeOptions: WriteOptions = new WriteOptions()
  writeOptions.setDisableWAL(true)
  writeOptions.setIgnoreMissingColumnFamilies(true)
  writeOptions.setSync(false)

  def importRelations(): Unit ={
    importData()
    soloDB.close()
//    inRelationDB.close()
//    outRelationDB.close()
//    relationTypeDB.close()
    logger.info(s"$globalCount relations imported.")
  }

  override protected def _importTask(taskId: Int): Boolean = {
    val serializer = RelationSerializer
    var innerCount = 0

    val inBatch = new WriteBatch()
    val outBatch = new WriteBatch()
    val storeBatch = new WriteBatch()
    val labelBatch = new WriteBatch()

    while (importerFileReader.notFinished) {
      val batchData = importerFileReader.getLines
      if(batchData.nonEmpty) {
        batchData.foreach(line => {
          innerCount += 1
          val lineArr = line.replace("\n", "").split(",")
          val relation = _wrapEdge(lineArr)
          val serializedRel = serializer.serialize(relation)
          storeBatch.put(KeyHandler.relationKeyToBytes(relation.id), serializedRel)
          inBatch.put(KeyHandler.outEdgeKeyToBytes(relation.to, relation.typeId, relation.from), ByteUtils.longToBytes(relation.id))
          outBatch.put(KeyHandler.outEdgeKeyToBytes(relation.from, relation.typeId, relation.to), ByteUtils.longToBytes(relation.id))
          labelBatch.put(KeyHandler.relationLabelIndexKeyToBytes(relation.typeId, relation.id), Array.emptyByteArray)

          if(innerCount % 1000000 == 0) {
            soloDB.write(writeOptions, storeBatch)
            soloDB.write(writeOptions, inBatch)
            soloDB.write(writeOptions, outBatch)
            soloDB.write(writeOptions, labelBatch)
            storeBatch.clear()
            inBatch.clear()
            outBatch.clear()
            labelBatch.clear()
          }
        })
        soloDB.write(writeOptions, storeBatch)
        soloDB.write(writeOptions, inBatch)
        soloDB.write(writeOptions, outBatch)
        soloDB.write(writeOptions,labelBatch)
        storeBatch.clear()
        inBatch.clear()
        outBatch.clear()
        labelBatch.clear()
        if(globalCount.addAndGet(batchData.length) % 10000000 == 0){
          val time1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
          logger.info(s"${globalCount.get() / 10000000}kw of $estEdgeCount(est) edges imported. $time1")
        }
      }
      // forbid to access file reader at same time
      Thread.sleep(10*taskId)
    }

    val flushOptions = new FlushOptions
    soloDB.flush(flushOptions)
//    soloDB.flush(flushOptions)
//    soloDB.flush(flushOptions)
//    soloDB.flush(flushOptions)
    logger.info(s"$innerCount, $taskId")
    true
  }

  private def _setEdgeHead(): Map[Int, String] = {
    _setHead(4, headFile)
  }

  private def _wrapEdge(lineArr: Array[String]): StoredRelationWithProperty = {
    val relId: Long = lineArr(0).toLong
    val fromId: Long = lineArr(1).toLong
    val toId: Long = lineArr(2).toLong
    val edgeType: Int = PDBMetaData.getTypeId(lineArr(3))
    val propMap: Map[Int, Any] = _getPropMap(lineArr, propSortArr, 4)

    new StoredRelationWithProperty(relId, fromId, toId, edgeType, propMap)
  }
}
