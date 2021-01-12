package cn.pandadb.kernel

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, RocksDBStorage}
import cn.pandadb.kernel.util.serializer.BaseSerializer
import org.rocksdb.{FlushOptions, RocksDB}

/**
 * @Author: Airzihao
 * @Description:
 * @Date: Created at 19:47 2020/12/3
 * @Modified By:
 */
object PDBMetaData {

  private val _nodeIdAllocator: AtomicLong = new AtomicLong(0)
  private val _relationIdAllocator: AtomicLong = new AtomicLong(0)
  private val _indexIdAllocator: AtomicInteger = new AtomicInteger(0)

  def availableNodeId: Long = _nodeIdAllocator.getAndIncrement()
  def availableRelId: Long = _relationIdAllocator.getAndIncrement()
  def availabelIndexId: Int = _indexIdAllocator.getAndIncrement()

  private val _propIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)
  private val _typeIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)
  private val _labelIdManager: MetaIdManager = new MetaIdManager(Int.MaxValue)

  def persist(rocksDB: RocksDB): Unit = {
    rocksDB.put("_nodeIdAllocator".getBytes(), BaseSerializer.serialize(_nodeIdAllocator.get()))
    rocksDB.put("_relationIdAllocator".getBytes(), BaseSerializer.serialize(_relationIdAllocator.get()))
    rocksDB.put("_indexIdAllocator".getBytes(), BaseSerializer.serialize(_indexIdAllocator.get()))
    rocksDB.put("_propIdManager".getBytes(), _propIdManager.serialized)
    rocksDB.put("_typeIdManager".getBytes(), _typeIdManager.serialized)
    rocksDB.put("_labelIdManager".getBytes(), _labelIdManager.serialized)
    rocksDB.put(KeyHandler.nodeIdGeneratorKeyToBytes(), ByteUtils.longToBytes(_nodeIdAllocator.get()))
    rocksDB.put(KeyHandler.relationIdGeneratorKeyToBytes(), ByteUtils.longToBytes(_relationIdAllocator.get()))
    _labelIdManager.all.foreach{
      kv=>
        val key = KeyHandler.nodeLabelKeyToBytes(kv._1)
        rocksDB.put(key, ByteUtils.stringToBytes(kv._2))
    }
    _typeIdManager.all.foreach{
      kv=>
        val key = KeyHandler.relationTypeKeyToBytes(kv._1)
        rocksDB.put(key, ByteUtils.stringToBytes(kv._2))
    }
    _propIdManager.all.foreach{
      kv=>
        val key = KeyHandler.propertyNameKeyToBytes(kv._1)
        rocksDB.put(key, ByteUtils.stringToBytes(kv._2))
        rocksDB.put(key, ByteUtils.stringToBytes(kv._2))
    }
    rocksDB.flush(new FlushOptions)
  }

  def init(rocksDB: RocksDB): Unit = {
    _nodeIdAllocator.set(BaseSerializer.bytes2Long(rocksDB.get("_nodeIdAllocator".getBytes())))
    _relationIdAllocator.set(BaseSerializer.bytes2Long(rocksDB.get("_relationIdAllocator".getBytes())))
    _indexIdAllocator.set(BaseSerializer.bytes2Int(rocksDB.get("_indexIdAllocator".getBytes())))
    _propIdManager.init(rocksDB.get("_propIdManager".getBytes()))
    _typeIdManager.init(rocksDB.get("_typeIdManager".getBytes()))
    _labelIdManager.init(rocksDB.get("_labelIdManager".getBytes()))
    rocksDB.close()
  }

  def isPropExists(prop: String): Boolean = _propIdManager.isNameExists(prop)

  def isLabelExists(label: String): Boolean = _labelIdManager.isNameExists(label)

  def isTypeExists(edgeType: String): Boolean = _typeIdManager.isNameExists(edgeType)

  def getPropId(prop: String): Int = {
    _propIdManager.getId(prop)
  }

  def getPropName(propId: Int): String = {
    _propIdManager.getName(propId)
  }

  def getLabelId(label: String): Int = {
    _labelIdManager.getId(label)
  }

  def getLabelName(labelId: Int): String = {
    _labelIdManager.getName(labelId)
  }

  def getTypeId(edgeType: String): Int = {
    _typeIdManager.getId(edgeType)
  }

  def getTypeName(typeId: Int): String = {
    _typeIdManager.getName(typeId)
  }
}
