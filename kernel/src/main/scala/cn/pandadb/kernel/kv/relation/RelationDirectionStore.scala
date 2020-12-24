package cn.pandadb.kernel.kv.relation


import cn.pandadb.kernel.kv.relation.RelationDirection.{Direction, IN}
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler}
import cn.pandadb.kernel.store.StoredRelation
import org.rocksdb.RocksDB

/**
 * @ClassName RelationDirectionStore
 * @Description TODO
 * @Author huchuan
 * @Date 2020/12/22
 * @Version 0.1
 */
object RelationDirection extends Enumeration {
  type Direction = Value
  val IN = Value (0)
  val OUT = Value (1)
}
class RelationDirectionStore(db: RocksDB, DIRECTION: Direction) {
  /**
   * in edge data structure
   * ------------------------
   * type(1Byte),nodeId(8Bytes),relationLabel(4Bytes),category(8Bytes),fromNodeId(8Bytes)-->relationValue(id, properties)
   * ------------------------
   */

  def getKey(relation: StoredRelation): Array[Byte] =
    if (DIRECTION == IN) KeyHandler.edgeKeyToBytes(relation.to, relation.typeId, relation.from)
    else                 KeyHandler.edgeKeyToBytes(relation.from, relation.typeId, relation.to)

  def set(relation: StoredRelation): Unit = {
    val keyBytes = getKey(relation)
    db.put(keyBytes, ByteUtils.longToBytes(relation.id))
  }

  def delete(relation: StoredRelation): Unit = {
    val keyBytes = getKey(relation)
    db.delete(keyBytes)
  }

  def get(node1: Long, edgeType: Int, node2: Long): Long = {
    val keyBytes = KeyHandler.edgeKeyToBytes(node1, edgeType, node2)
    ByteUtils.getLong(db.get(keyBytes), 0)
  }

  def getNodeIds(nodeId: Long): Iterator[Long] = {
    val prefix = KeyHandler.edgeKeyPrefixToBytes(nodeId)
    new NodeIdIterator(db, prefix)
  }

  def getNodeIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = KeyHandler.edgeKeyPrefixToBytes(nodeId, edgeType)
    new NodeIdIterator(db, prefix)
  }

  class NodeIdIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val fromNodeId = ByteUtils.getLong(iter.key(), 12)
      iter.next()
      fromNodeId
    }
  }

  def getRelationIds(nodeId: Long): Iterator[Long] = {
    val prefix = KeyHandler.edgeKeyPrefixToBytes(nodeId)
    new RelationIdIterator(db, prefix)
  }

  def getRelationIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = KeyHandler.edgeKeyPrefixToBytes(nodeId, edgeType)
    new RelationIdIterator(db, prefix)
  }

  class RelationIdIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val id = ByteUtils.getLong(iter.value(), 0)
      iter.next()
      id
    }
  }

  def getRelations(nodeId: Long): Iterator[StoredRelation] = {
    val prefix = KeyHandler.edgeKeyPrefixToBytes(nodeId)
    new RelationIterator(db, prefix)
  }

  def getRelations(nodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    val prefix = KeyHandler.edgeKeyPrefixToBytes(nodeId, edgeType)
    new RelationIterator(db, prefix)
  }

  class RelationIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[StoredRelation]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): StoredRelation = {
      val key = iter.key()
      val node1 = ByteUtils.getLong(key, 0)
      val relType = ByteUtils.getInt(key, 8)
      val node2  = ByteUtils.getLong(key, 12)
      val id = ByteUtils.getLong(iter.value(), 0)

      val res =
        if(DIRECTION==IN) StoredRelation(id, node2, node1, relType)
        else              StoredRelation(id, node1, node2, relType)
      iter.next()
      res
    }
  }

  def all(): Iterator[StoredRelation] = {
    new RelationIterator(db, Array.emptyByteArray)
  }

  def close(): Unit = {
    db.close()
  }
}