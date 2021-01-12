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
    if (DIRECTION == IN) KeyHandler.inEdgeKeyToBytes(relation.to, relation.typeId, relation.from)
    else                 KeyHandler.outEdgeKeyToBytes(relation.from, relation.typeId, relation.to)

  def set(relation: StoredRelation): Unit = {
    val keyBytes = getKey(relation)
    db.put(keyBytes, ByteUtils.longToBytes(relation.id))
  }

  def delete(relation: StoredRelation): Unit = {
    val keyBytes = getKey(relation)
    db.delete(keyBytes)
  }

  def deleteRange(firstId: Long): Unit = {
    db.deleteRange(
      KeyHandler.outEdgeKeyPrefixToBytes(firstId,0),
      KeyHandler.outEdgeKeyPrefixToBytes(firstId, -1))
  }

  def deleteRange(firstId: Long, typeId: Int): Unit = {
    db.deleteRange(
      KeyHandler.outEdgeKeyToBytes(firstId, typeId, 0),
      KeyHandler.outEdgeKeyToBytes(firstId, typeId, -1))
  }

  def get(node1: Long, edgeType: Int, node2: Long): Option[Long] = {
    val keyBytes = KeyHandler.outEdgeKeyToBytes(node1, edgeType, node2)
    val value = db.get(keyBytes)
    if (value!=null)
      Some(ByteUtils.getLong(value, 0))
    else
      None
  }

  def getNodeIds(nodeId: Long): Iterator[Long] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(nodeId)
    new NodeIdIterator(db, prefix)
  }

  def getNodeIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(nodeId, edgeType)
    new NodeIdIterator(db, prefix)
  }

  class NodeIdIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[Long]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): Long = {
      val fromNodeId = ByteUtils.getLong(iter.key(), 13)
      iter.next()
      fromNodeId
    }
  }

  def getRelationIds(nodeId: Long): Iterator[Long] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(nodeId)
    new RelationIdIterator(db, prefix)
  }

  def getRelationIds(nodeId: Long, edgeType: Int): Iterator[Long] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(nodeId, edgeType)
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
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(nodeId)
    new RelationIterator(db, prefix)
  }

  def getRelations(nodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    val prefix = KeyHandler.outEdgeKeyPrefixToBytes(nodeId, edgeType)
    new RelationIterator(db, prefix)
  }

  class RelationIterator(db: RocksDB, prefix: Array[Byte]) extends Iterator[StoredRelation]{
    val iter = db.newIterator()
    iter.seek(prefix)

    override def hasNext: Boolean = iter.isValid && iter.key().startsWith(prefix)

    override def next(): StoredRelation = {
      val key = iter.key()
      val node1 = ByteUtils.getLong(key, 1)
      val relType = ByteUtils.getInt(key, 9)
      val node2  = ByteUtils.getLong(key, 13)
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
