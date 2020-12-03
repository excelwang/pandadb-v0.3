package cn.pandadb.kernel.kv

import cn.pandadb.kernel.{GraphRAM, NodeId, PropertyStore, TypedId}
import cn.pandadb.kernel.store.{MergedChanges, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}

class RocksDBGraphImpl(dbPath: String) {
  private val rocksDB = RocksDBStorage.getDB(dbPath)
  private val nodeStore = new NodeStore(rocksDB)
  private val relationStore = new RelationStore(rocksDB)
  private val nodeLabelIndex = new NodeLabelIndex(rocksDB)
  private val relationLabelIndex = new RelationLabelIndex(rocksDB)
  private val relationInEdgeIndex = new RelationInEdgeIndexStore(rocksDB)
  private val relationOutEdgeIndex = new RelationOutEdgeIndexStore(rocksDB)
  def clear(): Unit = {
  }

  def close(): Unit = {
    rocksDB.close()
  }

  // node operations
//  def addNode(t: StoredNode): Unit = {
//    nodeStore.set(t.id, t.labelIds, null)
//  }
//
//  def addNode(t: StoredNodeWithProperty): Unit = {
//    nodeStore.set(t.id, t.labelIds, t.properties)
//  }

  def addNode(nodeId: Long, labelIds: Array[Int], propeties: Map[String, Any]): Unit = {
    nodeStore.set(nodeId, labelIds, propeties)
    labelIds.foreach(labelId => nodeLabelIndex.add(labelId, nodeId))
  }

  def deleteNode(id: Long): Unit = {
    val node = nodeStore.delete(id)
    node.labelIds.foreach(labelId => nodeLabelIndex.delete(labelId, id))
  }

  def nodeAt(id: Long): StoredNode = {
    nodeStore.get(id)
  }

//  def nodes(): Iterator[StoredNode] = {
//    nodeStore.all()
//  }

  def findNodes(labelId: Int): Iterator[Long] = {
    nodeLabelIndex.getNodes(labelId)
  }

  def allNodes(): Iterator[StoredNode] = {
    nodeStore.all()
  }

  // relation operations
  def addRelation(t: StoredRelation): Unit = {
    relationStore.setRelation(t.id, t.from, t.to, t.labelId, 0, null)

    relationInEdgeIndex.setIndex(t.from, t.labelId, 0, t.to, t.id, null)
    relationOutEdgeIndex.setIndex(t.from, t.labelId, 0, t.to, t.id, null)
  }

  def addRelation(t: StoredRelationWithProperty): Unit = {
    relationStore.setRelation(t.id, t.from, t.to, t.labelId, t.category, t.properties)

    relationInEdgeIndex.setIndex(t.from, t.labelId, 0, t.to, t.id, t.properties)
    relationOutEdgeIndex.setIndex(t.from, t.labelId, 0, t.to, t.id, t.properties)
  }

  def addRelation(relId: Long, from: Long, to: Long, labelId: Int, propeties: Map[String, Any]): Unit = {
    relationStore.setRelation(relId, from, to, labelId, 0, propeties)

    relationInEdgeIndex.setIndex(from, labelId, 0, to, relId, propeties)
    relationOutEdgeIndex.setIndex(from, labelId, 0, to, relId, propeties)
  }

  def deleteRelation(id: Long): Unit = {
    val relation = relationStore.getRelation(id)
    relationStore.deleteRelation(id)

    relationInEdgeIndex.deleteIndex(relation.from, relation.labelId, relation.category, relation.to)
    relationOutEdgeIndex.deleteIndex(relation.from, relation.labelId, relation.category, relation.to)

  }
//
//  def deleteRelation(fromNode: Long, toNode: Long, labelId: Long, category: Long): Unit= {
//  }

  def relationAt(id: Long): StoredRelation = {
    relationStore.getRelation(id)
  }

  def allRelations(): Iterator[StoredRelation] = {
    relationStore.getAll()
  }

  def findRelations(labelId: Int): Iterator[Long] = {
    relationLabelIndex.getRelations(labelId)
  }
  // out
  def findOutEdgeRelations(fromNodeId: Long): Iterator[StoredRelation] = {
    relationOutEdgeIndex.getRelations(fromNodeId)
  }
  def findOutEdgeRelations(fromNodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    relationOutEdgeIndex.getRelations(fromNodeId, edgeType)
  }
  def findOutEdgeRelations(fromNodeId: Long, edgeType: Int, category: Long): Iterator[StoredRelation] = {
    relationOutEdgeIndex.getRelations(fromNodeId, edgeType, category)
  }

  def findToNodes(fromNodeId: Long): Iterator[Long] = {
    relationOutEdgeIndex.findNodes(fromNodeId)
  }
  def findToNodes(fromNodeId: Long, edgeType: Int): Iterator[Long] = {
    relationOutEdgeIndex.findNodes(fromNodeId, edgeType)
  }
  def findToNodes(fromNodeId: Long, edgeType: Int, category: Long): Iterator[Long] = {
    relationOutEdgeIndex.findNodes(fromNodeId, edgeType, category)
  }

  // in
  def findInEdgeRelations(toNodeId: Long): Iterator[StoredRelation] = {
    relationInEdgeIndex.getRelations(toNodeId)
  }

  def findInEdgeRelations(toNodeId: Long, edgeType: Int): Iterator[StoredRelation] = {
    relationInEdgeIndex.getRelations(toNodeId, edgeType)
  }

  def findInEdgeRelations(toNodeId: Long, edgeType: Int, category: Long): Iterator[StoredRelation] = {
    relationInEdgeIndex.getRelations(toNodeId, edgeType, category)
  }

  def findFromNodes(toNodeId: Long): Iterator[Long] = {
    relationInEdgeIndex.findNodes(toNodeId)
  }

  def findFromNodes(toNodeId: Long, edgeType: Int): Iterator[Long] = {
    relationInEdgeIndex.findNodes(toNodeId, edgeType)
  }

  def findFromNodes(toNodeId: Long, edgeType: Int, category: Long): Iterator[Long] = {
    relationInEdgeIndex.findNodes(toNodeId, edgeType, category)
  }

  // below is the code added by zhaozihao, for possible further use.
  def relsFrom(id: Long): Iterable[StoredRelation] = ???
  def relsTo(id: Long): Iterable[StoredRelation] = ???

  def searchByLabel(label: Label): Iterable[StoredNode] = ???
  def searchByType(t: Type): Iterable[StoredRelation] = ???

  //essential? could check whether index available in implemantion.
  def searchByIndexedProperty(stat: Stat): Iterable[StoredNode] = ???
  def searchByCategory(category: Category): Iterable[StoredRelation] = ???

  def searchByProp(stat: Stat): Iterable[StoredNode] = ???





}


case class Label(label: String)
case class Type(t: String)

case class Stat()
case class Category()