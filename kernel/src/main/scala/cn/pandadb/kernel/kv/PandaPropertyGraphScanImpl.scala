package cn.pandadb.kernel.kv

import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.meta.{NameStore, Statistics}
import cn.pandadb.kernel.optimizer.{HasStatistics, PandaPropertyGraphScan}
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI, StoredNode, StoredNodeWithProperty, StoredRelation, StoredRelationWithProperty}
import org.opencypher.lynx.PropertyGraphScanner
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, Node, Relationship, get}

import scala.util.control.Breaks


class PandaPropertyGraphScanImpl(nodeStore: NodeStoreSPI,
                                 relationStore: RelationStoreSPI,
                                 indexStore: IndexStoreAPI,
                                 statistics: Statistics)
      extends PandaPropertyGraphScan[Long] {

  val loop = new Breaks

  protected def mapRelation(rel: StoredRelation): Relationship[Long] = {
    new Relationship[Long] {
      override type I = this.type

      override def id: Long = rel.id

      override def startId: Long = rel.from

      override def endId: Long = rel.to

      override def relType: String = relationStore.getRelationTypeName(rel.typeId).get

      override def copy(id: Long, source: Long, target: Long, relType: String, properties: CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = Map.empty[String, Any]
        rel match {
          case rel: StoredRelationWithProperty =>
            props = rel.properties.map(kv => (relationStore.getRelationTypeName(kv._1).getOrElse("unknown"), kv._2))
          case _ =>
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  protected def mapNode(node: StoredNode): Node[Long] = {
    new Node[Long] {
      override type I = this.type

      override def id: Long = node.id

      override def labels: Set[String] = node.labelIds.toSet.map((id: Int) => nodeStore.getLabelName(id).get)

      override def copy(id: Long, labels: Set[String], properties: CypherValue.CypherMap): this.type = ???

      override def properties: CypherMap = {
        var props: Map[String, Any] = Map.empty[String, Any]
        node match {
          case node: StoredNodeWithProperty =>
            props = node.properties.map {
              kv =>
                (nodeStore.getPropertyKeyName(kv._1).get, kv._2)
            }
          case _ =>
            val n = nodeStore.getNodeById(node.id)
            props = n.asInstanceOf[StoredNodeWithProperty].properties.map {
              kv =>
                (nodeStore.getPropertyKeyName(kv._1).get, kv._2)
            }
        }
        CypherMap(props.toSeq: _*)
      }
    }
  }

  override def nodeAt(id: Long): CypherValue.Node[Long] = mapNode(
    nodeStore.getNodeById(id).orNull
  )

  override def allNodes(labels: Set[String], exactLabelMatch: Boolean): Iterable[Node[Long]] = {
    if (labels.size>1){
      throw new Exception("PandaDB doesn't support multiple label matching at the same time")
    }
    val labelIds = nodeStore.getLabelIds(labels)
    val nodes = nodeStore.getNodesByLabel(labelIds.head)
    nodes.map(mapNode(_)).toIterable
  }


  override def allNodes(): Iterable[Node[Long]] = {
    nodeStore.allNodes().map(node => mapNode(node)).toIterable
  }

  override def allRelationships(): Iterable[CypherValue.Relationship[Long]] = {
    relationStore.allRelations().map(rel => mapRelation(rel)).toIterable
  }

  override def allRelationships(relTypes: Set[String]): Iterable[Relationship[Long]] = {
    if (relTypes.size>1){
      throw new Exception("PandaDB doesn't support multiple label matching at the same time")
    }
    val relations: Iterator[Long] = relationStore.getRelationIdsByRelationType(relationStore.getRelationTypeId(relTypes.head))
    relations.map(relId => mapRelation(relationStore.getRelationById(relId).get)).toIterable
  }

  // PandaPropertyGraphScan

  override def getRelationByNodeId(nodeId: Long, direction: Int): Iterable[Relationship[Long]] = {
    direction match{
      case OUT => relationStore.findOutRelations(nodeId).map(mapRelation).toIterable
      case IN  => relationStore.findInRelations(nodeId).map(mapRelation).toIterable
      case UNDIRECTION => (relationStore.findOutRelations(nodeId).map(mapRelation) ++
        relationStore.findInRelations(nodeId).map(mapRelation)).toIterable
      case _ => null
    }
  }

  override def getRelationByNodeId(nodeId: Long, direction: Int, typeString: String): Iterable[Relationship[Long]] = {
    val typeId = relationStore.getRelationTypeId(typeString)
    direction match{
      case OUT => relationStore.findOutRelations(nodeId, typeId).map(mapRelation).toIterable
      case IN  => relationStore.findInRelations(nodeId, typeId).map(mapRelation).toIterable
      case UNDIRECTION => (relationStore.findOutRelations(nodeId, typeId).map(mapRelation) ++
        relationStore.findInRelations(nodeId, typeId).map(mapRelation)).toIterable
      case _ => null
    }
  }

  override def getRelationByNodeIdWithProperty(nodeId: Long, direction: Int): Iterable[Relationship[Long]] = {
    direction match{
      case OUT => relationStore.findToNodeIds(nodeId).map(relationStore.getRelationById).filter(_.isDefined).map(a=>mapRelation(a.get)).toIterable
      case IN  => relationStore.findFromNodeIds(nodeId).map(relationStore.getRelationById).filter(_.isDefined).map(a=>mapRelation(a.get)).toIterable
      case UNDIRECTION => (relationStore.findToNodeIds(nodeId).map(relationStore.getRelationById).filter(_.isDefined).map(a=>mapRelation(a.get)) ++
        relationStore.findFromNodeIds(nodeId).map(relationStore.getRelationById).filter(_.isDefined).map(a=>mapRelation(a.get))).toIterable
      case _ => null
    }
  }

  override def getRelationByNodeIdWithProperty(nodeId: Long, direction: Int, typeString: String): Iterable[Relationship[Long]] ={
    val typeId = relationStore.getRelationTypeId(typeString)
    direction match{
      case OUT => relationStore.findToNodeIds(nodeId, typeId).map(relationStore.getRelationById).filter(_.isDefined).map(a=>mapRelation(a.get)).toIterable
      case IN  => relationStore.findFromNodeIds(nodeId, typeId).map(relationStore.getRelationById).filter(_.isDefined).map(a=>mapRelation(a.get)).toIterable
      case UNDIRECTION => (relationStore.findToNodeIds(nodeId, typeId).map(relationStore.getRelationById).filter(_.isDefined).map(a=>mapRelation(a.get)) ++
        relationStore.findFromNodeIds(nodeId, typeId).map(relationStore.getRelationById).filter(_.isDefined).map(a=>mapRelation(a.get))).toIterable
      case _ => null
    }
  }

  override def allRelations(): Iterable[Relationship[Long]] =
    relationStore.allRelations().map(mapRelation).toIterable

  override def allRelationsWithProperty: Iterable[Relationship[Long]] =
    relationStore.allRelationsWithProperty().map(mapRelation).toIterable

  override def getRelationByType(typeString: String): Iterable[Relationship[Long]] = {
    getRelationByTypeWithProperty(typeString)
  }

  override def getRelationByTypeWithProperty(typeString: String): Iterable[Relationship[Long]] = {
    relationStore
      .getRelationIdsByRelationType(
        relationStore.getRelationTypeId(typeString)
      )
      .map(relationStore.getRelationById)
      .filter(_.isDefined)
      .map(s=>mapRelation(s.get))
      .toIterable
  }

  override def getNodeById(Id: Long): Node[Long] = nodeStore.getNodeById(Id).map(mapNode).orNull

  override def getNodesByLabel(labelString: String): Iterable[Node[Long]] =
    nodeStore.getNodesByLabel(nodeStore.getLabelId(labelString)).map(mapNode).toIterable

  override def isPropertyWithIndex(labels: Set[String], propertyName: String): (Int, String, Set[String], Long) =
    labels.map(isPropertyWithIndex(_,propertyName)).minBy(_._4)

  override def isPropertysWithIndex(labels: Set[String], propertyNames: Set[String]): (Int, String, Set[String], Long) = {
    val propertyIds = propertyNames.map(nodeStore.getPropertyKeyId).toArray.sorted
    val range = propertyIds.indices
    val combinations = range.flatMap{
      i => (i until propertyIds.length).map(propertyIds.slice(i,_).toArray)
    }.toSet
    val res = labels.flatMap{
      label =>
      combinations.map {
        props =>
        (indexStore.getIndexId(nodeStore.getLabelId(label), props), label, props)
      }
    }
      .filter(_._1.isDefined)
      .map{ p => (p._1.get, p._2, p._3, statistics.getIndexPropertyCount(p._1.get))}
      .filter(_._4.isDefined)
      .map{ p => (p._1, p._2, p._3, p._4.get)}
      .minBy(_._4)
      (res._1, res._2, res._3.map(nodeStore.getPropertyKeyName(_).get).toSet, res._4)
  }

  override def isPropertyWithIndex(label: String, propertyName: String): (Int, String, Set[String], Long) =
    isPropertysWithIndex(label, Set(propertyName))

  override def isPropertysWithIndex(label: String, propertyName: Set[String]): (Int, String, Set[String], Long) = {
    val indexId = indexStore.getIndexId(
      nodeStore.getLabelId(label),
      propertyName.map(nodeStore.getPropertyKeyId).toArray.sorted
    )
    val count = indexId.map(statistics.getIndexPropertyCount(_).getOrElse(-1L)).getOrElse(-1L)
    (indexId.getOrElse(-1), label, propertyName, count)
  }


  override def findNodeId(indexId: Int, value: Any): Iterable[Long] = indexStore.find(indexId, value).toIterable

  override def findNode(indexId: Int, value: Any): Iterable[Node[Long]] =
    indexStore.find(indexId, value).map(nodeStore.getNodeById).filter(_.isDefined).map(s=>mapNode(s.get)).toIterable

  // fixme should find both int and double
  override def findRangeNodeId(indexId: Int, from: Any, to: Any): Iterable[Long] = {
    (from,to) match {
      case range: (Int, Int) => indexStore.findIntRange(indexId, range._1, range._2).toIterable
      case range: (Float, Float) => indexStore.findFloatRange(indexId, range._1, range._2).toIterable
      case _ => null
    }
  }

  override def findRangeNode(indexId: Int, from: Any, to: Any): Iterable[Node[Long]] =
    findRangeNodeId(indexId, from, to).map(nodeStore.getNodeById).filter(_.isDefined).map(s=>mapNode(s.get))

  override def startWithNodeId(indexId: Int, start: String): Iterable[Long] =
    indexStore.findStringStartWith(indexId, start).toIterable

  override def startWithNode(indexId: Int, start: String): Iterable[Node[Long]] =
    startWithNodeId(indexId, start).map(nodeStore.getNodeById).filter(_.isDefined).map(s=>mapNode(s.get))

  //Statistics

  override def getAllNodesCount(): Long = statistics.allNodesCount

  override def getNodesCountByLabel(label: String): Long =
    statistics.getNodeLabelCount(nodeStore.getLabelId(label)).getOrElse(-1)

  override def getNodesCountByLabelAndProperty(label: String, propertyName: String): Long = {
    indexStore
      .getIndexId(nodeStore.getLabelId(label), Array(nodeStore.getPropertyKeyId(propertyName)))
      .map(statistics.getIndexPropertyCount(_).getOrElse(-1L))
      .getOrElse(-1L)
  }

  override def getNodesCountByLabelAndPropertys(label: String, propertyName: String*): Long = {
    indexStore
      .getIndexId(nodeStore.getLabelId(label), propertyName.toArray.map(nodeStore.getPropertyKeyId))
      .map(statistics.getIndexPropertyCount(_).getOrElse(-1L))
      .getOrElse(-1L)
  }

  override def getAllRelsCount(): Long = statistics.allRelationCount

  override def getRelsCountByLabel(label: String): Long =
    statistics.getRelationTypeCount(relationStore.getRelationTypeId(label)).getOrElse(-1L)

  override def getRelsCountByLabelAndProperty(label: String, propertyName: String): Long = {
    indexStore
      .getIndexId(nodeStore.getLabelId(label), Array(nodeStore.getPropertyKeyId(propertyName)))
      .map(statistics.getIndexPropertyCount(_).getOrElse(-1L))
      .getOrElse(-1L)
  }

  override def getRelsCountByLabelAndPropertys(label: String, propertyName: String*): Long = {
    indexStore
      .getIndexId(nodeStore.getLabelId(label), propertyName.toArray.map(nodeStore.getPropertyKeyId))
      .map(statistics.getIndexPropertyCount(_).getOrElse(-1L))
      .getOrElse(-1L)
  }

  def refresh():Unit = {
    statistics.nodeCount = nodeStore.nodesCount
    statistics.relationCount = relationStore.relationCount

    nodeStore.allLabelIds().foreach{
      l =>
        statistics.setNodeLabelCount(l, nodeStore.getNodeIdsByLabel(l).length)
    }
    relationStore.allRelationTypeIds().foreach{
      t =>
        statistics.setRelationTypeCount(t,
          relationStore.getRelationIdsByRelationType(t).length)
    }
    indexStore.allIndexId.foreach{
      id =>
        statistics.setIndexPropertyCount(id,
          indexStore.findByPrefix(ByteUtils.intToBytes(id)).length)
    }
  }
}
