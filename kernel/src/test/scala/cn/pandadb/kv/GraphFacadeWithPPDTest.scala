package cn.pandadb.kv

import java.io.File

import cn.pandadb.kernel.kv.meta.{NameStore, NodeLabelNameStore, PropertyNameStore, RelationTypeNameStore}
import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI}
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue.{Node, Relationship}

class GraphFacadeWithPPDTest {

//  var nodeLabelStore: NameStore = _
//  var relLabelStore: NameStore = _
//  var propNameStore: NameStore = _
//  var graphStore: RocksDBGraphAPI = _

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var graphFacade: GraphFacadeWithPPD = _


  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata/output"))
    new File("./testdata/output").mkdirs()
    new File("./testdata/output/nodelabels").createNewFile()
    new File("./testdata/output/rellabels").createNewFile()

    val dbPath = "./testdata"
    var nodeStore = new NodeStoreAPI(dbPath)
    var relationStore = new RelationStoreAPI(dbPath)
    var indexStore = new IndexStoreAPI(dbPath)

//    graphStore = new RocksDBGraphAPI("./testdata/output/rocksdb")
//    nodeLabelStore = new NodeLabelNameStore(graphStore.getRocksDB)
//    relLabelStore = new RelationTypeNameStore(graphStore.getRocksDB)
//    propNameStore = new PropertyNameStore(graphStore.getRocksDB)




    graphFacade = new GraphFacadeWithPPD(
      new FileBasedIdGen(new File("./testdata/output/nodeid"), 100),
      new FileBasedIdGen(new File("./testdata/output/relid"), 100),
      nodeStore,
      relationStore,
      indexStore,
      {}
    )
  }

  @Test
  def test1(): Unit = {
    Assert.assertEquals(0, nodeStore.allNodes().size)
    Assert.assertEquals(0, relationStore.allRelations().size)

    graphFacade.addNode(Map("name" -> "1")).addNode(Map("name" -> "2")).addRelation("1->2", 1, 2, Map())

    //nodes: {1,2}
    //rels: {1}
    Assert.assertEquals(List(1, 2), nodeStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.addNode(Map("name" -> "3"))
    //nodes: {1,2,3}
    Assert.assertEquals(List(1, 2, 3), nodeStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.deleteNode(2)
    //nodes: {1,3}
    Assert.assertEquals(List(1, 3), nodeStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.addNode(Map("name" -> "4")).deleteNode(1L)
    //nodes: {3,4}
    Assert.assertEquals(List(3, 4), nodeStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.addNode(Map("name" -> "5")).addNode(Map("name" -> "6")).deleteNode(5L).deleteNode(3L)
    //nodes: {4,6}
    Assert.assertEquals(List(4, 6), nodeStore.allNodes().toSeq.map(_.id).sorted)

    graphFacade.close()
  }

  @Test
  def testQuery(): Unit = {
    val n1: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 40), "person")
    val n2: Long = graphFacade.addNode2(Map("name" -> "alex", "age" -> 20), "person")
    val n3: Long = graphFacade.addNode2(Map("name" -> "simba", "age" -> 10), "worker")
    val n4: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 50), "person")

    var res: CypherResult = null
    res = graphFacade.cypher("match (n) return n")
    res.show
    Assert.assertEquals(4, res.records.size)

    res = graphFacade.cypher("match (n) where n.name='alex' return n")
    res.show
    Assert.assertEquals(1, res.records.size)

    res = graphFacade.cypher("match (n) where n.age=20 return n")
    res.show
    Assert.assertEquals(1, res.records.size)

    // test index
    val indexId = graphFacade.createNodePropertyIndex("person", Set("name"))
    graphFacade.writeNodeIndexRecord(indexId, n1, "bob")

    res = graphFacade.cypher("match (n) where n.name='bob' return n")
    res.show
    Assert.assertEquals(2, res.records.size)

    return
    res = graphFacade.cypher("match (n:person) where n.name='alex' return n")
    res.show
    Assert.assertEquals(1, res.records.size)

    graphFacade.writeNodeIndexRecord(indexId, n4, "bob")
    res = graphFacade.cypher("match (n:person) where n.name='bob' return n")
    res.show
    Assert.assertEquals(2, res.records.size)

    graphFacade.close()
  }

  @Test
  def testQueryRelations(): Unit = {
    graphFacade.addRelation("knows", 1L, 2L, Map())
    graphFacade.addRelation("knows", 2L, 3L, Map())
  }

  @Test
  def testQueryLabels(): Unit = {
    val n1: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 40), "person")
    val n2: Long = graphFacade.addNode2(Map("name" -> "alex", "age" -> 20), "person")
    val n3: Long = graphFacade.addNode2(Map("name" -> "simba", "age" -> 10), "worker")
    graphFacade.allNodes().foreach{
      node=>
        println(node.id, node.labelIds.mkString(";"), node.properties)
    }
    val indexid = graphFacade.createNodePropertyIndex("person", Set("age"))

    val res = graphFacade.cypher("match (n:person) where n.age=40  return n")
    res.show
    Assert.assertEquals(1, res.records.size)

  }

  @Test
  def testQueryRelation(): Unit = {
    val n1: Long = graphFacade.addNode2(Map("name" -> "bob", "age" -> 40), "person")
    val n2: Long = graphFacade.addNode2(Map("name" -> "alex", "age" -> 20), "person")
    val n3: Long = graphFacade.addNode2(Map("name" -> "simba", "age" -> 10), "worker")
    graphFacade.addRelation("friend", 1L, 2L, Map())
    graphFacade.allRelations().foreach(println)
    val res = graphFacade.cypher("match (n)-[r]->(m) return r")
    res.show
  }

}
