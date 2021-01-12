package cn.pandadb.kv

import java.io.File
import cn.pandadb.kernel.kv.meta.{NameStore, NodeLabelNameStore, PropertyNameStore, RelationTypeNameStore, Statistics}
import cn.pandadb.kernel.kv.GraphFacadeWithPPD
import cn.pandadb.kernel.kv.index.IndexStoreAPI
import cn.pandadb.kernel.kv.node.NodeStoreAPI
import cn.pandadb.kernel.kv.relation.RelationStoreAPI
import cn.pandadb.kernel.optimizer.AnyValue
import cn.pandadb.kernel.store.{FileBasedIdGen, NodeStoreSPI, RelationStoreSPI, StoredNodeWithProperty}
import cn.pandadb.kernel.util.Profiler
import org.apache.commons.io.FileUtils
import org.junit.{Assert, Before, Test}
import org.opencypher.okapi.api.graph.CypherResult
import org.opencypher.okapi.api.value.CypherValue.{Node, Relationship}

import scala.collection.mutable.ArrayBuffer

class GraphFacadeWithPPDTest {

  //  var nodeLabelStore: NameStore = _
  //  var relLabelStore: NameStore = _
  //  var propNameStore: NameStore = _
  //  var graphStore: RocksDBGraphAPI = _

  var nodeStore: NodeStoreSPI = _
  var relationStore: RelationStoreSPI = _
  var indexStore: IndexStoreAPI = _
  var statistics: Statistics = _
  var graphFacade: GraphFacadeWithPPD = _


  @Before
  def setup(): Unit = {
    FileUtils.deleteDirectory(new File("./testdata"))
    new File("./testdata/output").mkdirs()

    val dbPath = "./testdata"
    graphFacade = new GraphFacadeWithPPD(dbPath,
      {}
    )
    nodeStore = graphFacade.nodeStore
    relationStore = graphFacade.relationStore
    indexStore = graphFacade.indexStore
    statistics = graphFacade.statistics
  }



  @Test
  def testCreate(): Unit ={
    val res = graphFacade.cypher("create (n:person{name:'joejoe',flag:true, idStr:'qq'}) ")
    //nodeStore.allNodes().foreach(n=>println(n.properties))
//    val res2 = graphFacade.cypher("match (n:person) where n.name='joejoe' return n")
//    res2.show
//    val res3 = graphFacade.cypher("match (n) return n")
//    res3.show

    Profiler.timing({
      println("Test preheat")
      val label = nodeStore.getLabelId("label0")
      val prop = nodeStore.getPropertyKeyId("flag")
      println(prop)
      var count = 0
      val nodes = nodeStore.getNodesByLabel(label)
      nodes.take(10).foreach(n=>print(n.properties))
      val res = ArrayBuffer[StoredNodeWithProperty]()
      while (nodes.hasNext && count < 10) {
        val node = nodes.next()
        if (node.properties.getOrElse(prop, null) == false) {
          res += node
          count += 1
        }
      }
      print(count)
    })
  }

  @Test
  def index(): Unit ={
    val createCypher = Array(
      "create (n:person{name: 'bob', age: 31})",
      "create (n:person{name: 'alice', age: 31.5})",
      "create (n:person{name: 'bobobobob', age: 3})",
      "create (n:person{name: 22, age: '31'})",
      "create (n:person{name: 'b', age: -100})",
      "create (n:person{name: 'ba', age: -100.000001})",
      "create (n:person{name: 'bob', age: 32})",
      "create (n:worker{name: 'alice', age: 31.5})",
      "create (n:worker{name: 'bobobobob', age: 3})",
      "create (n:worker{name: 22, age: '31'})",
      "create (n:worker{name: 'b', age: -100})",
      "create (n:worker{name: 'ba', age: -100.000001})",
    )
    createCypher.foreach{
      c=>
        println(c)
        graphFacade.cypher(c)
    }

    graphFacade.createIndexOnNode("person", Set("name"))
    graphFacade.createIndexOnNode("person", Set("age"))

    val matchCypher = Array(
      "match (n:person) where n.name = 'bob' return n",
      "match (n:person) where n.name = 22 return n",
      "match (n:person) where n.name = 'alice' return n",
//      "match (n:person) where n.name starts with 'alice' return n",
//      "match (n:person) where n.name starts with 'b' return n",
      "match (n:person) where n.age = 31 return n",
      "match (n:person) where n.age < 31 return n",
      "match (n:person) where n.age <= 31 return n",
      "match (n:person) where n.age <= 31.00 return n",
      "match (n:person) where n.age > -100 return n",
      "match (n:person) where n.age > -100.0000001 return n",
    )

    matchCypher.foreach{
      c=>
        println(c)
        graphFacade.cypher(c).show
    }
  }
}
