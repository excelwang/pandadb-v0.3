//package cn.pandadb.kv.performance
//
//import java.io.File
//import java.util.Date
//
//import cn.pandadb.kernel.kv.{NodeStore, NodeValue, RelationStore, RocksDBStorage}
//import org.junit.{After, Assert, Before, Test}
//import org.rocksdb.RocksDB
//
//import scala.io.Source
//
///**
// * @Author: Airzihao
// * @Description:
// * @Date: Created at 16:08 2020/11/28
// * @Modified By:
// */
//class RocksDBPerformanceTest {
//
//  val nodeFile = "D://dataset//graph500-22_unique_node";
//  val edgeFile = "D://dataset//graph500-22";
//  val dbPath = "C://rocksDB//performanceTest"
//  var db: RocksDB = null
//  var nodeStore: NodeStore = null
//  var relationStore: RelationStore = null
//
//  val expectedNodeValue = new NodeValue(3235527, Array(1, 2, 3), Map("idid" -> "3235527"))
//
//  @Before
//  def init(): Unit ={
//
//    val dir = new File(dbPath)
////    if (dir.exists()) {
////      _deleteDir(dir)
////    }
//    db = RocksDBStorage.getDB(dbPath)
//    nodeStore = new NodeStore(db)
//    relationStore = new RelationStore(db)
//  }
//
//  @After
//  def close(): Unit ={
//    db.close()
//  }
//
//  @Test
//  def writeTest(): Unit ={
//    val nodeIter = Source.fromFile(nodeFile, "utf-8").getLines()
//    val edgeIter = Source.fromFile(edgeFile, "utf-8").getLines()
//    val time0 =new Date().getTime
//    _loadAllNodes(nodeIter)
//    val time1 = new Date().getTime
//    println(s"load all nodes takes ${time1-time0} ms")
//    _loadAllEdges(edgeIter)
//    val time2 = new Date().getTime
//    println(s"load all nodes takes ${time1-time0} ms")
//    println(s"load all edges takes ${time2-time1} ms")
//  }
//
//  @Test
//  def readNodeTest(): Unit = {
////    val list: List[Long] = List(3235527, 527594, 3144469, 5282, 2303395)
//    val time0 = new Date().getTime
//    val nodeValue = findByNodeId(3235527)
////    list.foreach(id => findByNodeId(id))
//    val time1 = new Date().getTime
//    Assert.assertEquals(3235527, nodeValue.id)
//    Assert.assertArrayEquals(Array(1, 2, 3), nodeValue.labelIds)
//    Assert.assertEquals(Map("idid" -> "3235527"), nodeValue.properties)
//    println(s"find one node by id takes ${time1-time0} ms")
//  }
//
//  @Test
//  def readRelationTest(): Unit = {
//    val time0 = new Date().getTime
//    val iter = relationStore.getAll()
////      getAllRelation(KeyType.InEdge.id.toByte, 3235527)
//    val time1 = new Date().getTime
//    var degree = 0
//    while (iter.hasNext){
//      degree += 1
//      iter.next()
//    }
//    println(degree)
//    println(s"find one rel by startId takes ${time1-time0} ms")
//  }
//
//  def findByNodeProp(): Unit ={
//
//  }
//
//  def findByNodeId(id: Long): NodeValue = {
//    nodeStore.get(id)
//  }
//
//}
