package cn.pandadb.kv

import java.nio.ByteBuffer

import cn.pandadb.kernel.kv.index.NodeIndex
import cn.pandadb.kernel.kv.{ByteUtils, KeyHandler, RocksDBStorage}
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.{ReadOptions, RocksDB}
import org.apache.commons.lang3.RandomStringUtils

import scala.io.Source
import scala.tools.nsc.profile.Profiler
import scala.util.Random

@Test
class NodeIndexTest extends Assert {


  val path = "C:\\rocksDB"
  val hddPath = "D:\\rocksDB"

  @Test
  def rocksDBTest(): Unit = {
    val db:RocksDB = RocksDBStorage.getDB(path+"/test1")
    Assert.assertNotNull(db)

    db.put("aaa".getBytes(),"bbb".getBytes())
    db.put("hello".getBytes(),"world".getBytes())
    db.put("hello1".getBytes(),"world1".getBytes())
    db.put("hello2".getBytes(),"world2".getBytes())
    db.put("eee".getBytes(),"fff".getBytes())
    db.put("zzz".getBytes(),"zzzz".getBytes())
    db.put("hello3".getBytes(),"world3".getBytes())
    db.put("hhh".getBytes(),"hhh".getBytes())

    Assert.assertArrayEquals(db.get("hello".getBytes()), "world".getBytes())
    Assert.assertNull(db.get("hel".getBytes()))

    val iter = db.newIterator()
    val prefix = "hel".getBytes()
    iter.seek(prefix)
    while(iter.isValid) {
      println(iter.value().length)
      if (iter.value().length>0) {
        println(new String(iter.value()))
      }
      //      Assert.assertArrayEquals(iter.value(), "world".getBytes())
      iter.next()
    }
    iter.close()
    db.close()
  }

  def long2Bytes(long: Long): Array[Byte] ={
    val b = new Array[Byte](8)
    ByteUtils.setLong(b,0,long)
    b
  }

  @Test
  def indexBaseTest(): Unit= {
    val db:RocksDB = RocksDBStorage.getDB(path+"/test2")
    val LABEL = 1
    val PROPS = Array[Int](1)
    val ni = new NodeIndex(db)

    // drop index
    ni.dropIndex(LABEL, PROPS)

    // create index
    val indexId = ni.createIndex(LABEL, PROPS)
    val indexId2 = ni.createIndex(LABEL, PROPS)
    assert(indexId == indexId2)

    // get Index id
    val indexId3 = ni.getIndexId(LABEL, PROPS)
    assert(indexId == indexId3)

    // insert index
    val data = (0 until 50).iterator.map(_.toLong).map{
      l=>
        val v = (l/10).toInt
        (v, l)
    }
    ni.insertIndexRecordBatch(ni.getIndexId(LABEL, PROPS),data)

    // find 20-29
    Assert.assertArrayEquals(ni.find(indexId, 2).toArray,
      Array[Long](20,21,22,23,24,25,26,27,28,29)
    )

    // create and index2
    val LABEL2 = 2
    val PROPS2 = Array[Int](2)
    val indexId4 = ni.createIndex(LABEL2, PROPS2)
    assert(indexId4 == ni.getIndexId(LABEL2, PROPS2))
    val data2 = (0 until 50).iterator.map(_.toLong).map{
      l=>
        val v = (l%10).toInt
        (v, l)
    }
    ni.insertIndexRecordBatch(ni.getIndexId(LABEL2, PROPS2), data2)

    //drop index1
    ni.dropIndex(LABEL, PROPS)

    // find 20-29 by index 1
    assert(ni.find(indexId, 2).toArray.length==0)

    // find 1,11,21,31,41 by index 2
    Assert.assertArrayEquals(ni.find(ni.getIndexId(LABEL2, PROPS2), 1).toArray,
      Array[Long](1,11,21,31,41))
  }

  @Test
  def indexBigTest(): Unit= {
//    val db:RocksDB = RocksDBStorage.getDB(path+"/test3")
//    val ni = new NodeIndex(db)
//    val t0 = System.currentTimeMillis()
//    for (node <- 0 until 10000000) {
//      ni.writeIndexRow(1003, long2Bytes(scala.util.Random.nextInt(1000).toLong), node.toLong)
//    }
//    val t1 = System.currentTimeMillis()
//    println("create time: ", t1-t0)//47624
//    for (i <- 0 until 1000){
//      ni.find(1003, long2Bytes(100.toLong)).toList.length
//    }
//    val t2 = System.currentTimeMillis()
//    println("search 1000 time: ", t2-t1)//2774
  }

  @Test
  def stringIndexTest(): Unit = {
    val data = Map[Int,String](
      0->"张三",
      1->"李四",
      2->"王五",
      3->"张三",
      4->"张三丰",
      5->"李四光",
      6->"王五",
      7->"PandaDB is a Intelligent Graph Database.(",
      8->"PandaDB",
      9->"PandaDB is a Intelligent Graph Database.").map{
      v=>
        (v._1.toLong, v._2)
    }.iterator
//    println(data.length)
    val db:RocksDB = RocksDBStorage.getDB(path+"/test5")
    val ni = new NodeIndex(db)
    // create and insert
    val indexId = ni.createIndex(5,Array[Int](5))
    data.foreach{d=>ni.insertIndexRecord(indexId, d._2, d._1)}
//    showAll(db)
    // search
    Assert.assertArrayEquals(Array[Long](0, 3), ni.find(indexId, "张三").toArray)
//    ni.find(indexId, "王五").toArray.foreach(println(_))
    Assert.assertArrayEquals(Array[Long](4), ni.find(indexId, "张三丰").toArray)
    Assert.assertArrayEquals(Array[Long](8), ni.find(indexId, "PandaDB").toArray)
    Assert.assertArrayEquals(Array[Long](9), ni.find(indexId, "PandaDB is a Intelligent Graph Database.").toArray)

  }


  @Test
  def stringStartWithTest(): Unit = {
    val data = Map[Int,String](
      0->"张三",
      1->"张四",
      2->"张五",
      3->"张三",
      4->"张三丰",
      5->"张四四",
      6->"王五",
      7->"PandaDB is a Intelligent Graph Database.(",
      8->"PandaDB",
      9->"PandaDB is a Intelligent Graph Database.").map{
      v=>
        (v._1.toLong, v._2)
    }.iterator
    val db:RocksDB = RocksDBStorage.getDB(path+"/test8")
    val ni = new NodeIndex(db)
    // create and insert
    val indexId = ni.createIndex(5,Array[Int](5))
    data.foreach(d=> ni.insertIndexRecord(indexId, d._2, d._1))
    // search
    Assert.assertArrayEquals(Array[Long](0, 3, 4), ni.findStringStartWith(indexId, "张三").toArray.sorted)
    Assert.assertArrayEquals(Array[Long](0, 1, 2, 3, 4, 5), ni.findStringStartWith(indexId, "张").toArray.sorted)
    Assert.assertArrayEquals(Array[Long](7,8,9), ni.findStringStartWith(indexId, "PandaDB").toArray.sorted)
  }

  @Test
  def intRangeTest(): Unit = {
    val data = Map[Int,Int](
      0->1,
      1->2,
      2->10,
      3->20,
      4->100,
      5->0,
      6-> -1,
      7-> -10,
      8-> -20,
      9-> -100).map{
      v=>
        (v._2,  v._1.toLong)
    }.iterator
    val db:RocksDB = RocksDBStorage.getDB(path+"/test6")
    val ni = new NodeIndex(db)
    val indexId = ni.createIndex(6,Array[Int](6))
    data.foreach{
      d=>
        ni.insertIndexRecord(indexId, d._1, d._2)
    }
    Assert.assertArrayEquals(Array[Long](9,8,7,6,5,0,1,2,3,4), ni.findIntRange(indexId).toArray)
//    ni.findIntRange(indexId, 3,20).toList.foreach(println(_))
    Assert.assertArrayEquals(Array[Long](2,3), ni.findIntRange(indexId, 3,100).toArray)
    Assert.assertArrayEquals(Array[Long](5,0,1,2,3), ni.findIntRange(indexId, 0,99).toArray)
    Assert.assertArrayEquals(Array[Long](8,7,6,5,0,1,2,3), ni.findIntRange(indexId, -60,60).toArray)
  }

  @Test
  def floatRangeTest(): Unit = {
    val data = Map[Int,Double](
      0-> -100,
      1 -> -99.9999,
      2 -> -99,
      3 -> -3.1415926,
      4 -> -0.618,
      5 -> 0,
      6 -> 0.0000001,
      7 -> 1,
      8 -> 3.1415926,
      9 -> 18290.87817,
      10-> 18290.878171,
      11-> Double.MinValue,
      12-> Double.MaxValue).map{
      v=>
        (v._2, v._1.toLong)
    }.iterator
    val db:RocksDB = RocksDBStorage.getDB(path+"/test7")
    val ni = new NodeIndex(db)
    ni.dropIndex(7,Array[Int](7))
    val indexId = ni.createIndex(7,Array[Int](7))
    data.foreach(d => ni.insertIndexRecord(indexId,d._1, d._2))

//    ni.findFloatRange(indexId, -99999.toFloat, Double.MaxValue.toFloat).foreach(println(_))
//    ni.findFloatRange(indexId, -99999.toFloat, Double.MaxValue.toFloat).foreach(println(_))
    Assert.assertArrayEquals(Array[Long](0,1,2,3,4,5,6,7,8,9,10), ni.findFloatRange(indexId, -99999, Double.MaxValue.toFloat).toArray)
    Assert.assertArrayEquals(Array[Long](2,3,4,5,6,7,8), ni.findFloatRange(indexId, -99, 100).toArray)
    Assert.assertArrayEquals(Array[Long](3,4,5,6), ni.findFloatRange(indexId, -4, 0.001.toFloat).toArray)

  }



  def showAll(db: RocksDB): Unit = {
    val it = db.newIterator()
    it.seek(Array[Byte](0))
    while (it.isValid){
      val key = it.key()
      println(key.map(b=>b.toHexString).toList)
      it.next()
    }
  }
}