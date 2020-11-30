package cn.pandadb.kv

import java.nio.ByteBuffer

import cn.pandadb.kernel.kv.{NodeIndex, RocksDBStorage}
import org.junit.{After, Assert, Before, Test}
import org.rocksdb.{ReadOptions, RocksDB}
import org.apache.commons.lang3.RandomStringUtils

import scala.tools.nsc.profile.Profiler

@Test
class NodeIndexTest extends Assert {


  val path = "C:\\rocksDB"
  val hddPath = "D:\\rocksDB"

  @Test
  def rocksDBTest = {
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

  @Test
  def indexBaseTest= {
    val db:RocksDB = RocksDBStorage.getDB(path+"/test2")
    val ni = new NodeIndex(db)
    ni.deleteIndexRows(1001)
    ni.deleteIndexRows(1002)
    for (node <- 0 until 50) {
      // create factors index
      for (i <- 1 until 10) {
        if (node%i==0)
          ni.writeIndexRow(1001, i.toLong, node.toLong)
      }
      // create ten index
      ni.writeIndexRow(1002, (node/10).toLong, node.toLong)
    }

    // find 0,7,14,21...
    Assert.assertArrayEquals(ni.find(1001, 7.toLong).toArray,
      Array[Long](0,7,14,21,28,35,42,49)
    )
    // find 30-39
    Assert.assertArrayEquals(ni.find(1002, 3.toLong).toArray,
      Array[Long](30,31,32,33,34,35,36,37,38,39)
    )
  }

  @Test
  def indexBigTest= {
    val db:RocksDB = RocksDBStorage.getDB(path+"/test3")
    val ni = new NodeIndex(db)
    val t0 = System.currentTimeMillis()
    for (node <- 0 until 10000000) {
      ni.writeIndexRow(1003, scala.util.Random.nextInt(1000).toLong, node.toLong)
    }
    val t1 = System.currentTimeMillis()
    println("create time: ", t1-t0)
    for (i <- 0 until 1000){
      ni.find(1003, 100.toLong).toList.length
    }
    val t2 = System.currentTimeMillis()
    println("search 1000 time: ", t2-t1)
  }

  @Test
  def stringIndexTest = {
    val db:RocksDB = RocksDBStorage.getDB(path+"/test4")
    val ni = new NodeIndex(db)
    val t0 = System.currentTimeMillis()
    for (node <- 0 until 10000000) {
      ni.writeIndexRow(1003, scala.util.Random.nextInt(1000).toLong, node.toLong)
    }
    val t1 = System.currentTimeMillis()
    println("create time: ", t1-t0)
    for (i <- 0 until 1000){
      ni.find(1003, 100.toLong).toList.length
    }
    val t2 = System.currentTimeMillis()
    println("search 1000 time: ", t2-t1)
  }

}