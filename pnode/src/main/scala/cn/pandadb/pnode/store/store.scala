package cn.pandadb.pnode.store

import java.io._
import cn.pandadb.pnode.util.StreamExLike._
import io.netty.buffer.{ByteBuf, Unpooled}

trait LogRecord {
}

trait SequenceStore[T] {
  def list(): Stream[T]

  def save(t: Stream[T])

  def append(t: Iterable[T])

  def clear()
}

/**
 * [llll][...]
 * l-length of block (excluding d & l)
 */
trait FileBasedSequenceStore[T] extends SequenceStore[T] {
  def getFile: File

  def readObject(buf: ByteBuf): T

  private def createStream(dis: DataInputStream): Stream[T] = {
    try {
      val len = dis.readInt()

      val bytes = new Array[Byte](len)
      dis.read(bytes)
      val buf = Unpooled.wrappedBuffer(bytes)
      val t = readObject(buf)
      Stream.cons(t, {
        createStream(dis)
      })
    }
    catch {
      case _: EOFException => {
        dis.close()
        Stream.empty[T]
      }
      case e => throw e
    }
  }

  def list(): Stream[T] = {
    val dis = new DataInputStream(new FileInputStream(getFile))
    createStream(dis)
  }

  def clear(): Unit = {
    appender.getChannel.truncate(0)
  }

  def writeObject(buf: ByteBuf, t: T)

  protected def writeObjectBlock(buf: ByteBuf, t: T): Unit = {
    buf.writeInt(0) //length
    writeObject(buf, t)
    val len = buf.readableBytes()
    buf.setInt(1, len - 4)
  }

  def save(t: Stream[T]): Unit = {
    val appender = new FileOutputStream(getFile, false)
    val buf = Unpooled.buffer()
    t.foreach(writeObjectBlock(buf, _))
    appender.write(buf.array().slice(0, buf.readableBytes()))
    appender.close()
  }

  lazy val appender = new FileOutputStream(getFile, true)

  def append(t: T) = {
    append(Some(t))
  }

  def append(t: Iterable[T]) = {
    val buf = Unpooled.buffer()
    t.foreach(writeObjectBlock(buf, _))
    appender.write(buf.array().slice(0, buf.readableBytes()))
    appender.flush()
  }
}

class FileBasedLogStore(val file: File) extends FileBasedSequenceStore[LogRecord] {
  override def readObject(buf: ByteBuf): LogRecord = {
    val mark = buf.readByte()
    mark match {
      case 1 =>
        CreateNode(Node(buf.readLong()))

      case 11 =>
        DeleteNode(buf.readLong())

      case 2 =>
        CreateRelation(Relation(buf.readLong(), buf.readLong(), buf.readLong()))

      case 12 =>
        DeleteRelation(buf.readLong())
    }
  }

  override def getFile: File = file

  override def writeObject(buf: ByteBuf, t: LogRecord): Unit = {
    t match {
      case CreateNode(t) =>
        buf.writeByte(1)
        buf.writeLong(t.id)

      case CreateRelation(t) =>
        buf.writeByte(2)
        buf.writeLong(t.id).writeLong(t.from).writeLong(t.to)

      case DeleteNode(id) =>
        buf.writeByte(11)
        buf.writeLong(id)

      case DeleteRelation(id) =>
        buf.writeByte(12)
        buf.writeLong(id)
    }
  }
}

case class CreateNode(t: Node) extends LogRecord {

}

case class DeleteNode(id: Long) extends LogRecord {

}

case class CreateRelation(t: Relation) extends LogRecord {

}

case class DeleteRelation(id: Long) extends LogRecord {

}

case class Node(id: Long, labels: String*) {

}

case class Relation(id: Long, from: Long, to: Long, labels: String*) {

}

class FileBasedNodeStore(seqFile: File) extends FileBasedSequenceStore[Node] {

  override def readObject(buf: ByteBuf): Node =
    Node(buf.readLong(), buf.readString().split(";").toSeq: _*)

  override def getFile: File = seqFile

  override def writeObject(buf: ByteBuf, t: Node): Unit =
    buf.writeLong(t.id).writeString(t.labels.mkString(";"))
}

class FileBasedRelationStore(seqFile: File) extends FileBasedSequenceStore[Relation] {
  override def getFile: File = seqFile

  override def readObject(buf: ByteBuf): Relation = Relation(buf.readLong(), buf.readLong(), buf.readLong())

  override def writeObject(buf: ByteBuf, t: Relation): Unit =
    buf.writeLong(t.id).writeLong(t.from).writeLong(t.to).writeString(t.labels.mkString(";"))
}