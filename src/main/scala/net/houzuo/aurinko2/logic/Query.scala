package net.houzuo.aurinko2.logic

import java.util.logging.Logger
import scala.collection.mutable.ListBuffer
import scala.xml.Elem
import scala.xml.Node
import scala.xml.NodeSeq
import net.houzuo.aurinko2.storage.HashGet
import net.houzuo.aurinko2.storage.Output
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.concurrent.Promise

object Query {
  val LOG = Logger getLogger classOf[Query].getName
  val IO_TIMEOUT = 120000 // IO waiting timeout in milliseconds
}

class Query(val query: Node, val col: Collection) {
  Query.LOG fine query.toString

  private def sync[T](p: Promise[T]) = Await.result(p future, Query.IO_TIMEOUT millisecond)
  private def sync[T](p: Iterable[Promise[T]]) = p foreach { each => Await.result(each future, Query.IO_TIMEOUT millisecond) }

  /*
   * <eq limit="1" skip="1">
   *   <to>
   *     <performance>100%</performance>
   *   </to>
   *   <in>
   *     <path>sales</path>
   *     <path>product1</path>
   *   </in>
   * </eq>
   */
  def eq(op: Node): List[Int] = {
    val children = op.child.map { c => c.label -> c } toMap
    val path = children get "in" match {
      case Some(in) => in.child map (_.text) toList
      case None     => throw new Exception("Please specify matching path in <in></in>")
    }
    val limit = op attribute "limit" match {
      case Some(number) => number.text toInt
      case None         => Int.MaxValue
    }
    val skip = op attribute "limit" match {
      case Some(number) => number.text.toInt
      case None         => 0
    }
    val to = children get "to" match {
      case Some(doc) =>
        if (doc.child.size > 0 && doc.child(0).isInstanceOf[Elem])
          doc child 0 toString
        else
          doc text
      case None => throw new Exception("Please specify a document to match in <to></to>")
    }
    {
      // Index scan when possible
      if (col.hashes.contains(path)) {
        Query.LOG fine s"Index scan ${path}"
        val scan = HashGet(to.toString hashCode, limit, (_1, id) => {
          col.read(id) match {
            case Some(doc) => Collection.getIn(doc.asInstanceOf[NodeSeq], path) contains to
            case None      => false
          }
        }, new Output[List[Tuple2[Int, Int]]](null))
        sync(col.hashes.get(path).get._2.offer(scan))
        scan.result.data map (_._2)
      } else {
        // Otherwise, read each document and compare
        Query.LOG fine s"Inefficient! Collection scan ${path}"
        col.all filter { id =>
          col.read(id) match {
            case Some(doc) => Collection.getIn(doc.asInstanceOf[NodeSeq], path) contains to
            case None      => false
          }
        }
      }
    } drop skip take limit
  }

  def apply(): List[Int] = {
    val result = new ListBuffer[Int]
    for (op <- query.child) {
      op.label match {
        case "eq" => eq(op)
        case "ne" =>
        case _    => throw new Exception(s"Unknown query operation ${op.label}")
      }
    }
    result toList
  }
}