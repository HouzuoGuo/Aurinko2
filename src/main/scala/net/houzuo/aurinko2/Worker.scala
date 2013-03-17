package net.houzuo.aurinko2

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.Socket
import java.util.logging.Logger

import scala.annotation.migration
import scala.collection.mutable.ListBuffer
import scala.xml.Elem
import scala.xml.NodeSeq.seqToNodeSeq
import scala.xml.XML.loadString

import org.xml.sax.SAXParseException

import net.houzuo.aurinko2.logic.Database

object Worker {
  val LOG = Logger.getLogger(classOf[Worker].getName())
}

class Worker(val db: Database, val sock: Socket) {
  val in = new BufferedReader(new InputStreamReader(sock.getInputStream()))
  val out = new PrintWriter(sock.getOutputStream(), true)

  // Process requests until peer closes the connection
  try {
    val lines = new ListBuffer[String]
    while (true)
      this.synchronized {
        val line = in.readLine()
        if (line == null)
          throw new IOException("peer closed the connection")

        // Accumulate input until <go /> is given, which marks end of request
        val trimmed = line.trim().toLowerCase()
        if ("<go/>".equals(trimmed) || "<go />".equals(trimmed) || "<go></go>".equals(trimmed))
          try {
            go(loadString(lines.mkString("")))
          } catch {
            case e: SAXParseException => out.println(<err>Unable to parse request as XML document</err>)
          } finally { lines.clear() }
        else
          lines += line
      }
  } catch {
    case e: IOException => Worker.LOG.warning(s"${sock.getRemoteSocketAddress().toString()} disconnected")
  } finally {
    try {
      in.close()
    } catch { case e => }
    try {
      out.close()
    } catch { case e => }
    try {
      sock.close()
    } catch { case e => }
  }

  /** Print response to output. */
  def respond(res: => Option[Elem]) {
    try {
      res match {
        case Some(thing) => out.println(thing)
        case None        =>
      }
    } catch {
      case e: Exception => out.println(<err>{ e.getMessage() }</err>)
    } finally {
      out.println(<done/>)
    }
  }

  /** Process request. */
  def go(req: Elem) {
    req match {

      // Ping server
      case <ping/> => respond { Some(<pong/>) }

      // Get all collection names
      case <all/>  => respond { Some(<all>{ for (col <- db.all) yield <col>{ col }</col> }</all>) }

      // Get all collection data file queues' size
      case <load/> => respond {
        Some(<load>
               {
                 for (col <- db.all) yield <col name={ col }>{
                   for (thing <- db.get(col).load) yield <queue a={ thing._1.toString }>{ thing._2 }</queue>
                 }</col>
               }
             </load>)
      }

      // Create a collection
      case <create/> => respond {
        req.attribute("name") match {
          case Some(name) =>
            db.create(name.text); None
          case None => Some(<err>Please specify new collection name in "name" attribute</err>)
        }
      }
    }
  }
}