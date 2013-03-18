package net.houzuo.aurinko2

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.Socket
import java.util.logging.Logger

import scala.collection.mutable.ListBuffer
import scala.xml.Elem
import scala.xml.NodeSeq.seqToNodeSeq
import scala.xml.XML.loadString

import org.xml.sax.SAXParseException

import net.houzuo.aurinko2.logic.Database

object Worker {
  val LOG = Logger.getLogger(classOf[Worker].getName())
  val MAX_REQUEST_LENGTH = 20000000 // Max request length in bytes 
}

class Worker(val db: Database, val sock: Socket) {
  private val in = new BufferedReader(new InputStreamReader(sock.getInputStream()))
  private val out = new PrintWriter(sock.getOutputStream(), true)

  // Process requests until peer closes the connection
  try {
    val lines = new ListBuffer[String]
    var reqLength = 0
    while (true)
      this.synchronized {
        val line = in.readLine()
        if (line == null)
          throw new IOException("peer closed the connection")

        // Accumulate input until <go /> is given, which marks end of request
        val trimmed = line.trim().toLowerCase()
        if ("<go/>".equals(trimmed) || "<go />".equals(trimmed) || "<go></go>".equals(trimmed))
          if (lines.size == 0)
            respond { Some(<done/>) }
          else
            try {
              go(loadString(lines.mkString("")))
            } catch {
              case e: SAXParseException => out.println(<err>Unable to parse request as XML document</err>)
              case e: Exception =>
                respond { Some(<err>{ e.getMessage }</err>) }
                Worker.LOG.severe(s"${e.getMessage}: \n${e.getStackTraceString}")
            } finally { lines.clear(); reqLength = 0 }
        else {
          reqLength += line.length()
          if (reqLength > Worker.MAX_REQUEST_LENGTH) {
            respond { Some(<err>Request is too long</err>) }
            lines.clear()
            reqLength = 0
          } else
            lines += line

        }
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
  private def respond(res: => Option[Elem]) {
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
  private def go(req: Elem) {
    req.label.toLowerCase match {

      // Get all collection names
      case "all" => respond { Some(<all>{ for (col <- db.all) yield <col>{ col }</col> }</all>) }

      // Get IO queue length on all collections
      case "load" => respond {
        Some(<load>{
          for (col <- db.all) yield <col name={ col }>{
            for (thing <- db.get(col).load) yield <queue name={ thing._1.toString }>{ thing._2 }</queue>
          }</col>
        }</load>)
      }

      // Create collection
      case "create" => respond {
        req.attribute("name") match {
          case Some(name) =>
            db.create(name.text); None
          case None => Some(<err>Please specify collection name in "name" attribute</err>)
        }
      }

      // Rename collection
      case "rename" => respond {
        req.attribute("old") match {
          case Some(oldName) =>
            req.attribute("new") match {
              case Some(newName) =>
                db.rename(oldName.text, newName.text); None
              case None => Some(<err>Please spicify new collection in "new" attribute</err>)
            }
          case None => Some(<err>Please specify old collection in "old" attribute</err>)
        }
      }

      // Drop collection
      case "drop" => respond {
        req.attribute("name") match {
          case Some(name) =>
            db.drop(name.text); None
          case None => Some(<err>Please specify collection name in "name" attribute</err>)
        }
      }

      // Repair collection
      case "repair" => respond {
        req.attribute("name") match {
          case Some(name) =>
            db.repair(name.text); None
          case None => Some(<err>Please specify collection name in "name" attribute</err>)
        }
      }

      // Insert documents
      case "insert" => respond {
        req.attribute("col") match {
          case Some(colName) =>
            val col = db.get(colName.text)
            Some(<inserted>{ for (doc <- req.child) yield <id>{ col.insert(doc.asInstanceOf[Elem]) }</id> }</inserted>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Update documents
      case "update" => respond {
        req.attribute("col") match {
          case Some(colName) =>
            val col = db.get(colName.text)
            Some(<updated>{
              for (update <- req.child)
                yield update.attribute("id") match {
                case Some(oldID) =>
                  <id old={ oldID }>{ col.update(oldID.text.toInt, update.child(0).asInstanceOf[Elem]).get }</id>
                case None => throw new Exception("Please specify ID of all documents to update")
              }
            }</updated>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Delete documents
      case "delete" => respond {
        req.attribute("col") match {
          case Some(colName) =>
            val col = db.get(colName.text)
            for (toDelete <- req.child)
              col.delete(toDelete.text.toInt)
            None
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Get all documents
      case "findall" => respond {
        req.attribute("col") match {
          case Some(colName) =>
            val col = db.get(colName.text)
            Some(<collection>{
              for (
                docID <- req.attribute("limit") match {
                  case Some(number) => col.all.take(number.text.toInt)
                  case None         => col.all
                }
              ) yield col.read(docID).get
            }</collection>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }
    }
  }
}