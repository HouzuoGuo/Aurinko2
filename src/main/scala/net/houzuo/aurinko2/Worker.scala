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
import net.houzuo.aurinko2.logic.Query
import scala.xml.parsing.ConstructingParser
import scala.io.Source
import scala.xml.Node

object Worker {
  val LOG = Logger getLogger classOf[Worker].getName()
  val MAX_REQUEST_LENGTH = 20000000 // Max request length in bytes 
}

class Worker(val db: Database, val sock: Socket) {
  private val in = new BufferedReader(new InputStreamReader(sock getInputStream))
  private val out = new PrintWriter(sock getOutputStream, true)

  // Process requests until peer closes the connection
  try {
    val lines = new ListBuffer[String]
    var reqLength = 0
    while (true)
      this.synchronized {
        val line = in.readLine
        if (line == null)
          throw new IOException("peer closed the connection")

        // Accumulate input until <go /> is given, which marks end of request
        val trimmed = line.trim().toLowerCase
        if ("<go/>".equals(trimmed) || "<go />".equals(trimmed) || "<go></go>".equals(trimmed))
          if (lines.size == 0)
            respond { None }
          else
            try {
              go(ConstructingParser.fromSource(Source.fromString(lines mkString "\n"), true).document.docElem)
            } catch {
              case e: SAXParseException => out println <err>Request is not a valid XML document</err>
              case e: Exception =>
                respond { Some(<err>{ e.getMessage }</err>) }
                Worker.LOG severe s"${e.getMessage}: \n${e.getStackTraceString}"
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
    case e: IOException => Worker.LOG fine s"${sock.getRemoteSocketAddress toString} disconnected"
  } finally {
    try {
      in.close()
    } catch { case e: Exception => }
    try {
      out.close()
    } catch { case e: Exception => }
    try {
      sock.close()
    } catch { case e: Exception => }
  }

  /** Print response to output. */
  private def respond(res: => Option[Elem]) {
    try {
      res match {
        case Some(thing) => out println thing
        case None        =>
      }
    } catch {
      case e: Exception =>
        out println <err>{ e getMessage }</err>
        e printStackTrace
    } finally {
      out println <ok/>
    }
  }

  /** Process request. */
  private def go(req: Node) {
    req.label.toLowerCase match {

      // Get all collection names
      case "all" => respond { Some(<r>{ for (col <- db.all) yield <col>{ col }</col> }</r>) }

      // Get IO queue length on all collections
      case "load" => respond {
        Some(<r>{
          for (col <- db.all) yield <col name={ col }>{
            for (thing <- db get (col) load) yield <queue name={ thing._1.toString }>{ thing._2 }</queue>
          }</col>
        }</r>)
      }

      // Create collection
      case "create" => respond {
        req attribute "col" match {
          case Some(name) =>
            db create name.text; None
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Rename collection
      case "rename" =>
        respond {
          req attribute "col" match {
            case Some(oldName) =>
              req attribute "to" match {
                case Some(newName) =>
                  db rename (oldName.text, newName.text); None
                case None => Some(<err>Please spicify new collection in "to" attribute</err>)
              }
            case None => Some(<err>Please specify original collection name in "col" attribute</err>)
          }
        }

      // Drop collection
      case "drop" => respond {
        req attribute "col" match {
          case Some(name) =>
            db drop name.text; None
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Repair collection
      case "repair" => respond {
        req attribute "col" match {
          case Some(name) =>
            db repair name.text; None
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Insert documents
      case "insert" => respond {
        req attribute "col" match {
          case Some(colName) =>
            val col = db get colName.text
            Some(<r>{ col insert req.child.filter(_.isInstanceOf[Elem])(0).asInstanceOf[Elem] }</r>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Retrieve document by ID
      case "get" => respond {
        req attribute "col" match {
          case Some(colName) =>
            Some(<r>{
              db get colName.text read {
                req attribute "id" match {
                  case Some(id) => id.text toInt
                  case None     => throw new Exception("Please specify ID of document to retrie")
                }
              }
            }</r>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Update documents
      case "update" => respond {
        req attribute "col" match {
          case Some(colName) =>
            val col = db get colName.text
            Some(<r>{
              req.attribute("id") match {
                case Some(oldID) =>
                  <old>{ oldID text }</old>
                  <new>{ col.update(oldID.text toInt, req.child.filter(_.isInstanceOf[Elem])(0).asInstanceOf[Elem]) get }</new>
                case None => throw new Exception("Please specify ID of document to update")
              }
            }</r>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Delete documents
      case "delete" => respond {
        req attribute "col" match {
          case Some(colName) =>
            req attribute "id" match {
              case Some(id) => db.get(colName text).delete(id.text toInt)
              case None     => throw new Exception("Please specify document ID to delete")
            }
            None
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Get all documents
      case "findall" => respond {
        req attribute "col" match {
          case Some(colName) =>
            val col = db.get(colName.text)
            Some(<r>{
              for (
                docID <- req attribute ("limit") match {
                  case Some(number) => col.all.take(number.text toInt)
                  case None         => col.all
                }
              ) yield <doc id={ docID toString }>{ col.read(docID) get }</doc>
            }</r>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Get indexed paths in collection
      case "indexed" => respond {
        req attribute "col" match {
          case Some(colName) =>
            val col = db get colName.text
            Some(<r> {
              for (hash <- col.hashes) yield <index type="hash" hash-bits={ hash._2._2.hashBits toString } bucket-size={ hash._2._2.perBucket toString }> {
                for (pathSegment <- hash._1) yield <path>{ pathSegment }</path>
              }</index>
            }</r>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Put a hash index on collection
      case "hash-index" => respond {
        req attribute "col" match {
          case Some(colName) =>
            (db get colName.text).index(req.child.map(_.text) toList, req attribute ("hash-bits") match {
              case Some(number) => number.text toInt
              case None         => 12
            }, req attribute ("bucket-size") match {
              case Some(number) => number.text toInt
              case None         => 100
            })
            None
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Drop an index
      case "drop-index" => respond {
        req attribute "col" match {
          case Some(colName) =>
            (db get colName.text) unindex (req.child.map(_.text) toList)
            None
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Query and return document ID
      case "q" => respond {
        req attribute "col" match {
          case Some(colName) => Some(<r>{
            for (id <- new Query(db get colName.text) eval req) yield <id>{ id }</id>
          }</r>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Query and return document ID and content
      case "select" => respond {
        req attribute "col" match {
          case Some(colName) =>
            val col = db get colName.text
            Some(<r>{
              for (id <- new Query(col) eval req) yield <doc id={ id toString }>{ col read id get }</doc>
            }</r>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      // Query and return count of result documents
      case "count" => respond {
        req attribute "col" match {
          case Some(colName) =>
            val col = db get colName.text
            Some(<r>{ new Query(col) eval req size }</r>)
          case None => Some(<err>Please specify collection name in "col" attribute</err>)
        }
      }

      case "shutdown" => System exit 0

      case _          => Some(<err>Unknown command</err>)
    }
  }
}