package net.houzuo.aurinko2

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.Socket
import java.util.logging.Logger

import scala.xml.XML.loadString

import net.houzuo.aurinko2.logic.Database

object Worker {
  private val LOG = Logger.getLogger(classOf[Worker].getName())
}

/** Handle network IO and process requests. */
class Worker(val db: Database, val con: Socket) {
  private val in = new BufferedReader(new InputStreamReader(con.getInputStream()))
  private val out = new PrintWriter(con.getOutputStream(), true)

  try {
    while (true) {
    }
  } catch {
    case e: Exception => Worker.LOG.warning("Net worker exception: " + e.getMessage())
  } finally {

    // Close reader/writer/socket
    try {
      in.close()
    } catch {
      case e: Exception => Worker.LOG.warning("Failed to close input reader")
    } finally {}
    try {
      out.close()
    } catch {
      case e: Exception => Worker.LOG.warning("Failed to close output writer")
    } finally {}
    try {
      if (!con.isClosed()) con.close()
    } catch { case e: Exception => Worker.LOG.warning("Failed to close socket") }
  }
}