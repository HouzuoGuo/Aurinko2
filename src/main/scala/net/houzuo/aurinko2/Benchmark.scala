package net.houzuo.aurinko2

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.Socket

import scala.util.Random
import scala.xml.Node
import scala.math.min

import net.houzuo.aurinko2.logic.Database

object TimedExecution {

  def average(caption: String, iterations: Int)(func: => Unit) {
    val start = System.currentTimeMillis()
    func
    val duration = (System.currentTimeMillis() - start).toDouble
    println(caption)
    println(s"${duration / iterations} ms per iteration; ${iterations / duration * 1000} iterations per second; $duration ms in total.")
  }
}

/**
 * Connect to locally hosted server and do a series of benchmark tests.
 */
object Benchmark {
  private val random = new Random()

  /** Send an Aurinko2 command through the socket, expect but discard any server response. */
  def command(in: BufferedReader, out: PrintWriter, cmd: Node) {
    out.println(cmd)
    out.println(<go/>)
    while (true) {
      val response = in.readLine
      if (response == null)
        return
      if ("<done/>".equals(response))
        return
    }
  }
  def apply(db: Database, port: Int) {
    println("Please be patient, benchmark may take several minutes!")
    println("Wait 20 seconds for things to cool down...")
    Thread sleep 20000
    val single_sock = new Socket("localhost", port)
    val single_in = new BufferedReader(new InputStreamReader(single_sock getInputStream))
    val single_out = new PrintWriter(single_sock getOutputStream, true)

    try {
      // Prepare benchmark collections
      // 2 collections
      command(single_in, single_out, <drop col="__benchmark1"/>)
      command(single_in, single_out, <drop col="__benchmark2"/>)
      command(single_in, single_out, <create col="__benchmark1"/>)
      command(single_in, single_out, <create col="__benchmark2"/>)

      // 3 indexes each
      command(single_in, single_out, <hash-index col="__benchmark1" hash-bits="14" bucket-size="200"><path>i1</path><path>i2</path></hash-index>)
      command(single_in, single_out, <hash-index col="__benchmark1" hash-bits="14" bucket-size="200"><path>j1</path><path>j2</path></hash-index>)
      command(single_in, single_out, <hash-index col="__benchmark1" hash-bits="14" bucket-size="200"><path>k1</path><path>k2</path></hash-index>)
      command(single_in, single_out, <hash-index col="__benchmark2" hash-bits="14" bucket-size="200"><path>i1</path><path>i2</path></hash-index>)
      command(single_in, single_out, <hash-index col="__benchmark2" hash-bits="14" bucket-size="200"><path>j1</path><path>j2</path></hash-index>)
      command(single_in, single_out, <hash-index col="__benchmark2" hash-bits="14" bucket-size="200"><path>k1</path><path>k2</path></hash-index>)

      val iterations = 200000
      val numThreads = min(Runtime.getRuntime.availableProcessors * 100, 800) // Good to have many IO connections

      // Insert 400k documents (total) into two collections 
      {
        val inserts = {
          for (i <- 1 to numThreads) yield new Thread {
            val sock = new Socket("localhost", port)
            val in = new BufferedReader(new InputStreamReader(sock getInputStream))
            val out = new PrintWriter(sock getOutputStream, true)
            override def run() {
              for (j <- 1 to iterations / numThreads)
                command(in, out, <insert col="__benchmark1">
                                   <root>
                                     <i1><i2>{ random nextInt iterations }</i2></i1>
                                     <j1><j2>{ random nextInt iterations }</j2></j1>
                                     <k1><k2>{ random nextInt iterations }</k2></k1>
                                   </root>
                                 </insert>)
              in.close()
              out.close()
              sock.close()
            }
          }
        }.toList ::: {
          for (i <- 1 to numThreads) yield new Thread {
            val sock = new Socket("localhost", port)
            val in = new BufferedReader(new InputStreamReader(sock getInputStream))
            val out = new PrintWriter(sock getOutputStream, true)
            override def run() {
              for (j <- 1 to iterations / numThreads)
                command(in, out, <insert col="__benchmark2">
                                   <root>
                                     <i1><i2>{ random nextInt iterations }</i2></i1>
                                     <j1><j2>{ random nextInt iterations }</j2></j1>
                                     <k1><k2>{ random nextInt iterations }</k2></k1>
                                   </root>
                                 </insert>)
              in.close()
              out.close()
              sock.close()
            }
          }
        }.toList

        TimedExecution.average(s"Insert ${iterations * 2} documents", iterations * 2) {
          inserts foreach { _ start }
          inserts foreach { _ join }
        }
      }

      // Update 400k documents (total) in two collections
      {
        val docs1 = db.get("__benchmark1").all toArray
        val docs2 = db.get("__benchmark2").all toArray
        val updates = {
          for (i <- 1 to numThreads) yield new Thread {
            val sock = new Socket("localhost", port)
            val in = new BufferedReader(new InputStreamReader(sock getInputStream))
            val out = new PrintWriter(sock getOutputStream, true)
            override def run() {
              for (j <- 1 to iterations / numThreads)
                command(in, out, <update col="__benchmark1" id={ docs1(random nextInt iterations) toString }>
                                   <root>
                                     <i1><i2>{ random nextInt iterations }</i2></i1>
                                     <j1><j2>{ random nextInt iterations }</j2></j1>
                                     <k1><k2>{ random nextInt iterations }</k2></k1>
                                   </root>
                                 </update>)
              in.close()
              out.close()
              sock.close()
            }
          }
        }.toList ::: {
          for (i <- 1 to numThreads) yield new Thread {
            val sock = new Socket("localhost", port)
            val in = new BufferedReader(new InputStreamReader(sock getInputStream))
            val out = new PrintWriter(sock getOutputStream, true)
            override def run() {
              for (j <- 1 to iterations / numThreads)
                command(in, out, <update col="__benchmark2" id={ docs2(random nextInt iterations) toString }>
                                   <root>
                                     <i1><i2>{ random nextInt iterations }</i2></i1>
                                     <j1><j2>{ random nextInt iterations }</j2></j1>
                                     <k1><k2>{ random nextInt iterations }</k2></k1>
                                   </root>
                                 </update>)
              in.close()
              out.close()
              sock.close()
            }
          }
        }.toList

        TimedExecution.average(s"Update ${iterations * 2} documents", iterations * 2) {
          updates foreach { _ start }
          updates foreach { _ join }
        }
      }

      // Delete 400k documents (estimated, total) in two collections
      {
        val docs1 = db.get("__benchmark1").all toArray
        val docs2 = db.get("__benchmark2").all toArray
        val deletes = {
          for (i <- 1 to numThreads) yield new Thread {
            val sock = new Socket("localhost", port)
            val in = new BufferedReader(new InputStreamReader(sock getInputStream))
            val out = new PrintWriter(sock getOutputStream, true)
            override def run() {
              for (j <- 1 to iterations / numThreads)
                command(in, out, <delete col="__benchmark1" id={ docs1(random nextInt iterations) toString }/>)
              in.close()
              out.close()
              sock.close()
            }
          }
        }.toList ::: {
          for (i <- 1 to numThreads) yield new Thread {
            val sock = new Socket("localhost", port)
            val in = new BufferedReader(new InputStreamReader(sock getInputStream))
            val out = new PrintWriter(sock getOutputStream, true)
            override def run() {
              for (j <- 1 to iterations / numThreads)
                command(in, out, <delete col="__benchmark2" id={ docs2(random nextInt iterations) toString }/>)
              in.close()
              out.close()
              sock.close()
            }
          }
        }.toList

        TimedExecution.average(s"Delete ${iterations * 2} documents", iterations * 2) {
          deletes foreach { _ start }
          deletes foreach { _ join }
        }

        // 400k queries (3 lookups each) in two collections
        {
          val queries = {
            for (i <- 1 to numThreads) yield new Thread {
              val sock = new Socket("localhost", port)
              val in = new BufferedReader(new InputStreamReader(sock getInputStream))
              val out = new PrintWriter(sock getOutputStream, true)
              override def run() {
                for (j <- 1 to iterations / numThreads)
                  command(in, out, <q col="__benchmark1">
                                     <diff>
                                       <intersect>
                                         <eq limit={ random nextInt 100 toString }><to><i2>{ random nextInt iterations }</i2></to><in><path>i1</path><path>i2</path></in></eq>
                                         <eq limit={ random nextInt 100 toString }><to><j2>{ random nextInt iterations }</j2></to><in><path>j1</path><path>j2</path></in></eq>
                                       </intersect>
                                       <eq limit={ random nextInt 100 toString }><to><k2>{ random nextInt iterations }</k2></to><in><path>k1</path><path>k2</path></in></eq>
                                     </diff>
                                   </q>)
                in.close()
                out.close()
                sock.close()
              }
            }
          }.toList ::: {
            for (i <- 1 to numThreads) yield new Thread {
              val sock = new Socket("localhost", port)
              val in = new BufferedReader(new InputStreamReader(sock getInputStream))
              val out = new PrintWriter(sock getOutputStream, true)
              override def run() {
                for (j <- 1 to iterations / numThreads)
                  command(in, out, <q col="__benchmark2">
                                     <diff>
                                       <intersect>
                                         <eq limit={ random nextInt 100 toString }><to><i2>{ random nextInt iterations }</i2></to><in><path>i1</path><path>i2</path></in></eq>
                                         <eq limit={ random nextInt 100 toString }><to><j2>{ random nextInt iterations }</j2></to><in><path>j1</path><path>j2</path></in></eq>
                                       </intersect>
                                       <eq limit={ random nextInt 100 toString }><to><k2>{ random nextInt iterations }</k2></to><in><path>k1</path><path>k2</path></in></eq>
                                     </diff>
                                   </q>)
                in.close()
                out.close()
                sock.close()
              }
            }
          }.toList

          TimedExecution.average(s"${iterations * 2} queries", iterations * 2) {
            queries foreach { _ start }
            queries foreach { _ join }
          }
        }
      }
    } finally {
      // Clean-up
      command(single_in, single_out, <drop col="__benchmark1"/>)
      command(single_in, single_out, <drop col="__benchmark2"/>)
    }
  }
}
