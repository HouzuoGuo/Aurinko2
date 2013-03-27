package net.houzuo.aurinko2

import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.Socket
import scala.math.max
import scala.xml.Node
import scala.util.Random

object TimedExecution {

  def time(times: Int)(func: => Unit) {
    val start = System.currentTimeMillis()
    for (i <- 1 to times) func
    val duration = (System.currentTimeMillis() - start).toDouble
    println(s"${duration / times} ms per iteration; ${times / duration * 1000} iterations per second; $duration ms in total.")
  }

  def average(iterations: Int)(func: => Unit) {
    val start = System.currentTimeMillis()
    func
    val duration = (System.currentTimeMillis() - start).toDouble
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
  def apply(port: Int) {
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
      val numThreads = 400 // DB network clients better have more threads

      // Insert 400k documents (total) into two collections 
      {
        val inserts = {
          for (i <- 1 to numThreads) yield new Thread {
            val sock = new Socket("localhost", port)
            val in = new BufferedReader(new InputStreamReader(sock getInputStream))
            val out = new PrintWriter(sock getOutputStream, true)
            override def run() {
              for (j <- 1 to iterations / numThreads) {
                command(in, out, <insert col="__benchmark1">
                                   <root>
                                     <i1><i2>{ random.nextInt(iterations) }</i2></i1>
                                     <j1><j2>{ random.nextInt(iterations) }</j2></j1>
                                     <k1><k2>{ random.nextInt(iterations) }</k2></k1>
                                   </root>
                                 </insert>)

              }
            }
          }
        }.toList ::: {
          for (i <- 1 to numThreads) yield new Thread {
            val sock = new Socket("localhost", port)
            val in = new BufferedReader(new InputStreamReader(sock getInputStream))
            val out = new PrintWriter(sock getOutputStream, true)
            override def run() {
              for (j <- 1 to iterations / numThreads) {
                command(in, out, <insert col="__benchmark2">
                                   <root>
                                     <i1><i2>{ random.nextInt(iterations) }</i2></i1>
                                     <j1><j2>{ random.nextInt(iterations) }</j2></j1>
                                     <k1><k2>{ random.nextInt(iterations) }</k2></k1>
                                   </root>
                                 </insert>)

              }
            }
          }
        }.toList

        TimedExecution.average(iterations) {
          inserts foreach { _.start() }
          inserts foreach { _.join() }
        }
      }

    } finally {
      // Clean-up
      command(single_in, single_out, <drop col="__benchmark1"/>)
      command(single_in, single_out, <drop col="__benchmark2"/>)
    }
  }
}