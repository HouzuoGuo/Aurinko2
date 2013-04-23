package net.houzuo.aurinko2.test.logic

import scala.util.Random

import org.scalatest.FunSuite

import TemporaryFactory.collection
import net.houzuo.aurinko2.logic.Query
import net.houzuo.aurinko2.test.TimedExecution.average
import net.houzuo.aurinko2.test.TimedExecution.time

class Benchmark extends FunSuite {

  val random = new Random

  /*
   * This benchmark spawns multiple threads to simulate concurrent read and write operations.
   */
  test("collection performance benchmark") {
    val numThreads = Runtime.getRuntime.availableProcessors * 4
    val iterations = 200000
    val col = collection
    col.index(List("i1", "i2"), 14, 200)
    col.index(List("j1", "j2"), 14, 200)
    col.index(List("k1", "k2"), 14, 200)

    println("Insert 200k documents")
    val inserts = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads)
          col.insert(
            <root>
              <i1><i2>{ random nextInt iterations }</i2></i1>
              <j1><j2>{ random nextInt iterations }</j2></j1>
              <k1><k2>{ random nextInt iterations }</k2></k1>
              <more>
                ssh-rsa
AAAAB3NzaC1yc2EAAAADAQABAAABAQC4KLmaRhJ/
DKA/LnU8sI/gghFEiy+gA8P5qz3UfOoA6/uPeM2E3V139EmQ7J2C/
/+ceyO4OehcZY3BxyAe03SrQMtJSMGn0BKP79HDKhYOjMaldzAKz2
NJLxUdmzzLb4wV6I18pr7dTm1IkAhhS/GMkYAWqVhy3FnG2mc99FO
dKCZvSjzlw96or+5R0xOGPHjbM2qrZUzRl0aDkv52Nll2hrwFIhUC
JF4jxLyGmoeZVzyv4gY/Mz9oaNLNWhjt1HOUJ6pakY7FLDHCqvn4P
MRu4rC1LEJHL8CH++xTk99YOYsW+hylbTayM7GCkgEX1gjVg/p6ld
oEiNaBnmNgPavP howard@howard
              </more><more>
                       ssh-rsa
AAAAB3NzaC1yc2EAAAADAQABAAABAQC4KLmaRhJ/
DKA/LnU8sI/gghFEiy+gA8P5qz3UfOoA6/uPeM2E3V139EmQ7J2C/
/+ceyO4OehcZY3BxyAe03SrQMtJSMGn0BKP79HDKhYOjMaldzAKz2
NJLxUdmzzLb4wV6I18pr7dTm1IkAhhS/GMkYAWqVhy3FnG2mc99FO
dKCZvSjzlw96or+5R0xOGPHjbM2qrZUzRl0aDkv52Nll2hrwFIhUC
JF4jxLyGmoeZVzyv4gY/Mz9oaNLNWhjt1HOUJ6pakY7FLDHCqvn4P
MRu4rC1LEJHL8CH++xTk99YOYsW+hylbTayM7GCkgEX1gjVg/p6ld
oEiNaBnmNgPavP howard@howard
                     </more>
            </root>)
      }
    }
    average(iterations) {
      inserts foreach { _ start }
      inserts foreach { _ join }
    }

    println("Read 200k documents")
    val positions = col.all toArray
    val reads = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads)
          col.read(positions(random nextInt iterations))
      }
    }
    average(iterations) {
      reads foreach { _ start }
      reads foreach { _ join }
    }

    println("Update 200k documents")
    val updates = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads)
          col.update(positions(random nextInt iterations),
            <root>
              <i1><i2>{ random nextInt iterations }</i2></i1>
              <j1><j2>{ random nextInt iterations }</j2></j1>
              <k1><k2>{ random nextInt iterations }</k2></k1>
            </root>)
      }
    }
    average(iterations) {
      updates foreach { _ start }
      updates foreach { _ join }
    }

    println("200k queries")
    val queries = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads)
          new Query(col).eval(<root>
                                <diff>
                                  <intersect>
                                    <eq limit={ random nextInt 100 toString }><to><i2>{ random nextInt iterations }</i2></to><in><path>i1</path><path>i2</path></in></eq>
                                    <eq limit={ random nextInt 100 toString }><to><j2>{ random nextInt iterations }</j2></to><in><path>j1</path><path>j2</path></in></eq>
                                  </intersect>
                                  <eq limit={ random nextInt 100 toString }><to><k2>{ random nextInt iterations }</k2></to><in><path>k1</path><path>k2</path></in></eq>
                                </diff>
                              </root>)
      }
    }
    average(iterations) {
      queries foreach { _ start }
      queries foreach { _ join }
    }

    println("Delete 200k documents")
    val deletes = for (i <- 1 to numThreads) yield new Thread {
      override def run() {
        for (j <- 1 to iterations / numThreads)
          col.delete(positions(random nextInt iterations))
      }
    }
    average(iterations) {
      deletes foreach { _ start }
      deletes foreach { _ join }
    }

    println("Get all 200k document IDs")
    time(1) { col.all }

    println("Index 200k documents")
    col.unindex(List("i1", "i2"))
    time(1) { col.index(List("i1", "i2"), 14, 200) }
  }
}
