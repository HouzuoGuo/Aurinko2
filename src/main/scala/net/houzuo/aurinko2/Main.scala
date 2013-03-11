package net.houzuo.aurinko2

object Main {

  /**
   * Number of threads used in:
   * - Indexing all documents
   * - Benchmark logic.Collection
   */
  var parallelLevel = Runtime.getRuntime().availableProcessors() * 2

  def main(args: Array[String]): Unit = {}
}
