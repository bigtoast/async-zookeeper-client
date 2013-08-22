package com.github.bigtoast.zookeeper

import java.io.File
import java.net.InetSocketAddress
import java.util.Random
import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}

class EmbeddedZookeeper(val connectString: String) {
  import EmbeddedZookeeper._

  val snapshotDir = tempDir()
  val logDir = tempDir()
  val tickTime = 500
  val zookeeper = new ZooKeeperServer(snapshotDir, logDir, tickTime)
  val port = connectString.split(":")(1).toInt
  val factory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 100)
  factory.startup(zookeeper)

  def shutdown(): Unit = {
    factory.shutdown()
    rm(logDir)
    rm(snapshotDir)
  }


}

object EmbeddedZookeeper {

  val random = new Random()

  /**
   * Delete file, in case of directory do that recursively
   * @param file file or directory
   */
  def rm(file: File): Unit = {
    rm(Seq(file))
  }

  /**
   * Delete sequence of files or directories, in case of directory do that recursively
   * @param file seq of files or directories
   */
  def rm(file: Seq[File]): Unit = {
    file.headOption match {
      case Some(f) if f.isDirectory =>
        rm(f.listFiles().toSeq ++ file.drop(1))
        f.delete
      case Some(f) =>
        f.delete
        rm(file.drop(1))
      case None =>
    }
  }


  def tempDir(): File = {
    val ioDir = System.getProperty("java.io.tmpdir")
    val f = new File(ioDir, "kafka-" + random.nextInt(1000000))
    f.mkdirs()
    f.deleteOnExit()
    f
  }
}
