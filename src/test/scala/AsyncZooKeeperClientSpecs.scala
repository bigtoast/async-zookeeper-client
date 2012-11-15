
package com.github.bigtoast.zookeeper

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, WordSpec}
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.Executors
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.util.duration._
import org.apache.zookeeper.CreateMode
import akka.util.Duration
import compat.Platform
import com.github.bigtoast.zookeeper.AsyncResponse.FailedAsyncResponse
import org.apache.zookeeper.KeeperException.{NoNodeException, NotEmptyException, BadVersionException}
import AsyncZooKeeperClient._

class AsyncZooKeeperClientSpecs extends WordSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  val eService = Executors.newCachedThreadPool
  implicit val to = 3 second
  var zk :AsyncZooKeeperClient = _

  class DoAwait[T]( f :Future[T] ) {
    def await( implicit d: Duration ) :T = Await.result[T]( f, d )
  }

  implicit def toDoAwait[T]( f :Future[T] ) = new DoAwait[T]( f )

  before {
    zk = new AsyncZooKeeperClient("localhost:2181",1000,1000,"/async-client/tests", None, ExecutionContext.fromExecutorService( eService ) )
  }

  after {
    zk.deleteChildren("") map {
      case _ => zk.close
    } recover {
      case _ => zk.close
    } await
  }

  override def afterAll {
    eService.shutdown
  }

  "A relative path should have base path prepended" in {
    zk.mkPath("testers") should be ("/async-client/tests/testers")
  }

  "An emtpy path should be base path" in {
    zk.mkPath("") should be ("/async-client/tests")
  }

  "An absolute path should not have a base path" in {
    zk.mkPath("/abs/path") should be ("/abs/path")
  }

  "An absolute '/' path should be '/'" in {
    zk.mkPath("/") should be ("/")
  }

  "connecting should work with running server" in {
    Await.result( zk.isAlive, to ) should be (true)
  }

  "creating a node" should {
    "work with string data" in {
      val s = zk.create("crabber", Some("crabber".getBytes), CreateMode.EPHEMERAL, Some("ctx") ).await
      s.path should be ("/async-client/tests/crabber")
      s.name should be ("/async-client/tests/crabber")
      s.ctx should be (Some("ctx"))
    }
    "work with null data" in {
      val s = zk.create("crabber", None, CreateMode.EPHEMERAL, Some("ctx") ).await
      s.path should be ("/async-client/tests/crabber")
      s.name should be ("/async-client/tests/crabber")
      s.ctx should be (Some("ctx"))
    }
  }

  "creating and getting a node" should {
    "work with string data" in {
      val s = zk.createAndGet("crabber", Some("crabber".getBytes), CreateMode.EPHEMERAL, Some("ctx") ).await
      s.path should be ("/async-client/tests/crabber")
      s.data.get.deser[String] should be ("crabber")
      s.stat.getNumChildren should be (0)
      s.ctx should be (Some("ctx"))
    }

    "work with null data" in {
      val s = zk.createAndGet("crabber", None, CreateMode.EPHEMERAL, Some("ctx") ).await
      s.path should be ("/async-client/tests/crabber")
      s.data should be ('empty)
      s.stat.getMtime should be < ( Platform.currentTime )
      s.ctx should be (Some("ctx"))
    }
  }

  "createPath should recursively create nodes" in {
    val stat = for {
      void <- zk.createPath("a/b/c/d/e/f/g")
      e1   <- zk.exists("a/b/c/d/e/f/g")
    } yield { e1 }

    stat.await.path should be ("/async-client/tests/a/b/c/d/e/f/g")
  }

  "Delete children should work" in {
    val stat = for {
      void    <- zk.createPath("a/b/c/d/e/f/g")
      deleted <- zk.deleteChildren("")
      stat    <- zk.exists("")
    } yield { stat }

    stat.await.stat.getNumChildren should be (0)
  }

  "delete" should {
    "fail if versions don't match" in {
      val void = for {
        str  <- zk.create("blubbs", Some("blubbs".getBytes), CreateMode.EPHEMERAL )
        fail <- zk.delete("blubbs", 666 )
      } yield fail

      intercept[FailedAsyncResponse]{
        void.await
      }.exception.isInstanceOf[BadVersionException] should be (true)
    }

    "fail if force is false and the node has children" in {
      val void = for {
        parent <- zk.createAndGet("blubbs", Some("blubbs".getBytes), CreateMode.PERSISTENT )
        kid    <- zk.create("blubbs/clubbs", Some("clubbs".getBytes), CreateMode.EPHEMERAL )
        fail   <- zk.delete("blubbs", parent.stat.getVersion )
      } yield fail

      intercept[FailedAsyncResponse]{
        void.await
      }.exception.isInstanceOf[NotEmptyException] should be (true)
    }

    "succeed if force is true and the node has children" in {
      val void = for {
        parent <- zk.createAndGet("blubbs", "blubbs".getBytes, CreateMode.PERSISTENT )
        kid    <- zk.create("blubbs/clubbs", "clubbs".getBytes, CreateMode.EPHEMERAL )
        fail   <- zk.delete("blubbs", parent.stat.getVersion, force = true )
      } yield fail

      void.await   // no exception

    }

    "succeed if force is false and the node has no children" in {
      val void = for {
        parent <- zk.createAndGet("blubbs", "blubbs".getBytes, CreateMode.PERSISTENT )
        fail   <- zk.delete("blubbs", parent.stat.getVersion )
      } yield fail

      void.await
    }
  }

  "set and get" should {
    "succeed if the node exists" in {
      val resp = for {
        init <- zk.create("crabs", null, CreateMode.PERSISTENT )
        set  <- zk.set("crabs", "crabber".getBytes, 0 )
        node <- zk.get("crabs")
      } yield node

      resp.await.data.get.deser[String] should be ("crabber")
      resp.await.stat.getVersion should be (1)
    }

    "fail on set if the node doesn't exist" in {
      val resp = for {
        fail  <- zk.set("crabs", "crabber".getBytes )
      } yield fail

      intercept[FailedAsyncResponse]{
        resp.await
      }.exception.isInstanceOf[NoNodeException] should be (true)
    }

    "succeed with null data" in {
      val resp = for {
        init <- zk.create("crabs", "crabber".getBytes, CreateMode.PERSISTENT )
        set  <- zk.set("crabs", null, 0 )
        node <- zk.get("crabs")
      } yield node

      resp.await.data should be ('empty)
      resp.await.stat.getVersion should be (1)
    }

  }

}
