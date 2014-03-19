
package com.github.bigtoast.zookeeper

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, WordSpec}
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.util.duration._
import org.apache.zookeeper.{WatchedEvent, Watcher, CreateMode}
import akka.util.Duration
import compat.Platform
import com.github.bigtoast.zookeeper.AsyncResponse.FailedAsyncResponse
import org.apache.zookeeper.KeeperException.{NoNodeException, NotEmptyException, BadVersionException}
import AsyncZooKeeperClient._
import org.apache.zookeeper.Watcher.Event.EventType
import java.util.concurrent.atomic.AtomicInteger

class AsyncZooKeeperClientSpecs extends WordSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  val eService = Executors.newCachedThreadPool
  implicit val to = 3 second
  var zk :AsyncZooKeeperClientImpl = _

  class DoAwait[T]( f :Future[T] ) {
    def await( implicit d: Duration ) :T = Await.result[T]( f, d )
  }

  implicit def toDoAwait[T]( f :Future[T] ) = new DoAwait[T]( f )

  before {
    zk = new AsyncZooKeeperClientImpl("localhost:2181",1000,1000,"/async-client/tests", None, ExecutionContext.fromExecutorService( eService ) )
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

  "watches when getting data" should {
    "be triggered when data changes" in {
      val waitForMe = new CountDownLatch(1)
      val watch = new Watcher {
        def process(event: WatchedEvent) = if ( event.getType == EventType.NodeDataChanged ) waitForMe.countDown
      }
      for {
        init <- zk.createAndGet("chubbs", "chubbs".getBytes, CreateMode.EPHEMERAL, watch = Some(watch) )
        seq  <- zk.set("chubbs", "blubber".getBytes )
      } yield seq
      waitForMe.await(1, TimeUnit.SECONDS)
    }

    "be triggered after multiple reads" in {
      val waitForMe = new CountDownLatch(1)
      val watch = new Watcher {
        def process(event: WatchedEvent) = if ( event.getType == EventType.NodeDataChanged ) waitForMe.countDown
      }
      for {
        init  <- zk.createAndGet("chubbs", "chubbs".getBytes, CreateMode.EPHEMERAL, watch = Some(watch) )
        data1 <- zk.get("chubbs")
        data2 <- zk.get("chubbs")
        seq   <- zk.set("chubbs", "blubber".getBytes )
      } yield seq

      waitForMe.await(1, TimeUnit.SECONDS)
    }

    "be triggered for multiple watches after multiple reads" in {
      val waitForMe = new CountDownLatch(1)
      val waitForMeToo = new CountDownLatch(1)
      val watch1 = new Watcher {
        def process(event: WatchedEvent) = if ( event.getType == EventType.NodeDataChanged ) waitForMe.countDown
      }
      val watch2 = new Watcher {
        def process(event: WatchedEvent) = if ( event.getType == EventType.NodeDataChanged ) waitForMeToo.countDown
      }
      for {
        init  <- zk.createAndGet("chubbs", "chubbs".getBytes, CreateMode.EPHEMERAL )
        data1 <- zk.get("chubbs", watch = Some(watch1))
        data2 <- zk.get("chubbs", watch = Some(watch2))
        seq   <- zk.set("chubbs", "blubber".getBytes )
      } yield seq

      waitForMe.await(1, TimeUnit.SECONDS)
      waitForMeToo.await(1, TimeUnit.SECONDS)
    }
  }

  "setting watches on exist" in {
    val waitForMe    = new CountDownLatch(1)
    val waitForMeToo = new CountDownLatch(1)
    val watch1 = new Watcher {
      def process(event: WatchedEvent) = if ( event.getType == EventType.NodeDataChanged ) waitForMe.countDown
    }
    val watch2 = new Watcher {
      def process(event: WatchedEvent) = if ( event.getType == EventType.NodeDataChanged ) waitForMeToo.countDown
    }
    for {
      init  <- zk.createAndGet("chubbs", "chubbs".getBytes, CreateMode.EPHEMERAL )
      data1 <- zk.exists("chubbs", watch = Some(watch1))
      data2 <- zk.get("chubbs", watch = Some(watch2))
      seq   <- zk.set("chubbs", "blubber".getBytes )
    } yield seq

    waitForMe.await(1, TimeUnit.SECONDS)
    waitForMeToo.await(1, TimeUnit.SECONDS)
  }

  "setting watches on getChildren" in {
    val waitForMe = new CountDownLatch(1)
    val watch1 = new Watcher {
      def process(event: WatchedEvent) = if ( event.getType == EventType.NodeChildrenChanged ) waitForMe.countDown
    }

    for {
      init  <- zk.createAndGet("chubbs", "chubbs".getBytes, CreateMode.PERSISTENT )
      kids  <- zk.getChildren("chubbs", watch = Some(watch1) )
      void  <- zk.createPath("chubbs/blubber" )
    } yield void

    waitForMe.await(1, TimeUnit.SECONDS)

  }

  "watchData" should {
    "perminently watch any changes on node" in {
      val waitForMe = new CountDownLatch(3)
      val counter = new AtomicInteger(0)
      for {
        init  <- zk.create("splats", counter.get.toString.getBytes, CreateMode.PERSISTENT )
        watch <- zk.watchData("splats"){ (path, data) => waitForMe.countDown }
        one   <- zk.set("splats", counter.incrementAndGet.toString.getBytes )
        two   <- zk.set("splats", counter.incrementAndGet.toString.getBytes )
        three <- zk.set("splats", counter.incrementAndGet.toString.getBytes )
      } yield three

      waitForMe.await(1, TimeUnit.SECONDS) should be (true)
      counter.get should be ( 3 )
    }
  }


  "watchChildren" should {
    "perminently watch child changes" in {
      val waitForMe = new CountDownLatch(3)
      for {
        init  <- zk.create("splats", None, CreateMode.PERSISTENT )
        watch <- zk.watchChildren("splats"){ kids => waitForMe.countDown }
        one   <- zk.createPath("splats/one")
        two   <- zk.createPath("splats/two")
        three <- zk.createPath("splats/three")
      } yield three

      waitForMe.await(1, TimeUnit.SECONDS) should be (true)
      waitForMe.getCount should be ( 0 )
    }
  }

}
