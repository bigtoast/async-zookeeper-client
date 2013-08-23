
package com.github.bigtoast.zookeeper

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, WordSpec}
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import org.apache.zookeeper.{KeeperException, WatchedEvent, Watcher, CreateMode}
import Watcher.Event.KeeperState._
import compat.Platform
import com.github.bigtoast.zookeeper.AsyncResponse.FailedAsyncResponse
import org.apache.zookeeper.KeeperException.{Code, NoNodeException, NotEmptyException, BadVersionException}
import AsyncZooKeeperClient._
import org.apache.zookeeper.Watcher.Event.EventType
import java.util.concurrent.atomic.AtomicInteger

import com.github.bigtoast.rokprox._
import akka.actor.ActorSystem
import scala.util.{Failure, Success}

class FaultToleranceSpecs extends WordSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  import scala.concurrent.ExecutionContext.Implicits.global


  implicit val to = 3 second
  var zkServer :EmbeddedZookeeper = _
  var zk :AsyncZooKeeperClient = _
  var prox :RokProxy = _

  var sys :ActorSystem = _

  class DoAwait[T]( f :Future[T] ) {
    def await( implicit d: Duration ) :T = Await.result[T]( f, d )
  }

  implicit def toDoAwait[T]( f :Future[T] ) = new DoAwait[T]( f )

  override def beforeAll { sys = ActorSystem("blabs") }

  before {
    zkServer = new EmbeddedZookeeper("127.0.0.1:22181")
  	prox = RokProx.proxy("zk").from("127.0.0.1:2345").to("127.0.0.1:22181").build(sys)
    Thread.sleep(1000)

  }

  after {
    zk.deleteChildren("") map {
      case _ => zk.close
    } recover {
      case _ => zk.close
    } await

    zk.close

    prox.shutdown
    zkServer.shutdown()
    Thread.sleep(1000)

  }

  override def afterAll {
    sys.shutdown
  }

  "Breaking a connection" should {
  	"trigger a reconnect" in {

      zk = new AsyncZooKeeperClient("127.0.0.1:2345",2000,10000,"/async-client/tests", None, ExecutionContext.global )

      val latch = new CountDownLatch(1)

  		zk.watchConnection {
  			case Disconnected =>
  				//println("\n\n Got Disconnected \n\n")
  				latch.countDown
  			case e =>
  			    //println("\n\n Got %s \n\n" format e)
  		}

  		val init = zk.createAndGet("testers", Some("besters" getBytes), CreateMode.EPHEMERAL).await

  		val cxns = prox.cxns.await

  		cxns should have size 1

  		val cxn = cxns.head

  		//println("Breaking the connection")

  		prox.breakCxn(cxn.id)

  		latch.await(3L, TimeUnit.SECONDS) should be (true)

  		val second = zk.get("testers").await.data.get.deser[String] should be ("besters")
  	}

    // This test will not work until https://github.com/bigtoast/rokprox/issues/2 will be resolved
//  	"trigger a session timeout" in {
//
//      zk = new AsyncZooKeeperClient("127.0.0.1:2345",2000,1000,"/async-client/tests", None, ExecutionContext.global )
//
//      val expiredLatch = new CountDownLatch(1)
//
//  		zk.watchConnection {
//  			case Expired =>
//  				println("\n\n got expired \n\n")
//  				expiredLatch.countDown
//  			case e =>
//  				println("\n\n Got %s \n\n" format e)
//  		}
//
//      val init = zk.createAndGet("testers", Some("besters" getBytes), CreateMode.EPHEMERAL).await
//
//      println("Interrupting the connection for 10 seconds")
//
//	  	prox.pause(5 seconds)
//
//      expiredLatch.await(20, TimeUnit.SECONDS) should be (true)
//
//      prox.restore
//
//      intercept[AsyncResponse.FailedAsyncResponse] {
//  			zk.get("testers").await
//  		}
//  	}
   }
}
