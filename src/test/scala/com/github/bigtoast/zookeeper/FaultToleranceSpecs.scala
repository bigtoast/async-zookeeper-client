
package com.github.bigtoast.zookeeper

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, WordSpec}
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.{TimeUnit, CountDownLatch, Executors}
import akka.dispatch.{Await, ExecutionContext, Future}
import akka.util.duration._
import org.apache.zookeeper.{WatchedEvent, Watcher, CreateMode}
import Watcher.Event.KeeperState._
import akka.util.Duration
import compat.Platform
import com.github.bigtoast.zookeeper.AsyncResponse.FailedAsyncResponse
import org.apache.zookeeper.KeeperException.{NoNodeException, NotEmptyException, BadVersionException}
import AsyncZooKeeperClient._
import org.apache.zookeeper.Watcher.Event.EventType
import java.util.concurrent.atomic.AtomicInteger

import com.github.bigtoast.rokprox._
import akka.actor.ActorSystem

class FaultToleranceSpecs extends WordSpec with ShouldMatchers with BeforeAndAfter with BeforeAndAfterAll {

  val eService = Executors.newCachedThreadPool
  implicit val to = 3 second
  var zk :AsyncZooKeeperClient = _
  var prox :RokProxy = _

  var sys :ActorSystem = _

  class DoAwait[T]( f :Future[T] ) {
    def await( implicit d: Duration ) :T = Await.result[T]( f, d )
  }

  implicit def toDoAwait[T]( f :Future[T] ) = new DoAwait[T]( f )

  override def beforeAll { sys = ActorSystem("blabs") }

  before {
  	prox = RokProx.proxy("zk").from("127.0.0.1:2345").to("127.0.0.1:2181").build(sys)

  	Thread.sleep(2000)

    zk = AsyncZooKeeperClient("127.0.0.1:2345",1000,1000,"/async-client/tests", None, ExecutionContext.fromExecutorService( eService ) )

  }

  after {
    zk.deleteChildren("") map {
      case _ => zk.close
    } recover {
      case _ => zk.close
    } await

    prox.shutdown
    Thread.sleep(1000)
  }

  override def afterAll {
    eService.shutdown
    sys.shutdown
  }

  "Breaking a connection" should {
  	"trigger a reconnect" in {

  		val latch = new CountDownLatch(1)

  		zk.watchConnection {
  			case Disconnected =>
  				println("\n\n Got Disconnected \n\n")
  				latch.countDown
  			case e =>
  			    println("\n\n Got %s \n\n" format e)
  		}

  		val init = zk.createAndGet("testers", Some("besters" getBytes), CreateMode.EPHEMERAL).await

  		val cxns = prox.cxns.await

  		cxns should have size 1

  		val cxn = cxns.head

  		println("Breaking the connection")

  		prox.breakCxn(cxn.id)

  		latch.await(2L, TimeUnit.SECONDS) should be (true)

  		val second = zk.get("testers").await.data.get.deser[String] should be ("besters")
  	}

  	"trigger a session timeout" in {
  		val expiredLatch = new CountDownLatch(1)

  		zk.watchConnection {
  			case Expired =>
  				println("\n\n got expired \n\n")
  				expiredLatch.countDown
  			case e =>
  				println("\n\n Got %s \n\n" format e)
  		}

  		val init = zk.createAndGet("testers", Some("besters" getBytes), CreateMode.EPHEMERAL).await

  		println("Interrupting the connection for 10 seconds")

	  	prox.pause()

  		expiredLatch.await(10, TimeUnit.SECONDS) should be (true)

  		prox.restore

  		intercept[AsyncResponse.FailedAsyncResponse] {
  			zk.get("testers").await
  		}
  	}
  }

}
