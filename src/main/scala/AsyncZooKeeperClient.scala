
package com.github.bigtoast.zookeeper

import org.slf4j.LoggerFactory
import org.apache.zookeeper._
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.ZooDefs.Ids
import scala.collection.mutable
import scala.collection.JavaConversions._
import org.apache.zookeeper.AsyncCallback._
import akka.dispatch.{Future, ExecutionContext, Promise }
import org.apache.zookeeper.KeeperException.Code
import java.util


sealed trait AsyncResponse {
  def ctx :Option[Any]
  def code :Code
}

object AsyncResponse {

  trait SuccessAsyncResponse extends AsyncResponse {
    val code = Code.OK
  }

  case class FailedAsyncResponse( exception :KeeperException, path :String, stat :Stat, ctx :Option[Any] ) extends RuntimeException(exception) with AsyncResponse {
    val code = exception.code
  }

  case class GetChildrenResponse( children :Seq[String], path :String, stat :Stat, ctx :Option[Any]  ) extends SuccessAsyncResponse

  case class StatResponse( path :String, stat :Stat, ctx :Option[Any]  ) extends SuccessAsyncResponse

  case class VoidResponse( path :String, ctx :Option[Any] ) extends SuccessAsyncResponse

  case class GetDataResponse( data :Array[Byte], path :String, stat :Stat, ctx :Option[Any]  ) extends SuccessAsyncResponse

  case class DeleteResponse( children :Seq[String], path :String, stat :Stat, ctx :Option[Any]  ) extends SuccessAsyncResponse

  case class StringResponse( name :String, path :String, ctx :Option[Any] ) extends SuccessAsyncResponse


}

/**
 *
 * Scala wrapper around the async ZK api. This is largely based on the twitter scala wrapper now maintained by 4square.
 * https://github.com/foursquare/scala-zookeeper-client
 *
 * It uses Akka 2.0 Futures. Once our company gets on scala 2.10 I will refactor to use SIP 14 Futures.
 *
 * I didn't implement any ACL stuff because I never use that shiz.
 *
 */
class AsyncZooKeeperClient(servers: String, sessionTimeout: Int, connectTimeout: Int,
                      basePath : String, watcher: Option[AsyncZooKeeperClient => Unit], eCtx :ExecutionContext ) {

  import AsyncResponse._

  implicit val c = eCtx

  private val log = LoggerFactory.getLogger(this.getClass)

  @volatile private var zk : ZooKeeper = null

  connect()

  def this(servers: String, sessionTimeout: Int, connectTimeout: Int, basePath : String, eCtx :ExecutionContext) =
    this(servers, sessionTimeout, connectTimeout, basePath, None, eCtx)

  def this(servers: String, sessionTimeout: Int, connectTimeout: Int,
           basePath : String, watcher: AsyncZooKeeperClient => Unit, eCtx :ExecutionContext) =
    this(servers, sessionTimeout, connectTimeout, basePath, Some(watcher), eCtx)

  def this(servers: String, eCtx :ExecutionContext) =
    this(servers, 3000, 3000, "", None, eCtx)

  def this(servers: String, watcher: AsyncZooKeeperClient => Unit, eCtx :ExecutionContext) =
    this(servers, 3000, 3000, "", watcher, eCtx)

  def getHandle() : ZooKeeper = zk

  /**
   * connect() attaches to the remote zookeeper and sets an instance variable.
   */
  private def connect() {
    val connectionLatch = new CountDownLatch(1)
    val assignLatch = new CountDownLatch(1)
    if (zk != null) {
      zk.close()
    }
    zk = new ZooKeeper(servers, sessionTimeout,
      new Watcher { def process(event : WatchedEvent) {
        sessionEvent(assignLatch, connectionLatch, event)
      }})
    assignLatch.countDown()
    log.info("Attempting to connect to zookeeper servers {}", servers)
    connectionLatch.await(sessionTimeout, TimeUnit.MILLISECONDS)
    try {
      isAlive
    } catch {
      case e => {
        throw new RuntimeException("Could not connect to zookeeper ensemble: " + servers
          + ". Connection timed out after " + connectTimeout + " milliseconds!", e)
      }
    }
  }

  def sessionEvent(assignLatch: CountDownLatch, connectionLatch : CountDownLatch, event : WatchedEvent) {
    log.info("Zookeeper event: {}", event)
    assignLatch.await()
    event.getState match {
      case KeeperState.SyncConnected => {
        try {
          watcher.map(fn => fn(this))
        } catch {
          case e:Exception =>
            log.error("Exception during zookeeper connection established callback", e)
        }
        connectionLatch.countDown()
      }
      case KeeperState.Expired => {
        // Session was expired; create a new zookeeper connection
        connect()
      }
      case _ => // Disconnected -- zookeeper library will handle reconnects
    }
  }

  /**
   * Given a string representing a path, return each subpath
   * Ex. subPaths("/a/b/c", "/") == ["/a", "/a/b", "/a/b/c"]
   */
  def subPaths(path : String, sep : Char) = {
    val l = path.split(sep).toList
    val paths = l.tail.foldLeft[List[String]](Nil){(xs, x) =>
      (xs.headOption.getOrElse("") + sep.toString + x)::xs}
    paths.reverse
  }

  private def makeNodePath(path : String) = "%s/%s".format(basePath, path).replaceAll("//", "/")

  def handleResponse[T]( rc :Int, path :String, p :Promise[T])( f : => T ) :Future[T] = {
    Code.get(rc) match {
      case Code.OK => p.success( f )
      case error if path == null => p.failure( KeeperException.create(error) )
      case error => p.failure( KeeperException.create(error, path ) )
    }
  }

  def exists(path: String, ctx :Option[Any] = None ): Future[StatResponse] = {
    val p = Promise[StatResponse]
    zk.exists(makeNodePath(path), false, new StatCallback {
      def processResult(rc: Int, path: String, ignore: Any, stat: Stat) {
        handleResponse(rc, path, p){ StatResponse(path, stat, ctx ) }
      }
    }, ctx )
    p
  }

  def getChildren(path: String, ctx :Option[Any] = None ): Future[GetChildrenResponse] = {
    val p = Promise[GetChildrenResponse]
    zk.getChildren(makeNodePath(path), false, new Children2Callback {
      def processResult(rc: Int, path: String, ignore: Any, children: util.List[String], stat: Stat) {
        handleResponse(rc, path, p ){ GetChildrenResponse( children.toSeq, path, stat, ctx ) }
      }
    }, ctx)
    p
  }

  def close() = zk.close

  def isAlive: Future[Boolean] = exists("/") map { _.stat.getVersion >= 0 }

  def create(path: String, data: Array[Byte], createMode: CreateMode, ctx :Option[Any] = None ): Future[StringResponse] = {
    val p = Promise[StringResponse]
    zk.create(makeNodePath(path), data, Ids.OPEN_ACL_UNSAFE, createMode, new StringCallback {
      def processResult(rc: Int, path: String, ignore: Any, name: String) {
        handleResponse(rc, path, p){ StringResponse( name, path, ctx ) }
      }
    }, ctx )
    p
  }

  /**
   * ZooKeeper version of mkdir -p
   */
  def createPath(path: String) :Future[VoidResponse] = {
    Future.sequence {
      for {
        subPath <- subPaths(makeNodePath(path), '/')
      } yield {
         create(subPath, null, CreateMode.PERSISTENT).recover {
           case e :FailedAsyncResponse if e.code == Code.NODEEXISTS => VoidResponse(subPath,None)
         }
      }
    } map { _ =>
      VoidResponse(path, None)
    }
  }

  def get(path: String, ctx :Option[Any] = None ):Future[GetDataResponse] = {
    val p = Promise[GetDataResponse]
    zk.getData(makeNodePath(path), false, new DataCallback {
      def processResult(rc: Int, path: String, ignore: Any, data: Array[Byte], stat: Stat) {
        handleResponse(rc, path, p){ GetDataResponse(data, path, stat, ctx ) }
      }
    }, ctx )
    p
  }

  def set(path: String, data: Array[Byte], version: Int = -1, ctx :Option[Any] = None): Future[StatResponse] = {
    val p = Promise[StatResponse]
    zk.setData(path, data, version, new StatCallback {
      def processResult(rc: Int, path: String, ignore: Any, stat: Stat) {
        handleResponse(rc, path, p){ StatResponse(path, stat, ctx ) }
      }
    }, ctx )
    p
  }

  def delete(path: String, version :Int = -1, ctx :Option[Any] = None ) :Future[VoidResponse] = {
    val p = Promise[VoidResponse]
    zk.delete(makeNodePath(path), version, new VoidCallback {
      def processResult(rc: Int, path: String, ignore: Any) {
        handleResponse(rc, path, p){ VoidResponse(path, ctx) }
      }
    }, ctx )
    p
  }

  /**
   * Delete a node along with all of its children
   */
  def deleteRecursive(path : String, ctx :Option[Any] = None ) :Future[VoidResponse] = {
      getChildren(path) flatMap { response =>
        Future.sequence {
          response.children.map { child =>
            deleteRecursive(path + "/" + child )
          }
        }
      } flatMap { _ => delete( path, -1, ctx ) }
  }

  /**
   * Watches a node. When the node's data is changed, onDataChanged will be called with the
   * new data value as a byte array. If the node is deleted, onDataChanged will be called with
   * None and will track the node's re-creation with an existence watch.
   */
  /*
  def watchNode(node : String, onDataChanged : (Option[Array[Byte]], Stat) => Unit) {
    log.debug("Watching node {}", node)
    val path = makeNodePath(node)
    def updateData {
      val stat = new Stat()
      try {
        onDataChanged(Some(zk.getData(path, dataGetter, stat)), stat)
      } catch {
        case e:KeeperException => {
          log.warn("Failed to read node {}: {}", path, e)
          deletedData
        }
      }
    }

    def deletedData {
      onDataChanged(None, new Stat())
      if (zk.exists(path, dataGetter) != null) {
        // Node was re-created by the time we called zk.exist
        updateData
      }
    }
    def dataGetter = new Watcher {
      def process(event : WatchedEvent) {
        if (event.getType == EventType.NodeDataChanged || event.getType == EventType.NodeCreated) {
          updateData
        } else if (event.getType == EventType.NodeDeleted) {
          deletedData
        }
      }
    }
    updateData
  }  */

  /**
   * Gets the children for a node (relative path from our basePath), watches
   * for each NodeChildrenChanged event and runs the supplied updateChildren function and
   * re-watches the node's children.
   */
  /*
  def watchChildren(node : String, updateChildren : Seq[String] => Unit) {
    val path = makeNodePath(node)
    val childWatcher = new Watcher {
      def process(event : WatchedEvent) {
        if (event.getType == EventType.NodeChildrenChanged ||
          event.getType == EventType.NodeCreated) {
          watchChildren(node, updateChildren)
        }
      }
    }
    try {
      val children = zk.getChildren(path, childWatcher)
      updateChildren(children)
    } catch {
      case e:KeeperException => {
        // Node was deleted -- fire a watch on node re-creation
        log.warn("Failed to read node {}: {}", path, e)
        updateChildren(List())
        zk.exists(path, childWatcher)
      }
    }
  } */

  /**
   * WARNING: watchMap must be thread-safe. Writing is synchronized on the watchMap. Readers MUST
   * also synchronize on the watchMap for safety.
   */
  /*
  def watchChildrenWithData[T](node : String, watchMap: mutable.Map[String, T], deserialize: Array[Byte] => T) {
    watchChildrenWithData(node, watchMap, deserialize, None)
  } */

  /**
   * Watch a set of nodes with an explicit notifier. The notifier will be called whenever
   * the watchMap is modified
   */
  /*
  def watchChildrenWithData[T](node : String, watchMap: mutable.Map[String, T],
                               deserialize: Array[Byte] => T, notifier: String => Unit) {
    watchChildrenWithData(node, watchMap, deserialize, Some(notifier))
  }

  private def watchChildrenWithData[T](node : String, watchMap: mutable.Map[String, T],
                                       deserialize: Array[Byte] => T, notifier: Option[String => Unit]) {
    def nodeChanged(child : String)(childData : Option[Array[Byte]], stat : Stat) {
      childData match {
        case Some(data) => {
          watchMap.synchronized {
            watchMap(child) = deserialize(data)
          }
          notifier.map(f => f(child))
        }
        case None => // deletion handled via parent watch
      }
    }

    def parentWatcher(children : Seq[String]) {
      val childrenSet = Set(children : _*)
      val watchedKeys = Set(watchMap.keySet.toSeq : _*)
      val removedChildren = watchedKeys -- childrenSet
      val addedChildren = childrenSet -- watchedKeys
      watchMap.synchronized {
        // remove deleted children from the watch map
        for (child <- removedChildren) {
          log.debug("Node {}: child {} removed", node, child)
          watchMap -= child
        }
        // add new children to the watch map
        for (child <- addedChildren) {
          // node is added via nodeChanged callback
          log.debug("Node {}: child {} added", node, child)
          watchNode("%s/%s".format(node, child), nodeChanged(child))
        }
      }
      for (child <- removedChildren) {
        notifier.map(f => f(child))
      }
    }

    watchChildren(node, parentWatcher)
  }
  */
}