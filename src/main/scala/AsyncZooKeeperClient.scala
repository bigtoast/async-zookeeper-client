
package com.github.bigtoast.zookeeper

import org.slf4j.LoggerFactory
import org.apache.zookeeper._
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.data.Stat
import org.apache.zookeeper.ZooDefs.Ids
import scala.collection.JavaConversions._
import org.apache.zookeeper.AsyncCallback._
import akka.dispatch.{Await, Future, ExecutionContext, Promise}
import org.apache.zookeeper.KeeperException.Code
import java.util
import akka.util.duration._


sealed trait AsyncResponse {
  def ctx :Option[Any]
  def code :Code
}

object AsyncResponse {

  trait SuccessAsyncResponse extends AsyncResponse {
    val code = Code.OK
  }

  case class FailedAsyncResponse( exception :KeeperException, path :Option[String], stat: Option[Stat], ctx :Option[Any] ) extends RuntimeException(exception) with AsyncResponse {
    val code = exception.code
  }

  case class ChildrenResponse( children :Seq[String], path :String, stat :Stat, ctx :Option[Any]  ) extends SuccessAsyncResponse

  case class StatResponse( path :String, stat :Stat, ctx :Option[Any]  ) extends SuccessAsyncResponse

  case class VoidResponse( path :String, ctx :Option[Any] ) extends SuccessAsyncResponse

  case class DataResponse( data :Option[Array[Byte]], path :String, stat :Stat, ctx :Option[Any]  ) extends SuccessAsyncResponse

  case class DeleteResponse( children :Seq[String], path :String, stat :Stat, ctx :Option[Any]  ) extends SuccessAsyncResponse

  case class StringResponse( name :String, path :String, ctx :Option[Any] ) extends SuccessAsyncResponse


}

/** This just provides some implicits to help working with byte arrays. They are totally optional */
object AsyncZooKeeperClient {
  implicit def bytesToSome( bytes :Array[Byte] ) :Option[Array[Byte]] = Some(bytes)

  implicit val stringDeser :Array[Byte] => String = bytes => bytes.view.map(_.toChar).mkString

  class Deserializer( bytes :Array[Byte] ) {
    def deser[T]( implicit d :Array[Byte] => T ) = d( bytes )
  }

  implicit def toDeser( bytes :Array[Byte] ) :Deserializer = new Deserializer( bytes )
}

/**
 *
 * Scala wrapper around the async ZK api. This is based on the twitter scala wrapper now maintained by 4square.
 * https://github.com/foursquare/scala-zookeeper-client
 *
 * It uses Akka 2.0 Futures. Once our company gets on scala 2.10 I will refactor to use SIP 14 Futures.
 *
 * I didn't implement any ACL stuff because I never use that shiz.
 *
 * You can pass in a base path (defaults to "/") which will be prepended to any path that does not start with a "/".
 * This allows you to specify a context to all requests. Absolute paths, those starting with "/", will not have the
 * base path prepended.
 *
 * On connection the base path will be created if it does not already exist.
 *
 *
 */
class AsyncZooKeeperClient(val servers: String, val sessionTimeout: Int, val connectTimeout: Int,
                      val basePath : String, watcher: Option[AsyncZooKeeperClient => Unit], eCtx :ExecutionContext ) {

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
    this(servers, 3000, 3000, "/", None, eCtx)

  def this(servers: String, watcher: AsyncZooKeeperClient => Unit, eCtx :ExecutionContext) =
    this(servers, 3000, 3000, "/", watcher, eCtx)

  /** get the underlying ZK connection */
  def getHandle = zk

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
      isAliveSync
      Await.result( createPath(""), 10 seconds )
    } catch {
      case e => {
        log.error("Could not connect to zookeeper ensemble: " + servers
          + ". Connection timed out after " + connectTimeout + " milliseconds!", e)

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

  private[zookeeper] def handleNull( op :Option[Array[Byte]] ) :Array[Byte] = if ( op == null ) null else op.getOrElse(null)

  /** Given a string representing a path, return each subpath
   *  Ex. subPaths("/a/b/c", "/") == ["/a", "/a/b", "/a/b/c"]
   */
  def subPaths(path : String, sep : Char) =
    path.split(sep).toList match {
      case Nil => Nil
      case l :: tail =>
        tail.foldLeft[List[String]](Nil){ (xs, x) =>
          (xs.headOption.getOrElse("") + sep.toString + x) :: xs } reverse
    }

  /** Create a zk path from a relative path. If an absolute path is passed in the base path will not be prepended.
    * Trailing '/'s will be stripped except for the path '/' and all '//' will be translated to '/'.
    *
    * @param path relative or absolute path
    * @return absolute zk path
    */
  private[zookeeper] def mkPath(path : String) = (path startsWith "/" match {
    case true => path
    case false => "%s/%s".format(basePath, path)
  }).replaceAll("//", "/") match {
    case str if str.length > 1 => str.stripSuffix("/")
    case str => str
  }

  /** helper method to convert a zk response in to a client reponse and handle the errors */
  def handleResponse[T]( rc :Int, path :String, p :Promise[T], stat:Stat, cxt :Option[Any] )( f : => T ) :Future[T] = {
    Code.get(rc) match {
      case Code.OK => p.success( f )
      case error if path == null => p.failure( FailedAsyncResponse( KeeperException.create(error),Option(path), Option(stat), cxt ) )
      case error => p.failure( FailedAsyncResponse( KeeperException.create(error, path ),Option(path), Option(stat), cxt ) )
    }
  }

  /** Wrapper around the ZK exists method. Watch is hardcoded to false.
    *
    * @see <a target="_blank" href="http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#exists(java.lang.String, boolean, org.apache.zookeeper.AsyncCallback.StatCallback, java.lang.Object)">
    *        http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#exists(java.lang.String, boolean, org.apache.zookeeper.AsyncCallback.StatCallback, java.lang.Object)</a>
    */
  def exists(path: String, ctx :Option[Any] = None ): Future[StatResponse] = {
    val p = Promise[StatResponse]
    zk.exists(mkPath(path), false, new StatCallback {
      def processResult(rc: Int, path: String, ignore: Any, stat: Stat) {
        handleResponse(rc, path, p, stat, ctx ){ StatResponse(path, stat, ctx ) }
      }
    }, ctx )
    p
  }

  /** Wrapper around the ZK getChildren method. Watch is hardcoded to false.
    *
    * @see <a target="_blank" href="http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#getChildren(java.lang.String, boolean, org.apache.zookeeper.AsyncCallback.Children2Callback, java.lang.Object)">
    *        http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#getChildren(java.lang.String, boolean, org.apache.zookeeper.AsyncCallback.Children2Callback, java.lang.Object)</a>
    */
  def getChildren(path: String, ctx :Option[Any] = None ): Future[ChildrenResponse] = {
    val p = Promise[ChildrenResponse]
    zk.getChildren(mkPath(path), false, new Children2Callback {
      def processResult(rc: Int, path: String, ignore: Any, children: util.List[String], stat: Stat) {
        handleResponse(rc, path, p, stat, ctx  ){ ChildrenResponse( children.toSeq, path, stat, ctx ) }
      }
    }, ctx)
    p
  }

  /** close the underlying zk connection */
  def close = zk.close

  /** Checks the connection by checking existence of "/" */
  def isAlive: Future[Boolean] = exists("/") map { _.stat.getVersion >= 0 }

  /** Check the connection synchronously */
  def isAliveSync :Boolean = try {
    zk.exists("/", false)
    true
  } catch {
    case e =>
      log.warn("ZK not connected in isAliveSync", e)
      false
  }

    /** Recursively create a path with persistent nodes and no watches set. */
    def createPath(path: String) :Future[VoidResponse] = {
      Future.sequence {
        for {
          subPath <- subPaths(mkPath(path), '/')
        } yield {
          create(subPath, null, CreateMode.PERSISTENT).recover {
            case e :FailedAsyncResponse if e.code == Code.NODEEXISTS => VoidResponse(subPath,None)
          }
        }
      } map { _ =>
        VoidResponse(path, None)
      }
    }

    /** Create a path with OPEN_ACL_UNSAFE hardcoded
      * @see <a target="_blank" href="http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#create(java.lang.String, byte[], java.util.List, org.apache.zookeeper.CreateMode, org.apache.zookeeper.AsyncCallback.StringCallback, java.lang.Object)">
      *         http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#create(java.lang.String, byte[], java.util.List, org.apache.zookeeper.CreateMode, org.apache.zookeeper.AsyncCallback.StringCallback, java.lang.Object)
      *      </a>
      */
    def create(path :String, data :Option[Array[Byte]], createMode :CreateMode, ctx :Option[Any] = None ) :Future[StringResponse] = {
      val p = Promise[StringResponse]
      zk.create(mkPath(path), handleNull( data ), Ids.OPEN_ACL_UNSAFE, createMode, new StringCallback {
        def processResult(rc: Int, path: String, ignore: Any, name: String) {
          handleResponse(rc, path, p, null, ctx ){ StringResponse( name, path, ctx ) }
        }
      }, ctx )
      p
    }

    /** Create a node and then return it. Under the hood this is a create followed by a get. If the stat or data is not
      * needed use a plain create which is cheaper.
      */
    def createAndGet(path :String, data :Option[Array[Byte]], createMode :CreateMode, ctx :Option[Any] = None ) :Future[DataResponse] = {
      create(path, data, createMode, ctx ) flatMap { _ => get(path, ctx) }
    }


    /** Return the node if it exists, otherwise create a new node with the data passed in. If the node is created a get will
      * be called and the value returned. In case of a race condition where a two or more requests are executed at the same
      * time and one of the creates will fail with a NodeExistsException, it will be handled and a get will be called.
      */
    def getOrCreate(path :String, data :Option[Array[Byte]], createMode :CreateMode, ctx :Option[Any] = None ) :Future[DataResponse] = {
      get(path) recoverWith {
        case FailedAsyncResponse( e : KeeperException.NoNodeException, _, _, _) =>
          create(path, data, createMode, ctx ) flatMap { _ => get( path ) } recoverWith {
            case FailedAsyncResponse( e :KeeperException.NodeExistsException, _, _, _ ) =>
              get(path)
          }
      }
    }

    /** Wrapper around zk getData method. Watch is hardcoded to false.
      * @see <a target="_blank" href="http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#getData(java.lang.String, boolean, org.apache.zookeeper.AsyncCallback.DataCallback, java.lang.Object)">
      *        http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#getData(java.lang.String, boolean, org.apache.zookeeper.AsyncCallback.DataCallback, java.lang.Object)
      *      </a>
      */
    def get(path: String, ctx :Option[Any] = None ) :Future[DataResponse] = {
      val p = Promise[DataResponse]
      zk.getData(mkPath(path), false, new DataCallback {
        def processResult(rc: Int, path: String, ignore: Any, data: Array[Byte], stat: Stat) {
          handleResponse(rc, path, p, stat, ctx ){ DataResponse(Option(data), path, stat, ctx ) }
        }
      }, ctx )
      p
    }

    /** Wrapper around the zk setData method.
      * @see <a target="_blank" href="http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#setData(java.lang.String, byte[], int, org.apache.zookeeper.AsyncCallback.StatCallback, java.lang.Object)">
      *        http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#setData(java.lang.String, byte[], int, org.apache.zookeeper.AsyncCallback.StatCallback, java.lang.Object)
      *      </a>
      */
    def set(path :String, data :Option[Array[Byte]], version :Int = -1, ctx :Option[Any] = None): Future[StatResponse] = {
      val p = Promise[StatResponse]
      zk.setData(mkPath(path), handleNull( data ), version, new StatCallback {
        def processResult(rc: Int, path: String, ignore: Any, stat: Stat) {
          handleResponse(rc, path, p, stat, ctx ){ StatResponse(path, stat, ctx ) }
        }
      }, ctx )
      p
    }

    /** Wrapper around zk delete method.
      * @see <a target="_blank" href="http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#delete(java.lang.String, int, org.apache.zookeeper.AsyncCallback.VoidCallback, java.lang.Object)">
      *        http://zookeeper.apache.org/doc/r3.4.1/api/org/apache/zookeeper/ZooKeeper.html#delete(java.lang.String, int, org.apache.zookeeper.AsyncCallback.VoidCallback, java.lang.Object)
      *      </a>
      *
      * The version only applies to the node indicated by the path. If force is true all children will be deleted even if the
      * version doesn't match.
      *
      * @param force Delete all children of this node
      */
    def delete(path: String, version :Int = -1, ctx :Option[Any] = None, force :Boolean = false ) :Future[VoidResponse] = {
      if ( force ) {
        deleteChildren(path, ctx) flatMap { _ => delete(path, version, ctx, false ) }
      } else {
        val p = Promise[VoidResponse]
        zk.delete(mkPath(path), version, new VoidCallback {
          def processResult(rc: Int, path: String, ignore: Any) {
            handleResponse(rc, path, p, null, ctx ){ VoidResponse(path, ctx) }
          }
        }, ctx )
        p
      }
    }

    /** Delete all the children of a node but not the node. */
    def deleteChildren(path : String, ctx :Option[Any] = None ) :Future[VoidResponse] = {
      def recurse( p :String ) :Future[VoidResponse] = {
        getChildren(p) flatMap { response =>
          Future.sequence {
            response.children.map { child =>
              recurse( mkPath( p ) + "/" + child )
            }
          }
        } flatMap { seq =>
          if ( p == path )
            Promise.successful( VoidResponse( path, ctx ) )
          else
            delete( p, -1, ctx ) }
      }
      recurse(path)
    }

  /**
   * Watches a node. When the node's data is changed, onDataChanged will be called with the
   * new data value as a byte array. If the node is deleted, onDataChanged will be called with
   * None and will track the node's re-creation with an existence watch.
   */
  /*
  def watchNode(node : String, onDataChanged : (Option[Array[Byte]], Stat) => Unit) {
    log.debug("Watching node {}", node)
    val path = mkPath(node)
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
    val path = mkPath(node)
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