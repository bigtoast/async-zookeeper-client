Scala Async ZooKeeper Client
----------------------------

Callbacks are a pain in the butt and they aren't composable. This wraps the ZK async api and converts the annoying callbacks
into swimmingly sweet, eminently composable Futures. This also adds persistent watch goodness and some recursive operations.
Originally based on the twitter scala wrapper now maintained by 4square (https://github.com/foursquare/scala-zookeeper-client),
it has been pretty much rewritten at this point.

<b>Coolness Provided</b>
 * Futures instead of callbacks
 * Relative and Absolute paths
 * Persistent connection ( reconnect on expired session )
 * Persistent watches
 * Recursive create and delete

It uses Akka 2.0 Futures. Once our company gets on scala 2.10 I will refactor to use SIP 14 Futures.

I didn't implement any ACL stuff because I never use that shiz.

Api Docs [http://bigtoast.github.com/docs/async-zk-client/0.2.1/](http://bigtoast.github.com/docs/async-zk-client/0.2.1/)

Currently depends on 
 * ZK 3.4.3
 * akka-actor 2.0.4
 * Scala 2.9.2

Getting Started
---------------

build.sbt
```scala

resolvers += "Bigtoast Repo" at "http://bigtoast.github.com/repo"

libraryDependencies += "com.github.bigtoast" %% "async-zk-api" % "0.2.2"

```

Creating a persistent connection
--------------------------------
This automatically reconnect if the session expires

```scala

import com.github.bigtoast.zookeeper._
import akka.dispatch.ExecutionContext
import java.util.concurrent.Executors
import org.apache.zookeeper.Watcher.Event.KeeperState

/* This constructor will block until a connection is made successfully and the
 * client receives a SyncConnected event
 */
val zk = new AsyncZooKeeperClient(

    servers = "127.0.0.1:2181,127.0.0.1:2182",

    sessionTimeout = 4000,

    connectTimeout = 4000,

    /** All paths not starting with '/' will have the base
      * path prepended. Absolute paths, those starting with '/',
      * will ignore this base path.
      */
    basePath = "/death/to/false/metal",

    /** Or use the default ctx from your ActorSystem if you are using Akka already. */
    eCtx = ExecutionContext.fromExecutorService( Executors.newCachedThreadPool ) )


```

Basic Use
---------
Getting data, getting children, setting, creating and deleting works just like the normal async api
except that paths can be absolute or relative and instead of passing in annoying callbacks
we get rad composable futures.


```scala

/* Create a node ("/death/to/false/metal/newPath") with no data returning a Future[StringResponse] */
zk.create("newPath", None, CreateMode.EPHEMERAL_SEQUENTIAL )

/* Set some data returning a Future[StatResponse] */
zk.set("/death/to/false/metal/newPath",Some("chubbs".getBytes))

/* Get data returning a Future[DataResponse] */
zk.get("newPath")

/* Delete data returning a Future[VoidResponse] */
zk.delete("newPath")

/* compose all these */
for {
  strResp  <- zk.create("newPath", None, EPHEMERAL_SEQUENTIAL )
  statResp <- zk.set(strResp.path, Some("blubbs".getBytes) )
  dataResp <- zk.get(statResp.path)
  voidResp <- zk.delete(dataResp.path, dataResp.stat.getVersion )
} yield voidResp.path + " successfully deleted!!"


```

Additional Helpers
------------------
There are helper methods to recursively create nodes, recursively delete nodes, create nodes returning data
and create a node if it doesn't exist or return it if it already does

```scala

/* create a path returning a Future[VoidResponse] */
zk.createPath("/a/really/long/path")

/* create a node and return a Future[DataResponse]. This is useful if you want the Stat object */
zk.createAndGet("path", None, CreateMode.PERSISTENT )

/* create a node if it doesn't already exist, if it does return what is there. Returns Future[DataResponse] */
zk.getOrCreate("path", Some("blabbs".getBytes), CreateMode.PERSISTENT)

/* if we have paths: "path/a", "path/b", "path/b/c", this will delete "a", "b", "b/c" but will leave "path". Returns Future[VoidResponse] */
zk.deleteChildren("path")

/* same as above but "path" will be deleted as well */
zk.delete("path", force = true )


```

Persistent Watches
------------------
There are also helpers for setting persistent watches.

```scala

/* sets a persistent watch listening for connection events ( connected, disconnected etc.. ). KeeperState is passed
 * into the closure
 */
zk.watchConnection {
    case KeeperState.Expired =>
        // the client will automatically reconnect if the ZK session expires but
        // you can use this to register a callback if you need to.

    case _ =>

}

/** sets a persistent watch on '/death/to/false/metal/parent'. When triggered, getChildren
  * is called, resetting the watch. The ChildrenResponse is passed into the closure.
  *
  * returns a Future[ChildResponse] with the initial children of the parent node
  */
val initialKids = zk.watchChildren("parent"){ kids => /* do stuff with child response */ }

/** sets a persistent watch, returning the initial data at for that node. When the
  * watch is triggered a ( String, Option[DataResponse] ) is passed into the closure
  * and the watch is reset.
  */
val initData = zk.watchData("/some/path/with/data"){
                   case ( path, Some( data ) ) => // do something when the data changed
                   case ( path, None ) => // do something when the node is deleted
               }

// By default watchChildren and watchData set persistent watches, but you can set one time watches thusly
zk.watchChildren("/some/parent",false){ kids => /* triggered just once */ }

zk.watchData("/some/node/with/data", false){ case ( path, dataOp ) => /* triggered just once */ }

```

Dangerous Stuff
---------------

If you need the underlying zk client you can get it from the handle method. But don't hold on to it. If the ZK session expires, the
underlying client will be replaced.

```scala
zk.handle // returns Option[ZooKeeper]
```

The AsyncZooKeeperClient object contains some optional implicits to help out.

There is a Derserializer type class to assist in deserialization. Just provide an implicit function of Array[Byte] => T. 
One is already included for Array[Byte] => String.

```scala
import AsyncZooKeeperClient._
import java.io._

// simple java deserializer 
def fromBytes[T]( bytes :Array[Byte]) :T = {
    var ary :Array[Byte] = null
    var is :ObjectInputStream = null
    try {
      val ba = new ByteArrayInputStream( bytes )
      is = new ObjectInputStream( ba )
      is.readObject().asInstanceOf[T]
    } catch {
      case e :NullPointerException => throw new IOException("Byte array empty")
    } finally {
      if ( is != null )
        is.close
    }
  }
}

// my data 
case class Node( host :String, port :Int )

implicit def nodeDeser = fromBytes[Node] _

zk.get("cluster/member-1") map { ( path, dataOp ) => dataOp.map { _.deser[Node] } } // Returns a Future[Option[Node]]

```

Provided also is an implicit conversion from Array[Byte] to Option[Array[Byte]], althought Im still unsure if this is a good idea.
