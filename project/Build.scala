
import sbt._
import Keys._
import Process._

object AsyncZkClient extends Build {

  val VERSION = "0.2.3"

  val dependencies =
    "com.typesafe.akka"    %  "akka-actor" % "2.0.4" ::
    "org.apache.zookeeper" %  "zookeeper"  % "3.4.3" ::
    "org.scalatest"        %% "scalatest"  % "1.8" % "test" :: 
    "com.github.bigtoast"  %% "rokprox"    % "0.2.0" % "test" :: Nil

  val publishDocs = TaskKey[Unit]("publish-docs")

  val project = Project("async-zk-client",file("."),
    settings = Defaults.defaultSettings ++ Seq(
      organization := "com.github.bigtoast",
      name         := "async-zk-client",
      version      := VERSION,
      scalaVersion := "2.10.3",

      ivyXML :=
        <dependencies>
          <exclude org="com.sun.jmx" module="jmxri" />
          <exclude org="com.sun.jdmk" module="jmxtools" />
          <exclude org="javax.jms" module="jms" />
          <exclude org="thrift" module="libthrift" />
        </dependencies>,

      publishTo := Some(Resolver.file("bigtoast.github.com", file(Path.userHome + "/Projects/BigToast/bigtoast.github.com/repo"))),

      publishDocs <<= ( doc in Compile , target in Compile in doc, version ) map { ( docs, dir, v ) =>
        val newDir = Path.userHome / "/Projects/BigToast/bigtoast.github.com/docs/async-zk-client" / v
        IO.delete( newDir )
        IO.createDirectory( newDir )
        IO.copyDirectory( dir, newDir )
      },

      libraryDependencies ++= dependencies

    ))
}
