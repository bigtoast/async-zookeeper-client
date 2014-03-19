
import sbt._
import Keys._
import Process._

object AsyncZkClient extends Build {

  val VERSION = "0.3.0"

  val dependencies =  Seq(
    "com.typesafe.akka"    %  "akka-actor"         % "2.0.5",
    "org.apache.zookeeper" %  "zookeeper"          % "3.4.5" excludeAll (
                                                        ExclusionRule(organization = "log4j"),
                                                        ExclusionRule(organization = "org.slf4j")
                                                     ),
    "org.slf4j"            %  "log4j-over-slf4j"   % "1.7.5",
    "org.scalatest"        %% "scalatest"          % "1.9.1" % "test",
    "com.github.bigtoast"  %% "rokprox"            % "0.2.0" % "test",
    "org.slf4j"            %  "slf4j-simple"       % "1.7.5" % "test"
  )

  val publishDocs = TaskKey[Unit]("publish-docs")

  val project = Project("async-zk-client",file("."),
    settings = Defaults.defaultSettings ++ Seq(
      organization := "com.github.bigtoast",
      name         := "async-zk-client",
      version      := VERSION,
      scalaVersion := "2.9.2",

      publishTo := Some(Resolver.file("bigtoast.github.com", file(Path.userHome + "/Projects/BigToast/bigtoast.github.com/repo"))),

      publishDocs <<= ( doc in Compile , target in Compile in doc, version ) map { ( docs, dir, v ) =>
        val newDir = Path.userHome / "/Projects/BigToast/bigtoast.github.com/docs/async-zk-client" / v
        IO.delete( newDir )
        IO.createDirectory( newDir )
        IO.copyDirectory( dir, newDir )
      },

      resolvers += "Typesafe Releases" at "https://repo.typesafe.com/typesafe/releases/",

      libraryDependencies ++= dependencies

    )).settings(net.virtualvoid.sbt.graph.Plugin.graphSettings: _*)
}
