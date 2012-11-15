

organization := "com.github.bigtoast"

name := "async-zk-client"

version := "0.2.0"

scalaVersion := "2.9.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.4"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.3"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.8" % "test"

ivyXML :=  <dependencies>
           	<exclude org="com.sun.jmx" module="jmxri" />
              	<exclude org="com.sun.jdmk" module="jmxtools" />
              	<exclude org="javax.jms" module="jms" />
                <exclude org="thrift" module="libthrift" />
           </dependencies>

publishTo := Some(Resolver.file("bigtoast.github.com", file(Path.userHome + "/Projects/BigToast/bigtoast.github.com/repo")))

