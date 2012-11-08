

name := "async-zk-client"

version := "0.1"

scalaVersion := "2.9.2"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.3"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.3"

ivyXML :=  <dependencies>
           	<exclude org="com.sun.jmx" module="jmxri" />
              	<exclude org="com.sun.jdmk" module="jmxtools" />
              	<exclude org="javax.jms" module="jms" />
                <exclude org="thrift" module="libthrift" />
           </dependencies>

