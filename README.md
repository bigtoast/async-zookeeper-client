async-zookeeper-client
======================

Scala wrapper around the async ZK api. This is based on the twitter scala wrapper now maintained by 4square.

https://github.com/foursquare/scala-zookeeper-client

It uses Akka 2.0 Futures. Once our company gets on scala 2.10 I will refactor to use SIP 14 Futures.

I didn't implement any ACL stuff because I never use that shiz.

