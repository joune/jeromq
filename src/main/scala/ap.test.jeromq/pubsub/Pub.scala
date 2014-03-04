package ap.test.jeromq.pubsub

import org.zeromq.ZMQ

object Pub extends App
{
		// Prepare our context and publisher
		val context = ZMQ.context(1)
		val publisher = context.socket(ZMQ.PUB)
    var count = 0

		publisher.bind("tcp://*:5563")
		while (true) {
      count = count + 1
			// Write two messages, each with an envelope and content
			publisher.send("A".getBytes(), ZMQ.SNDMORE)
			publisher.send(s"$count - We don't want to see this".getBytes(), 0)
			publisher.send("B".getBytes(), ZMQ.SNDMORE)
			publisher.send(s"$count - We would like to see this".getBytes(), 0)
			Thread.sleep(1000)
		}
}
