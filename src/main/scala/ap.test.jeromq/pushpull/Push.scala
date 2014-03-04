package ap.test.jeromq.pushpull

import org.zeromq.ZMQ

object Push extends App
{
		// Prepare our context and publisher
		val context = ZMQ.context(1)
		val publisher = context.socket(ZMQ.PUSH)
    var count = 0

		publisher.bind("tcp://*:5564")
		while (true) {
      count = count + 1
			publisher.send(s"header #$count".getBytes(), ZMQ.SNDMORE)
			publisher.send(s"msg #$count".getBytes(), 0)
			Thread.sleep(1000)
		}
}
