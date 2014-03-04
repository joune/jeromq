package ap.test.jeromq.pushpull

import org.zeromq.ZMQ

object Pull extends App
{
		// Prepare our context and subscriber
		val context = ZMQ.context(1)
		val subscriber = context.socket(ZMQ.PULL)

		subscriber.connect("tcp://localhost:5564")
		while (true) {
			// Read envelope with header
			val header = new String(subscriber.recv(0))
			// Read message contents
			val contents = new String(subscriber.recv(0))
			println(header + " : " + contents)
		}
}
