package ap.test.jeromq.filemq

import org.filemq._

object FileClient extends App 
{
  val client = new FmqClient ()
  client.connect ("tcp://localhost:5670")
  client.setInbox (args (0))
  client.subscribe ("/")
}
