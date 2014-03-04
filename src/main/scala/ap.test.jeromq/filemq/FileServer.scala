package ap.test.jeromq.filemq

import org.filemq._

object FileServer extends App 
{
  val server = new FmqServer ()
  //server.configure ("src/test/resources/anonymous.cfg")
  server.publish (args (0), "/")
  server.setAnonymous (1)
  server.bind ("tcp://*:5670")
}
