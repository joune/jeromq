package ap.test.jeromq.dispatcher

import scala.concurrent.{Future, Promise, Await}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest._

class DispatcherSpec extends FlatSpec with Matchers 
{
  val ENDPOINT = "tcp://*:5565"

  def checkSum: Future[Boolean] = 
  {
    var expected = 0L
    var acc = 0L
    var starttime = 0L

    val p = Promise[Boolean]

    Future {
      FileHandler(ENDPOINT) 
      {
        case Open(name, size) => 
          expected = size
          starttime = System.currentTimeMillis
          Continue

        case Data(headers, bytes) => 
          acc = acc + bytes.length
          Continue

        case Eof(headers, bytes) => 
          acc = acc + bytes.length
          println(s"$headers EOF with total $acc bytes in ${System.currentTimeMillis - starttime}ms")
          p success (acc == expected)
          Done

        case Error(headers) => 
          println(s"$headers server error after total $acc bytes in ${System.currentTimeMillis - starttime}ms")
          p success (false)
          Done
      }
    }
    p.future
  }
  
  "Dispatcher" should "transfer a file from a single worker to a single client" in {
    val server = Future (FileDispatcher(ENDPOINT, 2)(Future("src/test/resources/tumblr.rss")))
    Await.result(checkSum, 1 seconds) should be (true)
    //server.destroy
  }
  //single worker, multiple clients
  //multiple workers, multiple clients
  //large files
  //load balancing
  //slow client
  //server restart
  //client restart
}
