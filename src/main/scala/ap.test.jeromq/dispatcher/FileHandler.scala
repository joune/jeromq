package ap.test.jeromq.dispatcher

import org.zeromq._
import collection.JavaConversions._

//Observables API
//import rx.lang.scala._ 
//import rx.lang.scala.subjects.ReplaySubject

//import play.api.libs.iteratee._ 

///////// TEST /////////////

object FileHandlerMain extends App
{
  var acc = 0L
  var starttime = 0L

  FileHandler("tcp://*:5565") {
    case Open(name, size) => 
      println(s"start download of $name of size $size")
      starttime = System.currentTimeMillis
      Continue

    case Data(headers, bytes) => 
      //println(s"$headers got ${bytes.length} bytes")
      acc = acc + bytes.length
      Continue

    case Eof(headers, bytes) => 
      println(s"$headers EOF with total ${acc + bytes.length} bytes in ${System.currentTimeMillis - starttime}ms")
      acc = 0
      Continue

    case Error(headers) => 
      println(s"$headers server error after total $acc bytes in ${System.currentTimeMillis - starttime}ms")
      acc = 0
      Continue
  }
}

///////// API /////////////

object FileHandler
{
  // "iteratees like" api (sort of)
  def apply(endpoint: String)(callback: Input[Array[Byte]] => Action) = new FileHandlerImpl(endpoint)(callback)
}

sealed trait Input[T]
case class Open[T](name: String, size: Long) extends Input[T]
case class Data[T](name: String, data: T) extends Input[T]
case class Eof[T](name: String, data: T) extends Input[T]
case class Error[T](name: String) extends Input[T]

sealed trait Action
case object Continue extends Action
case object Done extends Action
case object Error extends Action


///////// IMPLEM /////////////

class FileHandlerImpl(endpoint: String)(callback: Input[Array[Byte]] => Action) 
{
  import ProtocolV1._

  val context = ZMQ.context(1)

  // client is notified of new files then fetches file from server
  val client = context.socket(ZMQ.DEALER)
  client.connect(endpoint)
  val items = Array(new ZMQ.PollItem(client, ZMQ.Poller.POLLIN))
  
  ready 

  while (true) 
  {
    ZMQ.poll(items, 500)
    if (items(0).isReadable)
    {
      val zmsg = ZMsg.recvMsg(client)
      val protocol_version = zmsg.pop
      val cmd = zmsg.pop
      // header is the command/action
      cmd.toString match {
        case NEWFILE => startDownload(zmsg)
        
        case CHUNK => gotChunk(zmsg)

        case EOF => endDownload(zmsg.pop.toString, //filename
                                  zmsg.pop.getData)//contents
        
        case ABORT => serverError(zmsg.pop.toString)

        case x => dbg(s"cmd $x not implemented!")
      }
    }
  }

  def dbg(s: => String) = println(s)

  def ready = send(client, "hello")

  var starttime: Long = 0

  def startDownload(zmsg: ZMsg) = {
    val size = zmsg.pop.toString.toLong
    val filename = zmsg.pop.toString

    callback(Open(filename, size)) match {
      case Continue => send(client, GET, filename, "0")

      case _ => send(client, ABORT)
    }
  }

  def gotChunk(zmsg: ZMsg) 
  {
    val filename = zmsg.pop.toString
    val position = zmsg.pop.toString
    val data = zmsg.pop.getData

    callback(Data(filename, data)) match {
      case Continue => send(client, GET, filename, position)

      case _ => send(client, ABORT)
    }
  }

  def endDownload(filename: String, bytes: Array[Byte]) = end(Eof(filename, bytes))
  def serverError(filename: String) = end(Error(filename))

  def end(in:Input[Array[Byte]]) = callback(in) match {
    case Continue => ready //again for next file
    case _ => // do not notify the server again
  }

  def send(to: ZMQ.Socket, headers: String*) {
    val msg = new ZMsg()
    msg.addString(PROTOCOL_VERSION)
    for (h <- headers) msg.addString(h)
    msg.send(to, true)
  }
}
