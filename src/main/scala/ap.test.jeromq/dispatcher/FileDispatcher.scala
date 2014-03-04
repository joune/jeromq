package ap.test.jeromq.dispatcher

import org.zeromq._
import collection.JavaConversions._
import collection.mutable.{Queue, Map => mMap}
import scala.util.{Try, Success, Failure}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel

/**
 * file dispatcher 
 *
 * protocol: 
 * - dispatcher listens for incoming connections
 * - client sends hello when ready: reply with the next available file path
 * - client sends get(filepath, 0): reply with first chunk and keep file handle open
 * - client sends get(filepath, position): reply with chunk@position
 * - client may send an abort message to stop current download
 */


object FileDispatcherMain extends App
{
  FileDispatcher("tcp://*:5565", 10)(Future(args(0))) //send the same file continuously
}

trait FileDispatcher 
{
  def destroy
}

object FileDispatcher 
{
  def apply(endpoint: String, nbWorkers: Int = 1)(nextFile: => Future[String]) = 
    new FileDispatcherImpl(endpoint, nbWorkers)(nextFile)
}

////////////////// hidden implementation details! ///////////////////////////

// TODO : 
// - client heartbeat

object ProtocolV1
{
  val PROTOCOL_VERSION = "1"
  val HELLO = "hello"     //client ready
  val NEWFILE = "newfile" //notify new file to client
  val GET = "get"         //get request from client for one chunk
  val CHUNK = "chunk"     //server reply with one chunk
  val EOF = "eof"         //last chunk and end of file signal
  val ABORT = "abort"     //abort current transfer
}

class FileDispatcherImpl(endpoint: String, nbWorkers: Int)(nextFile: => Future[String]) extends FileDispatcher
{
  private val context =  ZMQ.context(1)

  // server is the frontend that pushes filenames to clients and receives requests
  private val server = context.socket(ZMQ.ROUTER)
  server.bind(endpoint)
  // backend handles clients requests
  private val backend = context.socket(ZMQ.DEALER)
  backend.bind("inproc://backend")
  // internal control socket for server side API
  private val control = context.socket(ZMQ.PAIR)
  control.bind("inproc://control")

  var stopped = false
  def destroy = {
    stopped = true
    //wait for workers to stop... ?
    context.term
  }

  // mutable map of connected clients:
  // contains the currrent state of known clients, shared by worker threads
  // if each worker is handling a different client, there should be no contention
  val clients = mMap[String, ClientState]() 

  // function to fetch the next file to serve and send it on the worker's control socket
  def next = nextFile map { f => 
    control.sendMore(ProtocolV1.NEWFILE)
    control.send(f)
  }

  // multithreaded server: router hands out requests to DEALER workers via a inproc queue
  private val workers = List.fill(nbWorkers)(new ServerWorker(context, this))
  workers foreach (_.start)
  ZMQ.proxy(server, backend, null)
}

sealed trait ClientState
case class Waiting(address: ZFrame) extends ClientState
case class Serving(fc: FileChannel) extends ClientState


class ServerWorker(ctx: ZMQ.Context, server: FileDispatcherImpl) extends Thread 
{
  import ProtocolV1._

  val worker = ctx.socket(ZMQ.DEALER)
  worker.connect("inproc://backend")
  val control = ctx.socket(ZMQ.PAIR)
  control.connect("inproc://control")

  val items = Array(
    new ZMQ.PollItem(control, ZMQ.Poller.POLLIN),
    new ZMQ.PollItem(worker, ZMQ.Poller.POLLIN)
  )

  val bufferSize = 1024 //FIXME make configurable
  val buffer = ByteBuffer.allocate(bufferSize)

  override def run() 
  {
    while (!server.stopped && !Thread.currentThread ().isInterrupted ()) 
    {
      ZMQ.poll(items, 500) //FIXME timeout?
      //if (rc == -1) break //  Context has been shut down

      if (items(0).isReadable) controlMessage 
      if (items(1).isReadable) clientMessage
    }
  }

  def clientMessage = 
  {
    val zmsg = ZMsg.recvMsg(worker) 
    val client = zmsg.pop //client id
    val clientId = client.toString
    val protocol_version = zmsg.pop
    val cmd = zmsg.pop //cmd is used to continue/stop
    dbg(cmd+" from "+client)
    cmd.toString match {
      case HELLO => 
        server.clients.getOrElseUpdate(clientId, Waiting(client)) //add client to known clients list, if not present
        server.next //request next file; the file name will come as a control message

      case GET => 
        val file = zmsg.pop.toString
        val position = zmsg.pop.toString.toLong
        dbg(s"clientReq: cmd: $cmd , file:$file, position=$position")

        getFileChannel(clientId, file) map { channel =>
          // we got a fileChannel: read from it, from the requested position
          buffer.position(0)
          Try(channel.read(buffer, position)) map { read => //nb of bytes read
            if (read < bufferSize) //eof reached
            {
              dbg(s"$read bytes sent for $file, eof reached")
              val last = new Array[Byte](read)
              buffer.position(0)
              buffer.get(last)
              send(client, last, EOF, file)
              server.clients.remove(clientId) match {
                case Some(Serving(channel)) => channel.close
                  case _ => //ignore, should not happen
              }
            }
            else 
            {
              channel.position(position+read)
              dbg(s"$read bytes sent for $file, new position is ${channel.position}")
              send(client, buffer.array, CHUNK, file, channel.position.toString)
            }
          } 
          // FIXME in case of read failure, nothing is sent: let the client retry after its timeout?
        } recover { 
          // IO error on this file: abort and notify application (FIXME)
          case x: Throwable => send(client, Array(), ABORT, file)
        }

      case ABORT => server.clients.remove(clientId)

      case x => dbg("cmd "+x+" unknown / not implemented!") //FIXME: forward msg to application layer 
    }
    zmsg.destroy
  }

  def controlMessage = 
  {
    val zmsg = ZMsg.recvMsg(control) 
    val cmd = zmsg.pop.toString
    dbg(cmd+"from server-control")
    cmd match {
      case NEWFILE => 
        val filepath = zmsg.pop.toString
        dbg(s"got newfile control $filepath")
        val file = new File(filepath)
        if (!file.exists) 
        {
          println(s"""404 file not found! $filepath (user.dir=${System.getProperty("user.dir")})""")
        }
        else server.clients.find {
          case (_, Waiting(_)) => true
          case _ => false
        } match {
          case Some((_, Waiting(client))) =>
            send(client, filepath.getBytes, NEWFILE, ""+file.length)
          case None => //wait for next hello
        }

      case x => dbg("control "+x+" unknown / not implemented!") 
    }
    zmsg.destroy
  }

  def getFileChannel(clientId: String, file: String): Try[FileChannel] = server.clients.get(clientId) match {
    case Some(Serving(fc)) => Success(fc)
    case _ => Try {
      val fc = new RandomAccessFile(file, "r").getChannel
      server.clients.put(clientId, Serving(fc))
      fc
    }
  }

  def dbg(s: => String) = if (java.lang.Boolean.getBoolean("debug")) println(this.hashCode+"@"+Thread.currentThread+": "+s)

  def send(to: ZFrame, data: Array[Byte], cmd: String*) = {
    val msg = new ZMsg()
    msg.add(to)
    msg.addString(PROTOCOL_VERSION)
    for (c <- cmd) msg.addString(c)
    msg.add(data)
    msg.send(worker, true)
  }
}



