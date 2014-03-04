package ap.test.jeromq.asyncsrv

import org.zeromq._
import java.io._

object ZDealer {

 val context = new ZContext(1)
 
 def main(args:Array[String]) :Unit = args.toList match {
   case nbClients::nbReqs::tail => List.fill(nbClients.toInt) { new Client(nbReqs.toInt) }.foreach( _.start )
   case _ => println("ZDealer [nbClients nbReqs]")
 }

 class Client(nbReqs:Int) extends ClientBase(context, ZMQ.DEALER, sok => {
   val items = Array(new ZMQ.PollItem(sok, ZMQ.Poller.POLLIN))
   var i = 0
   while(true) {//for (i <- 0 to nbReqs) {
     //async send req
     i = i + 1
     println("send("+Thread.currentThread.getName+"-"+i+")")
     sok.send(Thread.currentThread.getName+"-"+i, 0)

     //poll for available responses
     ZMQ.poll(items, 500)
     if (items(0).isReadable) {
       val rsp = ZMsg.recvMsg(sok)
       println("got resp "+rsp.getLast.toString)
       rsp.destroy
     }
   }
   context.destroy
 })
}

class ClientBase(context:ZContext, typ:Int, f: ZMQ.Socket => Unit) extends Thread {
 override def run = {
   val io = context.createSocket(typ)
   io.connect("tcp://*:5570")
   f(io)
   context.destroy
 }
}


