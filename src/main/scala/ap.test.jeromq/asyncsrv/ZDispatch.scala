package ap.test.jeromq.asyncsrv

import org.zeromq._

object ZDispatch {

 val context = new ZContext(1)
 val adr = "tcp://*:5570"
 
 def main(args:Array[String]) :Unit = {

   val io = context.createSocket(ZMQ.ROUTER) //IO
   io.bind(adr)
   println("server started @ "+adr)

   val dispatch = context.createSocket(ZMQ.DEALER) //workers dispatcher
   dispatch.bind("inproc://work")

   for ( i <- 1 to 2) new Worker(context).start

   ZMQ.proxy(io, dispatch, null)

   context.destroy
 }

 class Worker(ctx:ZContext) extends Thread {
   override def run = { 
     val in = ctx.createSocket(ZMQ.DEALER)
     in.connect("inproc://work")
     while(true) {
       Option(ZMsg.recvMsg(in)).foreach { msg =>
         val from = msg.pop
         val req = msg.pop
         msg.destroy
         println(Thread.currentThread+" reply to "+req)
         from.send(in, ZFrame.REUSE + ZFrame.MORE)
         req.send(in, ZFrame.REUSE)
         from.destroy
         req.destroy
       }
     }
   }
 }
}
