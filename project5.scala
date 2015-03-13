/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package project5

import java.util.ArrayList
import java.util.Collections
import java.util.Random
import java.util.logging.FileHandler
import java.util.logging.Logger
import java.util.logging.SimpleFormatter
import scala.actors.Actor

object project5 {
  def main(args: Array[String]): Unit = {
    var numNodes = 1000
    var numMsgs = 10
    NumMsgs.numMsgs = numMsgs
    
    var Nodes = new Array[Node](numNodes)
    for(i<-0 until numNodes) {
      var nbr:Node = null
      if(i==1) {
        nbr = Nodes(0)
      }else if( i>1 ) {
        nbr = Nodes(new Random().nextInt(i-1))
      }
      Nodes(i)=new Node(Utility.createRandomString(8), nbr);
      Nodes(i).start
      Thread.sleep(1L)
    }
  }
}

trait ActorLogger extends Actor {
  var logger = Logger.getLogger(this.toString)
  var fileHandler = new FileHandler(this.toString+".log", true)
  logger.addHandler(fileHandler)
  
  var formatter = new SimpleFormatter()
  fileHandler.setFormatter(formatter)
  
  var lpts = 0
  override def react(handler: PartialFunction[Any, Unit]): Nothing = {
      val handler2: PartialFunction[Any, Unit] = {
        case DMsg(ts, msg) => {
          if( ts > lpts ) {
            lpts = ts
          }
          lpts = lpts + 1
          log(lpts, "RECEIVE", sender.toString, msg.toString, "")
          handler.apply(msg);
        }
      }
      super.react(handler2)
  }

// the problem here is that I need the sender who sent this msg to this recipient
// but the bang(!) method is invoked on the recipient itself and there doesn't
// apparently seem to be a way to find out who invoked this method
// some ideas are use the stack trace from an exception, but that also doesn't 
// give the reference to the sender
//  ------------  
//  override def !(msg: Any): Unit = {
//    lpts = lpts + 1
//    log(lpts, "SEND", "[to:"+this.toString+"],[Msg:"+msg.toString+"]")
//    super.!(new DMsg(lpts, msg))
//  }
  
//  we would like to achieve msg logging with minimum amount of code in the 
//  client program but as it's mentioned above it's not achievable overriding the 
//  bang(!) operator. Here we create a new method send that envelops the 
//  -----------------
  def send(to:Actor, msg: Any): Unit = {
    lpts = lpts + 1
    log(lpts, "SEND", to.toString, msg.toString, "")
    to ! new DMsg(lpts, msg)
  }
  
  def log(ts:Int, etype:String, toFrom: String, msg:String, desc:String) = {
    logger.info("id:"+this.toString+" | "+"ts:"+ts+" | "+"event:"+etype+" | "+"to/from:"+toFrom+" | "+"msg:"+msg+" | "+"desc:"+desc)
  }
}

object NumMsgs{
  var numMsgs=0
}

object Counter{
  private var current = 0
  def inc = {current += 1; current}
}

abstract class NodeMsg
case class joinNW(newNode:Node) extends NodeMsg
case class ackJoin(rt:routingTable) extends NodeMsg
case object joinedNW extends NodeMsg
case object joinedNWAck extends NodeMsg
case class MsgMsg(from:Node, dest:String, hopCount:Int) extends NodeMsg
case class MsgDelivered(hopCount:Int) extends NodeMsg

case class DMsg(ts:Int, msg:Any)

class Node(idi: String, nbr:Node) extends ActorLogger with Comparable[Node] {
  var avgHops=0
  var msgC=0
  val id = idi
  //var routingTable = new routingTable(new nodeIdentifier(id, ""));
  var routingTable = new routingTable(this);
  if(nbr==null) {
    //do nothing
    //println("this is the first node")
  } else {
    //send joinRequest to nbr
    //println("neighbour is "+nbr.id)
    //nbr ! new joinNW(this)
    send(nbr, new joinNW(this))
  }
  
  def act{
    loop{
      react {
        case `joinNW`(newNode) => {
            //println("Join request recieved from "+newNode.id)
            //1.send the routing table of this node back to the sender
            //.something like
            //newNode ! ackJoin(this.routingTable)
            //newNode ! new ackJoin(routingTable)
            send(newNode, new ackJoin(routingTable))
            //2.propagate this msg as farther as possible
            //.something like
            var nextNode = routingTable.getNextNode(newNode)
            if(nextNode!=null) {
              //nextNode ! joinNW(newNode)
              send(nextNode, joinNW(newNode))
            }
        }
        case `ackJoin`(rt) => {
            //println("Recieved back the routing table from "+rt.curNodeId.id)
            //update own routing table and send back info to all the nodes in
            //the routing table to put this nodes id into their routing tables
            //or leaf sets
            var commPrefixLength = Utility.prefixLength(id, rt.curNodeId.id)
            var row = commPrefixLength
            //copy all the rows from the sent routing table into this nodes routing 
            //table till where the prefix matches
            for(i<-0 to row) {
              for(j<-0 until 16) {
                routingTable.rTable(i)(j) = rt.rTable(i)(j)
              }
            }
            //sender ! joinedNW
            send(sender.asInstanceOf[Node], joinedNW)
        }
        case `joinedNW` => {
            //this msg is recieved when the sender has incorporated the routing tables
            //of all nodes it approached during joining and it has sent back this 
            //msg now
            routingTable.put(sender.asInstanceOf[Node])
            //ack the reciept of this msg
            //sender ! joinedNWAck
            send(sender.asInstanceOf[Node], joinedNWAck)
        }
        case `joinedNWAck` => {
            //this finalises the network join phase
            //now this node is good to send msgs
            for (i<-0 until NumMsgs.numMsgs) {
              val msg = Utility.createRandomString(8)
              var nextNode = routingTable.getNextNode(msg)
              if(nextNode==null) {
                //println("No dest found")
                msgC = msgC+1
                if(msgC==NumMsgs.numMsgs) {
                  println(Counter.inc+": "+NumMsgs.numMsgs+" msgs delivered in avg. hops: "+(avgHops*1.0/NumMsgs.numMsgs).ceil)
                }
              } else {
                //nextNode ! new MsgMsg(this, msg, 0)
                send(nextNode, new MsgMsg(this, msg, 0))
              }
            }
        }
        case `MsgMsg`(from, to, hops) => {
            val msg = to
            var nextNode = routingTable.getNextNode(msg)
            if(nextNode==null) {
              //from ! new MsgDelivered(hops+1)
              send(from, new MsgDelivered(hops+1))
            } else {
              //nextNode ! new MsgMsg(from, msg, hops+1)
              send(nextNode, new MsgMsg(from, msg, hops+1))
            }
        }
        case `MsgDelivered`(hops) => {
            msgC=msgC+1
            //println("Hops "+hops)
            avgHops=avgHops+hops
            if(msgC==NumMsgs.numMsgs) {
              println(Counter.inc+": "+NumMsgs.numMsgs+" msgs delivered in avg. hops: "+(avgHops*1.0/NumMsgs.numMsgs).ceil)
            }
        }
      }
    }
  }
  
    @Override
    def compareTo(n:Node):Int = {
        if( java.lang.Long.parseLong(this.id, 16) > java.lang.Long.parseLong(n.id, 16) ) {
            return -1;
        } else if(java.lang.Long.parseLong(this.id, 16) < java.lang.Long.parseLong(n.id, 16)) {
            return 1;
        } else
            return 0;
    }  
}

object Utility {
  def createRandomString(length: Int): String = {
          var random = new Random();
          var sb = new StringBuilder();
          while (sb.length() < length) {
                  sb.append(Integer.toHexString(random.nextInt(16)));
          }
          return sb.toString();
  }
  
  def findCommonPrefix(s1 : String, s2 : String) : String = {
      def findCommonPrefixR(l1: List[Char], l2 : List[Char]) : List[Char] = {
          l1 match {
          case Nil => Nil
          case x::xs => if (l2 != Nil && l2.head == x) x :: findCommonPrefixR(xs, l2.tail) else Nil
          }
      }
      findCommonPrefixR(s1.toList, s2.toList).mkString
  }
  
  def prefixLength(s1: String, s2: String): Int = {
    return findCommonPrefix(s1,s2).length
  }
}

class routingTable(nd:Node) {
  var curNodeId = nd
//  var rTable:Array[Array[nodeIdentifier]]=Array.ofDim[nodeIdentifier](8,16)
  var rTable:Array[Array[Node]]=Array.ofDim[Node](8,16)
  var leafSet:ArrayList[Node]= new ArrayList[Node]
  
  def put(idi:Node) = {
    var idiIntId = java.lang.Long.parseLong(idi.id.toString, 16)
    var curNodeIntId = java.lang.Long.parseLong(curNodeId.id.toString, 16)
    var diff = idiIntId-curNodeIntId
    if(diff < 0) {
      diff = diff*(-1)
    }
    //println(diff)
    if(diff < 1000000) {
      if(leafSet.size==16) {
        leafSet.remove(15)
      }
      //println("Put into leaf set diff:"+diff)
      leafSet.add(idi)
      Collections.sort(leafSet)
    }
    else {
      var commPrefixLength = Utility.prefixLength(curNodeId.id, idi.id)
      var row = commPrefixLength
      var col = Integer.parseInt(idi.id.charAt(commPrefixLength).toString, 16)
      rTable(row)(col)=idi      
    }
  }
  
  def getNextNode(idi:Node):Node = {
    var idiIntId = java.lang.Long.parseLong(idi.id.toString, 16)
    var curNodeIntId = java.lang.Long.parseLong(curNodeId.id.toString, 16)
    var diff = idiIntId-curNodeIntId
    if(diff < 0) {
      diff = diff*(-1)
    }
    //println(diff)
    if(diff < 1000000 && leafSet.size > 0) {
      //println("get from leafset")
      leafSet.get(0)
    } else {
      var commPrefixLength = Utility.prefixLength(curNodeId.id, idi.id)
      var row = commPrefixLength
      var col = Integer.parseInt(idi.id.charAt(commPrefixLength).toString, 16)
      if(rTable(row)(col)!=null)
        rTable(row)(col)
      else
        null      
    }
  }
  
  def getNextNode(idi:String):Node = {
    var idiIntId = java.lang.Long.parseLong(idi, 16)
    var curNodeIntId = java.lang.Long.parseLong(curNodeId.id.toString, 16)
    var diff = idiIntId-curNodeIntId
    if(diff < 0) {
      diff = diff*(-1)
    }
    //println(diff)
    if(diff < 1000000 && leafSet.size > 0) {
      //println("get from leafset")
      leafSet.get(0)
    } else {
      var commPrefixLength = Utility.prefixLength(curNodeId.id, idi)
      var row = commPrefixLength
      var col = Integer.parseInt(idi.charAt(commPrefixLength).toString, 16)
      if(rTable(row)(col)!=null)
        rTable(row)(col)
      else {
        //search for next closest
        for(i<-row to 0) {
          for(j<-0 until 16) {
            if(rTable(i)(j)!=null) {
              return rTable(i)(j)
            }
          }
        }
        //if cannot find one in routing table just use one in the leafset
        if(leafSet.size > 0) {
          leafSet.get(0)
        }
        else {
          null
        }
      }
    }
  }  
  
  def toStringV():String = {
    var s:String="NodeId:"+curNodeId.id+"\n"
    for(i <- 0 until 8) {
      s+="row["+i+"]: "
      for(j <- 0 until 15) {
        if(rTable(i)(j)!=null)
          s+=rTable(i)(j).id+" "
        else
          s+="null "
      }
      s+="\n"
    }
    s
  }
}

class nodeIdentifier(idi:String, ipi:String) {
  var id=idi
  var ip=ipi
}