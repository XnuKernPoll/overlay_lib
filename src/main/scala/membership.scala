package topologies.membership

import com.twitter.finagle.mux._
import topologies._
import com.twitter.util._, com.twitter.finagle.{Service, Path}


import com.twitter.io.Buf 
import topologies._ 
import scodec._, codecs.{uint8}, scodec.bits.BitVector

import Buf.ByteArray.Shared


class PeerState(seed: Neighborhood, version: PNCounter = PNCounter.zero)  {
  var peers = seed
  var clock = version

  def join(n: Node) = {
    synchronized { peers = peers.join(n) }
    synchronized { clock = clock.incr } 
  }

  def leave(n: Node) = {
    synchronized { peers = peers.leave(n) }
    synchronized { clock = clock.decr} 
  }


}



object MembershipServer {

  def leave(state: PeerState, node: Node) = Future { state.leave(node) }.map {x =>
    val data = Buf.Utf8(s"peer ${node.key} was removed")
    Response(data)
  }

  def join(state: PeerState, node: Node) = Future { state.join(node) }.map { x =>
    val data = Buf.Utf8(s"peer ${node.key} was added")
    Response(data)
  }


  def list(state: PeerState) = Future {
    val nodes = state.peers.neighbors
    val payload = Node.listCodec.encode(nodes).toOption.map(bv => Shared(bv.toByteArray) )
    Response(payload.get) 
  }


  def getNode(req: Request) = {
    val buf = BitVector( Shared.extract( req.body )  )
    Node.codec.decode(buf).toOption.map{x => x.value}
  }


  def performOP(req: Request, state: PeerState, op: ( PeerState, Node )  => Future[Response] ) = getNode(req) match {
    case Some(node) => op(state, node)
    case None =>
      val buf = Buf.Utf8("unable to perform membership change")
      Future{ Response(buf) }
  }
  


  def handler(state: PeerState) = Service.mk[Request, Response] { req =>
    req.destination match {
      case Path.Utf8("membership", "join") => performOP(req, state, join)
      case Path.Utf8("membership", "leave") => performOP(req, state, leave)
      case Path.Utf8("membership", "list") => list(state)
    }
  }

}



object MembershipClient {

  def join(node: Node, conn: Service[Request, Response]): Future[Unit] = {
    val path = Path.Utf8("membership", "join")
    val buf = Node.codec.encode(node).toOption.map {x => Shared(x.toByteArray) }
    Future { Request(path, buf.get) }.flatMap {req => conn(req) }.map(x => ()) 
  }


  def leave(node: Node, conn: Service[Request, Response]): Future[Unit] = {
    val path = Path.Utf8("membership", "leave")
    val buf = Node.codec.encode(node).toOption.map {x => Shared(x.toByteArray) }
    Future { Request(path, buf.get) }.flatMap {req => conn(req) }.map(x => ()) 
  }

  def list(conn: Service[Request, Response]): Future[ List[Node] ] = {
    val path = Path.Utf8("membership", "list")
    val req = Request(path, Buf.Empty)

    conn(req).map {rep =>
      val buf =  BitVector( Shared.extract( rep.body )  )
      Node.listCodec.decode(buf).toOption.map(x => x.value).get 
    }
  }

}
