package topologies 
import com.twitter.io.{Buf}
import com.google.common.hash._
import java.nio.charset.StandardCharsets
import io.circe._, io.circe.generic.semiauto._
/** Node for use in dhts **/
case class Node(host: String, port: Int, hash: Array[Byte]) {
  def toHostPort = HostPort(host, port)
}



case class HostPort(address: String, port: Int) {
  def asString = s"${this.address}:${this.port}"

  def toNode: Node = {
    val hash = Hashing.sha1().hashString( this.asString, StandardCharsets.UTF_8 ).asBytes() 
    Node(this.address, this.port, hash) 
  }

}


object Node {

  implicit val nodeEncoder = deriveEncoder[Node]
  implicit val nodeDecoder = deriveDecoder[Node]

  def hashLong(bytes: Array[Byte] ): Long =  HashCode.fromBytes(bytes).asLong()

  def lcompare(l: Node, r: Node): Boolean = {
    val (lv, rv): (Long, Long) = (hashLong(l.hash), hashLong(r.hash)  )
    lv > rv
  }

  def rcompare(l: Node, r: Node): Boolean = {
    val (ln, rn): (Long, Long)  = (hashLong(l.hash), hashLong(r.hash) ) 
    rn > ln
  }
 
}

/** the state needed for p2p routing */
case class Neighborhood(my_node: Node, neighbors: List[Node]) {

  def join(n: Node) = {
    val nptbl = ( neighbors.toSet + n).toList
    val updated = this.copy(neighbors = Chord.sortPeers( nptbl )  )
    updated
  }

  def leave(n: Node) = {
    val nptbl = (neighbors.toSet - n).toList
    val updated = this.copy(neighbors = Chord.sortPeers( nptbl )  )
    updated 
  }

}



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


/** Basic Chord Implementation */
object Chord {

  import Node.hashLong
  type nodeID = Array[Byte]
 

  def sortPeers(peers: List[Node] ) = peers.sortWith( Node.rcompare(_, _) )
  type node_sort = (Node, Node) => Boolean


  object Ring {

    def getBucket(peers: List[Node], keyHash: nodeID): Int = {
      val hc = HashCode.fromBytes(keyHash)
      Hashing.consistentHash(HashCode.fromBytes(keyHash) , peers.size) - 1
    }

    def getNode(peers: List[Node], hash: nodeID) = peers( getBucket(peers, hash) ) 

    def successorList(peers: List[Node], keyHash: nodeID) = {
      val n = getNode(peers, keyHash)
      peers.filter( x => hashLong(x.hash) >= hashLong(n.hash) )
    }

    def successors(peers: List[Node], keyHash: nodeID, f: Int): List[Node] = {
      val succs = sortPeers( successorList(peers, keyHash) )

      if ( succs.size >= f) succs.take(f) else {
        val rem = f - succs.size
        val pl = sortPeers { predecessorList(peers, keyHash) }.take(rem)
        succs ++ pl 
      }

    }


    def predecessors(peers: List[Node], hash: nodeID, f: Int) = {
      val preds = predecessorList(peers, hash).sortWith(Node.rcompare)
      if (preds.size >= f) preds.take(f) else {
        val rem = f - preds.size
        val pl = sortPeers { successorList(peers, hash) }.take(rem)
        preds ++ pl 
      }
    }


    def predecessorList(peers: List[Node], keyHash: nodeID): List[Node] = {
      val n = getNode(peers, keyHash)

      peers.filter { x =>
        hashLong(x.hash) <= hashLong(n.hash)
      }

    }
    

  }


}


/**
  A module to create replica sets, and use consistent hashing to pick the appropriate partitiion 
*/
object PreferenceList {

  type Shards[T] = List[List[T]]

  def route[T](shards: Shards[T], keyHash: Array[Byte]) = {
    val b = Hashing.consistentHash(HashCode.fromBytes( keyHash), shards.size )
    shards(b) 
  }

  def inSets[T](shards: Shards[T], item: T) = shards.filter(shard => shard.contains(item) )

  type SF[T] = (T, T) => Boolean


  def partition[T](l: List[T], sorting: SF[T],  shardSize: Int): Shards[T]  = {
    val remainder = l.size ^ shardSize
    val peers = l.sortWith( sorting(_, _) ) 
    val partitions = peers.grouped(shardSize).toList
    val t = if (remainder > 0) ( partitions.last.toSet ++ l.take(remainder).toSet  ).toList else  { partitions.last }
    ( partitions.dropRight(1) :+ t ).toList
  }


}


