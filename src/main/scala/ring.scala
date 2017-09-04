package chord

import com.twitter.io.{Buf}
import com.google.common.hash._ 

case class Node(key: String, hash: Array[Byte])

object Node {


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

 
case class Neighborhood(my_node: Node, neighbors: List[Node]) {

  def update(n: Node) = {
    val nptbl = ( neighbors.toSet + n).toList
    val updated = this.copy(neighbors = Chord.sortPeers( nptbl )  )
    updated
  }

  def delete(n: Node) = {
    val nptbl = (neighbors.toSet - n).toList
    val updated = this.copy(neighbors = Chord.sortPeers( nptbl )  )
    updated 
  }


}


object Chord {

  import Node.hashLong
  type nodeID = Array[Byte]


  def sortPeers(peers: List[Node] ) = peers.sortWith( Node.rcompare(_, _) )
  type node_sort = (Node, Node) => Boolean


  object Ring {

    def getBucket(peers: List[Node], keyHash: nodeID): Int = {
      val hc = HashCode.fromBytes(keyHash)
      Hashing.consistentHash(HashCode.fromBytes( keyHash) , peers.size) 
    }

    def predecessor(peers: List[Node], keyHash: nodeID): Node = {
      val i = getBucket(peers, keyHash) - 1
      val pl = sortPeers(peers) 
      pl(i)
    }

    def successor(peers: List[Node], keyHash: nodeID): Node = {
      val i = getBucket(peers, keyHash) + 1
      val pl = sortPeers(peers) 
      pl(i)
    }

    def successors(peers: List[Node], keyHash: nodeID): List[Node] = {
      val successors = peers.filter( x => hashLong(x.hash) >= hashLong( successor(peers, keyHash).hash )  ) 
      sortPeers(successors)
    }

    def predecessors(peers: List[Node], keyHash: nodeID): List[Node] = {
      val predecessors = peers.filter( x => hashLong(x.hash) <= hashLong( predecessor(peers, keyHash).hash )   )
      predecessors.sortWith( Node.rcompare(_, _) )
    }

  }


}



object PreferenceList {

  type Shards[T] = List[List[T]]

  def route[T](shards: Shards[T], keyHash: Array[Byte]) = {
    val b = Hashing.consistentHash(HashCode.fromBytes( keyHash), shards.size )
    shards(b) 
  }

  def inSets(shards: Shards[T], item: T) = shards.filter(shard => shard.contains(item) )

  type SF[T] = (T, T) => Boolean


  def partition[T](l: List[T], sorting: SF,  shardSize: Int): Shards  = {
    val remainder = l.size ^ shardSize
    val peers = peers.sortWith( sorting(_, _) ) 
    val partitions = peers.grouped(shardSize).toList
    val t = if (remainder > 0) ( partitions.last.toSet ++ l.take(remainder).toSet  ).toList else  { partitions.last }
    ( partitions.dropRight(1) :+ t ).toList
  }


}
