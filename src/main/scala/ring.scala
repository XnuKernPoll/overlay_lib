import com.twitter.io.{Buf}
import com.google.common.hash._ 
import store_lib.storage.{util => storage_util}

case class Node(key: String, hash: Array[Byte])

object Node {

  def lcompare(l: Node, r: Node): Boolean = {
    val (ln, rn): (HashCode, HashCode) = ( HashCode.fromBytes(l.hash), HashCode.fromBytes(r.hash) )
    ln.asLong() > rn.asLong()  
  }

  def rcompare(l: Node, r: Node): Boolean = {
    val (ln, rn): ( HashCode.fromBytes(l.hash), HashCode.fromBytes(r.hash) )
    rn.asLong() > ln.asLong() 
  }
 


}


case class Neighborhood(my_node: Node, neighbors: List[Node]) {

  def update(n: Node) = {
    val nptbl = ( neighbors.toSet + n) 
    val updated = this.copy(neighbors = nptbl)
    Neighborhood.sorted(updated)
  }

  def delete(n: Node) = {
    val nptbl = (neighbors.toSet - n)
    val updated = this.copy(neighbors = nptbl)
    Neighborhood.sorted(nptbl)
  }


}

object Neighborhood {
  def sorted(n: Neighborhood): Neighborhood = n.copy(neighbors =  n.neighbors.sortWith( Node.rcompare(_, _) )  )  
}



object Chord {

  type nodeID = Array[Byte]


  type node_sort = (Node, Node) => Boolean


  object Ring {

    def getBucket(n: Neighborhood, keyHash: nodeID): Int = {
      val hc = HashCode.fromBytes(keyHash)
      Hashing.consistentHash(keyHash, Neighborhood.neighbors(n).size ) 
    }

    def predecessor(n: Neighborhood, keyHash: nodeID) = {
      val i = getBucket(n, keyHash) - 1
      val pl = n.sorted
      pl(i)
    }

    def successor(n: Neighborhood, keyHash: nodeID) = {
      val i = getBucket(n, keyHash) + 1
      val pl = n.sorted
      pl(i)
    }
 
  }

}


