package topologies
import topologies.chord._
import org.scalatest._
import Matchers._ 
import com.google.common.hash._


class ChordSpec extends FlatSpec with Matchers  {
  val sha1 = Hashing.sha1() 
  val nodes =  List("node1", "node2", "node3", "node4", "node5").map(x => Node(x, sha1.hashBytes(x.getBytes).asBytes() ) ).sortWith( Node.rcompare(_, _) )


  val succL = Ring.successors(nodes, nodes(4), 3)
  val pl = nodes.take(3)

  succL should contain theSameElementsAs(pl)

  val predL = Ring.predecessors(nodes, nodes(3), 3)
  predL should contain theSameElementsAs succL

  val predL2 = Ring.predecessors(nodes, nodes(0), 3)
  predL2 should contain allOf (nodes(4), nodes(3), nodes(2) )

  val r = PreferenceList.partition(nodes, Node.rcompare, 3)
  assert(r.size == 2)
  assert(r.head.size == 3)
  assert(r.last.size == 3)
}
