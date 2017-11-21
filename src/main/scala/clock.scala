package topologies
import io.circe._, io.circe.generic.semiauto._

/**  counters are logical clocks for causal ordering of events */

sealed trait Counter {
  def incr: Counter
  def version: Long 
}


object Counter {
  def compare(l: Counter, r: Counter) = l.version - r.version
}


/**  Vector Clocks, are labeled monotonic counters */
case class VectorClock(key: String, c: Long)  extends Counter {
  def incr: VectorClock = copy( c = (c + 1L )  )
  def version: Long = c
}


/**  Has two monotonic counters, one for adds, the other for deletes**/

case class PNCounter(key: String, add: Long, delete: Long ) extends Counter {
  def incr: PNCounter = copy( add = (add + 1L ) )
  def decr: PNCounter = copy( delete = ( delete + 1L ) )
  def version: Long = add + delete
}


object PNCounter {
  implicit val PNEncoder = deriveEncoder[PNCounter]
  implicit val PNDecoder = deriveDecoder[PNCounter]
  def zero = PNCounter("", 0L, 0L)
}


object VectorClock {
  implicit val VCEncoder = deriveDecoder[VectorClock]
  implicit val VCDecoder = deriveDecoder[VectorClock]
  def zero: VectorClock = VectorClock("", 0L)
}

