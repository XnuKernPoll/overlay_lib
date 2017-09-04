object RoutingWriters {
  def origin(peer: String, req: Request) = { req.xForwardedFor_=(peer); req }
  def lastHop(peer: String, req: Request) = { req.referrer_(peer), req}

  
}
