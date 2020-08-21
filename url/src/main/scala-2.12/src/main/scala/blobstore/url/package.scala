package blobstore

package object url {
  implicit class TypeConstraintsOps[From, To](ev: =:=[From, To]) {
    def substituteBoth[F[_, _]](ftf: F[To, From]): F[From, To] = ftf.asInstanceOf[F[From, To]]
    /** If `From = To` then `To = From` (equality is symmetric) */
    def flip: To =:= From = {
      type G[T, F] = F =:= T
      substituteBoth[G](ev)
    }
  }
}
