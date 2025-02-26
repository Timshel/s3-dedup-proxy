package timshel.s3dedupproxy

import cats.effect.IO

import org.http4s._
import org.http4s.dsl.io._

case class RedirectionController(
  config: BackendConfig,
  db: Database,
) {
  import RedirectionController._

  object StringVar {
    def unapply(str: String): Option[String] = Some(str)
  }

  val routes = org.http4s.HttpRoutes.of[IO] {
    case _ @ GET -> StringVar(identity) /: StringVar(bucket) /: key =>
      db.getMappingHash(identity, bucket, key.toString).flatMap {
        case None => IO.pure(Response[IO](Status.NotFound))
        case Some(hash) =>
          val path = ProxyBlobStore.hashToKey(hash)
          PermanentRedirect(config.publicHost + "/" + path)
      }
  }
}

object RedirectionController {
  val log = com.typesafe.scalalogging.Logger(classOf[RedirectionController])
}
