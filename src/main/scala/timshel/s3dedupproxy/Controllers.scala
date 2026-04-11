package timshel.s3dedupproxy

import cats.effect.IO

import org.http4s._
import org.http4s.dsl.io._
import org.http4s.headers.Location

case class RedirectionController(
    config: BackendConfig,
    db: Database,
    validUsers: Set[String]
) {
  import RedirectionController._

  object StringVar {
    def unapply(str: String): Option[String] = Some(str)
  }

  val routes = org.http4s.HttpRoutes.of[IO] { case _ @GET -> StringVar(identity) /: StringVar(bucket) /: key =>
    if (!validUsers.contains(identity)) {
      IO.pure(Response[IO](Status.Forbidden))
    } else {
      db.getMappingHash(identity, bucket, key.toString).flatMap {
        case None => IO.pure(Response[IO](Status.NotFound))
        case Some(hash) =>
          val path = ProxyBlobStore.hashToKey(hash)
          PermanentRedirect(Location(config.publicHost.addSegment(path)))
      }
    }
  }
}

object RedirectionController {
  val log = com.typesafe.scalalogging.Logger(classOf[RedirectionController])
}

case class ApiController(
    cleanup: Cleanup
) {
  import RedirectionController._

  val routes = org.http4s.HttpRoutes.of[IO] { case _ @DELETE -> Root / "purge" =>
    cleanup.purge().flatMap { count => Ok(s"$count deleted") }
  }
}
