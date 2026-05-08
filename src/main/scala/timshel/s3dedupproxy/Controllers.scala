package timshel.s3dedupproxy

import cats.data.{Kleisli, OptionT}
import cats.effect.IO

import org.http4s._
import org.http4s.dsl.io._
import org.http4s.headers.Location

final case class RedirectionController(
    config: BackendConfig,
    db: Database
) {
  import RedirectionController._

  object StringVar {
    def unapply(str: String): Option[String] = Some(str)
  }

  val routes = org.http4s.HttpRoutes.of[IO] { case _ @GET -> StringVar(identity) /: StringVar(bucket) /: key =>
    db.getMappingHash(identity, bucket, Uri.decode(key.renderString)).flatMap {
      case None => IO.pure(Response[IO](Status.NotFound))
      case Some(hash) =>
        val path = ProxyBlobStore.hashToKey(hash)
        PermanentRedirect(Location(config.publicHost.addPath(path)))
    }
  }
}

object RedirectionController {
  val log = com.typesafe.scalalogging.Logger(classOf[RedirectionController])
}

final case class ApiController(
    cleanup: Cleanup
) {
  import RedirectionController._

  val routes = ApiController.localOnly(org.http4s.HttpRoutes.of[IO] { case _ @DELETE -> Root / "purge" =>
    cleanup.purge().flatMap { count => Ok(s"$count deleted") }
  })
}

object ApiController {
  val log = com.typesafe.scalalogging.Logger(classOf[ApiController])

  def localOnly(service: HttpRoutes[IO]): HttpRoutes[IO] = Kleisli { (req: Request[IO]) =>
    val isLocal = req.remoteAddr.exists { addr =>
      addr.asIpv4.exists { a => a.isLoopback || a.isPrivate } || addr.asIpv6.exists { a => a.isLoopback || a.isPrivate }
    }
    log.debug(s"localOnly check for ${req.remoteAddr} - $isLocal")
    if (isLocal) service(req) else OptionT.some(Response[IO](Status.Forbidden))
  }

}
