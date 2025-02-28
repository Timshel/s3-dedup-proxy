package timshel.s3dedupproxy

import cats.effect._
import cats.effect.std.Dispatcher
import java.util.concurrent.Executors
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.middleware.Logger
import org.postgresql.ds.PGSimpleDataSource
import scala.concurrent.ExecutionContext
import skunk._
import skunk.codec.all._
import skunk.implicits._

case class Application(
    config: GlobalConfig,
    database: Database,
    proxy: org.gaul.s3proxy.S3Proxy,
    http: org.http4s.server.Server,
    cleanup: Cleanup
)

object Application extends IOApp {
  val log = com.typesafe.scalalogging.Logger(classOf[Application])

  /** */
  def run(args: List[String]): IO[ExitCode] = {
    default().use { app =>
      IO.never
    }
  }

  def default(): Resource[IO, Application] = {
    IO.blocking {
      pureconfig.ConfigSource.default.load[GlobalConfig] match {
        case Left(e)       => throw new RuntimeException(e.prettyPrint());
        case Right(config) => config
      }
    }.toResource.flatMap(cs => using(cs))
  }

  def using(config: GlobalConfig): Resource[IO, Application] = {
    import natchez.Trace.Implicits.noop

    (for {
      pool <- Session.pooled[IO](
        host = config.db.host.toString,
        port = config.db.port.value,
        user = config.db.user,
        database = config.db.database,
        password = Some(config.db.pass),
        max = 10
      )
      database = Database(pool)(runtime)
      dispatcher <- Dispatcher.parallel[IO]
      proxy   <- ProxyBlobStore.createProxy(config, database, dispatcher)
      cleanup <- Cleanup.scheduled(config, database, dispatcher)
      httpApp = org.http4s.server
        .Router(
          "/proxy/" -> RedirectionController(config.backend, database).routes,
          "/api/" -> ApiController(cleanup).routes
        )
        .orNotFound
      http <- EmberServerBuilder
        .default[IO]
        .withHost(config.api.host)
        .withPort(config.api.port)
        .withHttpApp(Logger.httpApp(true, true)(httpApp))
        .build
    } yield Application(config, database, proxy, http, cleanup))
      .preAllocate {
        migration(config.db)
      }
  }

  def migration(config: DBConfig): IO[Unit] = {
    (for {
      ds  <- simpleDataSource(config)
      fly <- IO.blocking(org.flywaydb.core.Flyway.configure().dataSource(ds).load())
      mr  <- IO.blocking(fly.migrate())
    } yield mr)
      .flatMap {
        case mr if mr.success => IO.pure(())
        case _                => IO.raiseError(new RuntimeException("Migration failure"))
      }
  }

  def simpleDataSource(config: DBConfig): IO[PGSimpleDataSource] = IO.blocking {
    val ds = org.postgresql.ds.PGSimpleDataSource()
    ds.setServerNames(Array(config.host.toString))
    ds.setPortNumbers(Array(config.port.value))
    ds.setUser(config.user)
    ds.setPassword(config.pass)
    ds.setDatabaseName(config.database)
    ds
  }
}
