package timshel.s3dedupproxy

import cats.effect._
import cats.effect.std.Dispatcher
import com.google.common.collect.{ImmutableList, Maps};
import java.io.File;
import java.net.URI;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.flywaydb.core.Flyway;
import org.gaul.s3proxy.S3Proxy;
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.server.middleware.Logger
import org.jclouds.blobstore.BlobStore;
import org.jclouds.ContextBuilder;
import org.postgresql.ds.PGSimpleDataSource
import skunk._
import skunk.codec.all._
import skunk.implicits._
import timshel.s3dedupproxy.{BackendConfig, Database, GlobalConfig};

case class Application(
    config: GlobalConfig,
    database: Database,
    proxy: S3Proxy
) {
  import Application._

  def start(): IO[Unit] = IO {
    proxy.start()
    Application.log.info("ready on http://localhost:23278")
  }

  def stop(): IO[Unit] = IO {
    log.info("Application is stopping")
    proxy.stop()
  }
}

object Application extends IOApp {
  val log = com.typesafe.scalalogging.Logger(classOf[Application])

  /** */
  def run(args: List[String]): IO[ExitCode] = {
    default().use { app =>
      (for {
        _ <- migration(app.config.db)
        _ <- app.start()
      } yield {
        val httpApp = org.http4s.server.Router(
          "/proxy/" -> RedirectionController(app.config.backend, app.database).routes
        ).orNotFound

        EmberServerBuilder.default[IO]
          .withHost(app.config.api.host)
          .withPort(app.config.api.port)
          .withHttpApp(Logger.httpApp(true, true)(httpApp))
          .build
      }
    ).bracket(server => server.use(_ => IO.never))(_ => app.stop())
    }
  }

  def default(): Resource[IO, Application] = {
    IO {
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
      dispatcher <- Dispatcher.parallel[IO]
    } yield {
      val database = Database(pool)(runtime)
      val proxy    = createProxy(config, database, dispatcher)

      Application(config, database, proxy)
    })
      .evalTap { _ =>
        IO {
          config.users.keys.foreach { identity => bufferStorePath(identity).mkdirs() }
          log.debug(s"Buffer store path created for users (${config.users.keys})")
        }
      }
  }

  def migration(config: DBConfig): IO[Unit] = {
    (for {
      ds  <- simpleDataSource(config)
      fly <- IO(Flyway.configure().dataSource(ds).load())
      mr  <- IO(fly.migrate())
    } yield mr)
      .flatMap {
        case mr if mr.success => IO.pure(())
        case _                => IO.raiseError(new RuntimeException("Migration failure"))
      }
  }

  def simpleDataSource(config: DBConfig): IO[PGSimpleDataSource] = IO {
    val ds = org.postgresql.ds.PGSimpleDataSource()
    ds.setServerNames(Array(config.host.toString))
    ds.setPortNumbers(Array(config.port.value))
    ds.setUser(config.user)
    ds.setPassword(config.pass)
    ds.setDatabaseName(config.database)
    ds
  }

  /** S3Proxy will throw if it sees an X-Amz header it doesn't recognize
    */
  def createProxy(config: GlobalConfig, db: Database, dispatcher: Dispatcher[IO]): S3Proxy = {
    val s3Proxy = S3Proxy
      .builder()
      .awsAuthentication(org.gaul.s3proxy.AuthenticationType.AWS_V2_OR_V4, "DUMMY", "DUMMY")
      .endpoint(URI.create("http://localhost:23278"))
      .jettyMaxThreads(24)
      .v4MaxNonChunkedRequestSize(128L * 1024L * 1024L)
      .ignoreUnknownHeaders(true)
      .build()

    val blobStore = createBlobStore(config.backend)

    s3Proxy.setBlobStoreLocator((identity, container, blob) => {
      val proxyBlobStore =
        ProxyBlobStore(createBufferStore(identity), blobStore, identity, config.backend.bucket, db, dispatcher)

      config.users.get(identity) match {
        case Some(secret) => Maps.immutableEntry(secret, proxyBlobStore);
        case None         => throw new SecurityException("Access denied")
      }
    });

    s3Proxy
  }

  def bufferStorePath(identity: String): File = {
    new File(s"/tmp/s3dedupproxy-buffer/$identity/")
  }

  def createBufferStore(identity: String): BlobStore = {
    val blobPath = bufferStorePath(identity)

    val overrides = new java.util.Properties()
    overrides.setProperty(org.jclouds.filesystem.reference.FilesystemConstants.PROPERTY_BASEDIR, blobPath.getPath())

    org.jclouds.ContextBuilder
      .newBuilder("filesystem-nio2")
      .credentials("identity", "credential")
      .modules(ImmutableList.of(new org.jclouds.logging.slf4j.config.SLF4JLoggingModule()))
      .overrides(overrides)
      .build(classOf[org.jclouds.blobstore.BlobStoreContext])
      .getBlobStore()
  }

  def createBlobStore(conf: BackendConfig): BlobStore = {
    val overrides = new java.util.Properties()
    overrides.setProperty(org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, conf.virtualHost.toString());

    ContextBuilder
      .newBuilder(if ("s3".equals(conf.protocol)) "aws-s3" else conf.protocol)
      .credentials(conf.accessKeyId, conf.secretAccessKey)
      .modules(ImmutableList.of(new org.jclouds.logging.slf4j.config.SLF4JLoggingModule()))
      .endpoint(conf.endpoint)
      .overrides(overrides)
      .build(classOf[org.jclouds.blobstore.BlobStoreContext])
      .getBlobStore()
  }

}
