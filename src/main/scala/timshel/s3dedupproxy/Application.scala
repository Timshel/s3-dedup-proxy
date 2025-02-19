package timshel.s3dedupproxy

import cats.effect._
import cats.effect.std.Dispatcher
import com.google.common.collect.{ImmutableList, Maps};
import com.jortage.poolmgr.JortageBlobStore
import java.net.URI;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.flywaydb.core.Flyway;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.ContextBuilder;
import skunk._
import skunk.codec.all._
import skunk.implicits._
import timshel.s3dedupproxy.{BackendConfig, Database, GlobalConfig};

case class Application(
    config: GlobalConfig,
    database: Database,
    flyway: Flyway,
    proxy: S3Proxy
) {

  def migrate(): IO[org.flywaydb.core.api.output.MigrateResult] = IO {
    val mr = this.flyway.migrate();
    if (!mr.success) throw new RuntimeException("Migration failure")
    mr
  }

  def start(): IO[ExitCode] = {
    for {
      _ <- migrate()
      _ <- startProxy()
      _ <- IO.never
    } yield ExitCode.Success
  }

  def startProxy(): IO[Unit] = IO {
    proxy.start()
    Application.log.info("ready on http://localhost:23278")
  }
}

object Application extends IOApp {
  val log = com.typesafe.scalalogging.Logger(classOf[Application])

  /** */
  def run(args: List[String]): IO[ExitCode] = {
    default().use { app =>
      app.start()
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

    val ds = org.postgresql.ds.PGSimpleDataSource()
    ds.setServerNames(Array(config.db.host))
    ds.setPortNumbers(Array(config.db.port))
    ds.setUser(config.db.user)
    ds.setPassword(config.db.pass)
    ds.setDatabaseName(config.db.database)

    for {
      pool <- Session.pooled[IO](
        host = config.db.host,
        port = config.db.port,
        user = config.db.user,
        database = config.db.database,
        password = Some(config.db.pass),
        max = 10
      )
      dispatcher <- Dispatcher.parallel[IO]
    } yield {
      val database = Database(pool)(runtime)
      val flyway   = Flyway.configure().dataSource(ds).load()
      val proxy    = createProxy(config, database, dispatcher)

      Application(config, database, flyway, proxy)
    }
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
      .build();

    val blobStore = createBlobStore(config.backend);

    s3Proxy.setBlobStoreLocator((identity, container, blob) => {
      val proxyBlobStore = ProxyBlobStore(blobStore, identity, config.backend.bucket, db, dispatcher)
      config.users.get(identity) match {
        case Some(secret) => Maps.immutableEntry(secret, proxyBlobStore);
        case None         => throw new SecurityException("Access denied")
      }
    });

    s3Proxy
  }

  def createBlobStore(conf: BackendConfig): BlobStore = {
    val protocol  = if ("s3".equals(conf.protocol)) "aws-s3" else conf.protocol;
    val overrides = new java.util.Properties();
    overrides.setProperty(org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, conf.virtualHost.toString());

    ContextBuilder
      .newBuilder(protocol)
      .credentials(conf.accessKeyId, conf.secretAccessKey)
      .modules(ImmutableList.of(new org.jclouds.logging.slf4j.config.SLF4JLoggingModule()))
      .endpoint(conf.endpoint)
      .overrides(overrides)
      .build(classOf[org.jclouds.blobstore.BlobStoreContext])
      .getBlobStore();
  }

}
