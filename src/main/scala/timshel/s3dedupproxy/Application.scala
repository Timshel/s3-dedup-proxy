package timshel.s3dedupproxy

import cats.effect._
import com.jortage.poolmgr.Poolmgr
import org.flywaydb.core.Flyway;
import skunk._
import skunk.implicits._
import skunk.codec.all._
import natchez.Trace.Implicits.noop

case class Application(
    config: GlobalConfig,
    database: Database,
    flyway: Flyway
) {

  def migrate() = IO {
    this.flyway.migrate()
  }

  def start(): IO[ExitCode] = {
    migrate().map { mr =>
      if (mr.success) {
        com.jortage.poolmgr.Poolmgr.start(config, database);
        ExitCode.Success
      } else ExitCode(2)
    }
  }
}

object Application extends IOApp {

  /**  */
  def run(args: List[String]): IO[ExitCode] = {
    default().use { app =>
      app.start()
    }
  }

  def default(): Resource[IO, Application] = {
    Resource
      .make(IO {
        pureconfig.ConfigSource.default.load[GlobalConfig] match {
          case Left(e)       => throw new RuntimeException(e.prettyPrint());
          case Right(config) => config
        }
      })(cs => IO {})
      .flatMap(cs => using(cs))
  }

  def using(config: GlobalConfig): Resource[IO, Application] = {
    val ds = org.postgresql.ds.PGSimpleDataSource()
    ds.setServerNames(Array(config.db.host))
    ds.setPortNumbers(Array(config.db.port))
    ds.setUser(config.db.user)
    ds.setPassword(config.db.pass)
    ds.setDatabaseName(config.db.database)

    val dbSession = Session.pooled[IO](
      host = config.db.host,
      port = config.db.port,
      user = config.db.user,
      database = config.db.database,
      password = Some(config.db.pass),
      max = 10,
    ).flatMap(s => s)

    dbSession.map { session =>
      val database = Database(session)(runtime)
      val flyway   = Flyway.configure().dataSource(ds).load()
      Application(config, database, flyway)
    }
  }

}
