package timshel.s3dedupproxy

import cats.effect.{Resource, IO}
import cats.implicits._
import com.jortage.poolmgr.Poolmgr
import org.flywaydb.core.Flyway
import skunk._
import skunk.implicits._
import skunk.codec.all._
import natchez.Trace.Implicits.noop
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts


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
        com.jortage.poolmgr.Poolmgr.start(config);
        ExitCode.Success
      } else ExitCode(2)
    }
  }
}

object Application extends IOApp {

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
    val hikariConfig = new HikariConfig()
    hikariConfig.setJdbcUrl(s"jdbc:postgresql://${config.db.host}:${config.db.port}/${config.db.database}")
    hikariConfig.setUsername(config.db.user)
    hikariConfig.setPassword(config.db.pass)
    hikariConfig.setMaximumPoolSize(config.db.maxPoolSize) // Make max pool size configurable
    hikariConfig.setDriverClassName("org.postgresql.Driver")

    val transactor = (for {
      ce <- ExecutionContexts.fixedThreadPool[IO](config.db.maxPoolSize) // Connection pool for DB interactions
      xa <- HikariTransactor.fromHikariConfig[IO](hikariConfig, ce)
    } yield xa).handleErrorWith { e =>
      Resource.liftK(IO.raiseError(new RuntimeException("Failed to initialize database transactor", e)))
    }

    transactor.evalMap { xa =>
    val database = Database(sessionPool)(runtime)
    val flyway   = Flyway.configure().dataSource(ds).load()
    Application(config, database, flyway)
    }

}
