package timshel.s3dedupproxy

import com.comcast.ip4s.{Host, Port}
import java.net.URI;
import java.nio.file.{Files, Path}
import org.http4s.Uri;
import org.quartz.CronExpression;
import pureconfig.*
import scala.util.Try;

given hostReader: ConfigReader[Host] = ConfigReader.fromStringOpt(Host.fromString)
given portReader: ConfigReader[Port] = implicitly[ConfigReader[Int]].emap { p =>
  Port.fromInt(p).toRight(pureconfig.error.CannotConvert(p.toString, "Port", "Impossible"))
}
given cronReader: ConfigReader[CronExpression] = ConfigReader.fromStringTry { str => Try(org.quartz.CronExpression(str)) }
given uriReader: ConfigReader[Uri] = implicitly[ConfigReader[String]].emap { s =>
  Uri.fromString(s).left.map { _ => pureconfig.error.CannotConvert(s, "Uri", "Impossible") }
}

final case class API(
    host: Host,
    port: Port
) derives ConfigReader

final case class Proxy(
    host: Host,
    port: Port,
    purge: CronExpression
) derives ConfigReader {
  val uri = new URI("http", null, host.toString, port.value, null, null, null)
}

final case class DBConfig(
    host: Host,
    port: Port,
    user: String,
    pass: String,
    database: String
) derives ConfigReader

final case class BackendConfig(
    protocol: String,
    endpoint: String,
    virtualHost: Boolean,
    accessKeyId: String,
    secretAccessKey: String,
    bucket: String,
    publicHost: Uri
) derives ConfigReader

final case class GlobalConfig(
    api: API,
    proxy: Proxy,
    backend: BackendConfig,
    backupBackend: Option[BackendConfig],
    db: DBConfig,
    users: Map[String, String],
    usersFile: Option[Path]
) derives ConfigReader
