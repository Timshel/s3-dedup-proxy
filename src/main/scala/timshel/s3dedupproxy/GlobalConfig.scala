package timshel.s3dedupproxy

import com.comcast.ip4s.{Host, Port}
import java.net.URI;
import org.http4s.Uri;
import org.quartz.CronExpression;
import pureconfig.*
import pureconfig.generic.semiauto.deriveReader
import scala.util.Try;

given hostReader: ConfigReader[Host] = ConfigReader.fromStringOpt(Host.fromString)
given portReader: ConfigReader[Port] = implicitly[ConfigReader[Int]].emap { p =>
  Port.fromInt(p).toRight(pureconfig.error.CannotConvert(p.toString, "Port", "Impossible"))
}
given cronReader: ConfigReader[CronExpression] = ConfigReader.fromStringTry { str => Try(org.quartz.CronExpression(str)) }
given uriReader: ConfigReader[Uri] = implicitly[ConfigReader[String]].emap { s =>
  Uri.fromString(s).left.map { _ => pureconfig.error.CannotConvert(s, "Uri", "Impossible") }
}

case class API(
    host: Host,
    port: Port
) derives ConfigReader

case class Proxy(
    host: Host,
    port: Port,
    purge: CronExpression
) derives ConfigReader {
  val uri = new URI("http", null, host.toString, port.value, null, null, null)
}

case class DBConfig(
    host: Host,
    port: Port,
    user: String,
    pass: String,
    database: String
) derives ConfigReader

case class BackendConfig(
    protocol: String,
    endpoint: String,
    virtualHost: Boolean,
    accessKeyId: String,
    secretAccessKey: String,
    bucket: String,
    publicHost: Uri
) derives ConfigReader

case class GlobalConfig(
    api: API,
    proxy: Proxy,
    backend: BackendConfig,
    backupBackend: Option[BackendConfig] = None,
    db: DBConfig,
    users: Map[String, String] = Map.empty,
    usersFile: Option[String] = None
) derives ConfigReader
