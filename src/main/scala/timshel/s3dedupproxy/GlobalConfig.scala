package timshel.s3dedupproxy

import com.comcast.ip4s.{Host, Port}
import java.net.URI;
import pureconfig.*
import pureconfig.generic.semiauto.deriveReader

given hostReader: ConfigReader[Host] = ConfigReader.fromStringOpt(Host.fromString)
given portReader: ConfigReader[Port] = implicitly[ConfigReader[Int]].emap { p =>
  Port.fromInt(p).toRight(pureconfig.error.CannotConvert(p.toString, "Port", "Impossible"))
}

case class API(
    host: Host,
    port: Port
) derives ConfigReader

case class Proxy(
    host: Host,
    port: Port
) derives ConfigReader {
  val uri = URI.create(s"http://${host}:${port}")
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
    publicHost: String
) derives ConfigReader

case class GlobalConfig(
    api: API,
    proxy: Proxy,
    backend: BackendConfig,
    backupBackend: Option[BackendConfig] = None,
    db: DBConfig,
    users: Map[String, String]
) derives ConfigReader
