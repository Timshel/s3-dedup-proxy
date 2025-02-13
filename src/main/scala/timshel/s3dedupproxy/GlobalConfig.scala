package timshel.s3dedupproxy

import pureconfig.*
import pureconfig.generic.semiauto.deriveReader

case class DBConfig(
    host: String,
    port: Int,
    user: String,
    pass: String,
    database: String,
    maxPoolSize: Int
) derives ConfigReader

case class BackendConfig(
    protocol: String,
    endpoint: String,
    accessKeyId: String,
    secretAccessKey: String,
    bucket: String,
    publicHost: String
) derives ConfigReader

case class RivetConfig(enabled: Boolean) derives ConfigReader

case class GlobalConfig(
    useNewUrls: Boolean,
    readOnly: Boolean,
    rivet: RivetConfig,
    backend: BackendConfig,
    backupBackend: Option[BackendConfig] = None,
    db: DBConfig,
    users: Map[String, String]
) derives ConfigReader
