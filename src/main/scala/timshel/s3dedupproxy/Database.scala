package timshel.s3dedupproxy

import cats.effect._
import cats.effect.unsafe.IORuntime
import com.google.common.hash.HashCode;
import java.time.OffsetDateTime
import java.util.UUID
import skunk._
import skunk.data.Completion
import skunk.implicits._
import skunk.codec.all._

case class Metadata(
    size: Long,
    eTag: String,
    contentType: String
)

case class Mapping(
    uuid: UUID,
    bucket: String,
    key: String,
    hash: HashCode,
    md5: HashCode,
    size: Long,
    eTag: String,
    contentType: String,
    created: OffsetDateTime,
    updated: OffsetDateTime
)

object Database {
  val hashD: Decoder[HashCode] = bytea.map(HashCode.fromBytes(_))
  val hashE: Encoder[HashCode] = bytea.contramap(_.asBytes())
  val PAGE_SIZE                = 100

  // ── Static query/command definitions ──

  val mappingHashQ: Query[String *: String *: String *: EmptyTuple, HashCode] =
    sql"""
      SELECT hash FROM file_mappings
        WHERE user_name = $text
          AND bucket = $text
          AND file_key = $text
    """.query(hashD)

  val putMappingC: Command[(String, String, String, HashCode, HashCode)] =
    sql"""
      INSERT INTO file_mappings (user_name, bucket, file_key, hash) VALUES ($text, $text, $text, $hashE)
        ON CONFLICT (user_name, bucket, file_key) DO UPDATE SET hash = $hashE, updated = now();
    """.command

  val delMappingsBucketC: Command[(String, String)] =
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND bucket = $text
    """.command

  val delMappingsPrefixC: Command[(String, String, String)] =
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND bucket = $text AND starts_with(file_key, $text)
    """.command

  val countMappingsQ: Query[HashCode, Long] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE hash = $hashE
    """.query(int8)

  val countMappingsBucketQ: Query[(String, String), Long] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE user_name = $text AND bucket = $text
    """.query(int8)

  val putMetadataC: Command[(HashCode, HashCode, Long, String, String, String, String)] =
    sql"""
      INSERT INTO file_metadata (hash, md5, size, etag, content_type) VALUES ($hashE, $hashE, $int8, $text, $text)
        ON CONFLICT (hash) DO UPDATE SET etag= $text, content_type = $text, updated = now();
    """.command

  val getMetadataQ: Query[HashCode, Metadata] =
    sql"""
      SELECT size, etag, content_type FROM file_metadata WHERE hash = $hashE
    """
      .query(int8 ~ text ~ text)
      .map { case s ~ e ~ ct => Metadata(s, e, ct) }

  val delMetadataC: Command[HashCode] =
    sql"""
      DELETE FROM file_metadata WHERE hash = $hashE
    """.command

  val getDanglingQ: Query[Int, HashCode] =
    sql"""
      SELECT file_metadata.hash
        FROM file_metadata
          LEFT JOIN file_mappings ON file_mappings.hash = file_metadata.hash
        WHERE file_mappings.uuid IS NULL
          AND file_metadata.created < NOW() - INTERVAL '1 hour'
        ORDER BY file_metadata.created ASC
        LIMIT $int4
    """
      .query(hashD)

  val getContainersQ: Query[String, String] =
    sql"""
      SELECT distinct file_mappings.bucket
        FROM file_mappings
        WHERE user_name = $text
        ORDER BY bucket ASC
    """
      .query(text)

  val getMappingsQ: Query[(String, String, String, String, Int), Mapping] =
    sql"""
      SELECT
          file_mappings.uuid, file_mappings.bucket, file_mappings.file_key,
          file_metadata.hash, file_metadata.md5, file_metadata.size, file_metadata.etag, file_metadata.content_type,
          file_mappings.created, file_mappings.updated
        FROM file_mappings
          INNER JOIN file_metadata ON file_metadata.hash = file_mappings.hash
        WHERE user_name = $text
          AND file_mappings.bucket = $text
          AND starts_with(file_mappings.file_key, $text)
          AND file_mappings.file_key > $text
        ORDER BY file_mappings.file_key ASC
        LIMIT $int4
    """
      .query(uuid ~ text ~ text ~ hashD ~ hashD ~ int8 ~ text ~ text ~ timestamptz ~ timestamptz)
      .map { case uu ~ b ~ k ~ h ~ m ~ s ~ e ~ ct ~ c ~ u => Mapping(uu, b, k, h, m, s, e, ct, c, u) }

  // ── Dynamic query builders (list-size dependent, cannot be pre-prepared) ──

  def delMappingsC(count: Int): Command[List[(String, String, String)]] = {
    val enc = (text *: text *: text).values.list(count)
    sql"""
      DELETE FROM file_mappings WHERE (user_name, bucket, file_key) = ANY(Array[$enc])
    """.command
  }

  def delMappingKeysC(count: Int): Command[(String, String, List[String])] = {
    sql"""
      DELETE FROM file_mappings
        WHERE user_name = $text
          AND bucket = $text
          AND file_key IN (${text.list(count)})
    """.command
  }

  def delMetadatasC(count: Int): Command[(List[HashCode])] = {
    sql"""
      DELETE FROM file_metadata WHERE hash IN (${hashE.list(count)})
    """.command
  }
}

/** Holds pre-prepared statements for a single session.
  * Created once per session acquisition, reused for all queries on that session.
  */
case class PreparedDatabase(
    session: Session[IO],
    getMappingHashPS: PreparedQuery[IO, String *: String *: String *: EmptyTuple, HashCode],
    putMappingPS: PreparedCommand[IO, (String, String, String, HashCode, HashCode)],
    delMappingsBucketPS: PreparedCommand[IO, (String, String)],
    delMappingsPrefixPS: PreparedCommand[IO, (String, String, String)],
    countMappingsPS: PreparedQuery[IO, HashCode, Long],
    countMappingsBucketPS: PreparedQuery[IO, (String, String), Long],
    putMetadataPS: PreparedCommand[IO, (HashCode, HashCode, Long, String, String, String, String)],
    getMetadataPS: PreparedQuery[IO, HashCode, Metadata],
    delMetadataPS: PreparedCommand[IO, HashCode],
    getDanglingPS: PreparedQuery[IO, Int, HashCode],
    getContainersPS: PreparedQuery[IO, String, String],
    getMappingsPS: PreparedQuery[IO, (String, String, String, String, Int), Mapping]
)

object PreparedDatabase {
  import Database.*

  def fromSession(session: Session[IO]): IO[PreparedDatabase] = for {
    getMappingHashPS    <- session.prepare(mappingHashQ)
    putMappingPS        <- session.prepare(putMappingC)
    delMappingsBucketPS <- session.prepare(delMappingsBucketC)
    delMappingsPrefixPS <- session.prepare(delMappingsPrefixC)
    countMappingsPS     <- session.prepare(countMappingsQ)
    countMappingsBucketPS <- session.prepare(countMappingsBucketQ)
    putMetadataPS       <- session.prepare(putMetadataC)
    getMetadataPS       <- session.prepare(getMetadataQ)
    delMetadataPS       <- session.prepare(delMetadataC)
    getDanglingPS       <- session.prepare(getDanglingQ)
    getContainersPS     <- session.prepare(getContainersQ)
    getMappingsPS       <- session.prepare(getMappingsQ)
  } yield PreparedDatabase(
    session, getMappingHashPS, putMappingPS, delMappingsBucketPS, delMappingsPrefixPS,
    countMappingsPS, countMappingsBucketPS, putMetadataPS, getMetadataPS, delMetadataPS,
    getDanglingPS, getContainersPS, getMappingsPS
  )
}

case class Database(
    pool: Resource[IO, Session[IO]]
)(implicit runtime: IORuntime) {
  import Database.*

  /** Pool that pre-prepares all static statements on session acquisition. */
  private val preparedPool: Resource[IO, PreparedDatabase] =
    pool.evalMap(PreparedDatabase.fromSession)

  def getMappingHash(user_name: String, bucket: String, file_key: String): IO[Option[HashCode]] =
    preparedPool.use { p => p.getMappingHashPS.option(user_name, bucket, file_key) }

  def putMapping(user_name: String, bucket: String, file_key: String, hash: HashCode): IO[Completion] =
    preparedPool.use { p => p.putMappingPS.execute(user_name, bucket, file_key, hash, hash) }

  def delMappings(mappings: List[(String, String, String)]): IO[Int] = {
    if (mappings.nonEmpty) {
      pool.use {
        _.prepare(delMappingsC(mappings.size))
          .flatMap { pc => pc.execute(mappings) }
          .map {
            case Completion.Delete(count) => count
            case _                        => throw new AssertionError("delMappings execution should only return Delete")
          }
      }
    } else IO.pure(0)
  }

  def delMappingKeys(user_name: String, bucket: String, keys: List[String]): IO[Int] = {
    if (keys.nonEmpty) {
      pool.use {
        _.prepare(delMappingKeysC(keys.size))
          .flatMap { pc => pc.execute(user_name, bucket, keys) }
          .map {
            case Completion.Delete(count) => count
            case _                        => throw new AssertionError("delMappingKeys execution should only return Delete")
          }
      }
    } else IO.pure(0)
  }

  def delMapping(user_name: String, bucket: String, file_key: String): IO[Int] =
    delMappingKeys(user_name, bucket, List(file_key))

  def delMappings(user_name: String, bucket: String): IO[Int] =
    preparedPool.use { p =>
      p.delMappingsBucketPS.execute(user_name, bucket).map {
        case Completion.Delete(count) => count
        case _                        => throw new AssertionError("delMappings execution should only return Delete")
      }
    }

  def delMappings(user_name: String, bucket: String, prefix: String): IO[Int] =
    preparedPool.use { p =>
      p.delMappingsPrefixPS.execute(user_name, bucket, prefix).map {
        case Completion.Delete(count) => count
        case _                        => throw new AssertionError("delMappings execution should only return Delete")
      }
    }

  def countMappings(hash: HashCode): IO[Long] =
    preparedPool.use { p => p.countMappingsPS.unique(hash) }

  def countMappings(user_name: String, bucket: String): IO[Long] =
    preparedPool.use { p => p.countMappingsBucketPS.unique(user_name, bucket) }

  def putMetadata(hash: HashCode, md5: HashCode, size: Long, eTag: String, contentType: String): IO[Completion] =
    preparedPool.use { p => p.putMetadataPS.execute(hash, md5, size, eTag, contentType, eTag, contentType) }

  def getMetadata(hashCode: HashCode): IO[Option[Metadata]] =
    preparedPool.use { p => p.getMetadataPS.option(hashCode) }

  def delMetadata(hash: HashCode): IO[Int] =
    preparedPool.use { p =>
      p.delMetadataPS.execute(hash).map {
        case Completion.Delete(count) => count
        case _                        => throw new AssertionError("delMetadata execution should only return Delete")
      }
    }

  def delMetadatas(hashes: List[HashCode]): IO[Int] = {
    if (hashes.nonEmpty) {
      pool.use {
        _.prepare(delMetadatasC(hashes.size))
          .flatMap { pc => pc.execute(hashes) }
          .map {
            case Completion.Delete(count) => count
            case _                        => throw new AssertionError("delMetadatas execution should only return Delete")
          }
      }
    } else IO.pure(0)
  }

  def getDangling(limit: Int): IO[List[HashCode]] =
    preparedPool.use { p => p.getDanglingPS.stream(limit, limit).compile.toList }

  def withMaker(maxResults: Int)(mappings: List[Mapping]): (List[Mapping], Option[String]) = {
    if (mappings.size == maxResults) {
      (mappings, mappings.lastOption.map(_.key))
    } else (mappings, None)
  }

  def getContainers(user_name: String): IO[List[String]] =
    preparedPool.use { p => p.getContainersPS.stream(user_name, PAGE_SIZE).compile.toList }

  def getMappings(
      user_name: String,
      bucket: String,
      prefix: Option[String] = None,
      marker: Option[String] = None,
      maxResults: Option[Int] = None
  ): IO[(List[Mapping], Option[String])] = {
    val after = marker.getOrElse("")
    val pre   = prefix.getOrElse("")
    val limit = maxResults.getOrElse(PAGE_SIZE)

    preparedPool
      .use { p => p.getMappingsPS.stream((user_name, bucket, pre, after, limit), limit).compile.toList }
      .map(withMaker(limit))
  }
}
