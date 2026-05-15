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
import scala.reflect.TypeTest

final case class Metadata(
    size: Long,
    eTag: String,
    contentType: String
)

final case class Mapping(
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
  val log = com.typesafe.scalalogging.Logger(classOf[Database])

  opaque type Delimiter = String
  opaque type Marker    = String
  opaque type Prefix    = String

  object Delimiter:
    def apply(s: String): Delimiter = s

  object Marker:
    def apply(s: String): Marker = s
    def empty: Marker            = ""

  object Prefix:
    def apply(s: String): Prefix = s
    def empty: Prefix            = ""
    def len(p: Prefix)           = p.length()

  val delim: Encoder[Delimiter] = text
  val marker: Encoder[Marker]   = text
  val prefix: Encoder[Prefix]   = text

  val void = new Decoder[Void] {
    def types                                         = List(skunk.data.Type.void)
    def decode(offset: Int, ss: List[Option[String]]) = Right(Void)
  }

  val hashD: Decoder[HashCode] = bytea.map(HashCode.fromBytes(_))
  val hashE: Encoder[HashCode] = bytea.contramap(_.asBytes())
  val mappingD: Decoder[Mapping] = (uuid ~ text ~ text ~ hashD ~ hashD ~ int8 ~ text ~ text ~ timestamptz ~ timestamptz).map {
    case uu ~ b ~ k ~ h ~ m ~ s ~ e ~ ct ~ c ~ u => Mapping(uu, b, k, h, m, s, e, ct, c, u)
  }
  val metadataD: Decoder[Metadata] = (int8 ~ text ~ text).map { case s ~ e ~ ct => Metadata(s, e, ct) }
  val PAGE_SIZE                    = 100

  def lockKey(hash: HashCode): Long = {
    val bytes = hash.asBytes()
    java.nio.ByteBuffer.wrap(bytes, bytes.length - 8, 8).getLong()
  }
}

case class Database(
    pool: Resource[IO, Session[IO]]
)(runtime: IORuntime) {
  import Database.*

  val lockQ: Query[Long, Void] = sql"SELECT pg_advisory_lock($int8)".query(void)

  val unlockQ: Query[Long, Boolean] = sql"SELECT  pg_advisory_unlock($int8)".query(bool)

  /** Acquires a session scoped advisory lock keyed on the hash.
    * Uses the last 8 bytes of the hash as the lock key to match the PostgreSQL hash_key() function.
    */
  def withAdvisoryLock[A](hash: HashCode)(body: Session[IO] => IO[A]): IO[A] = {
    val lockKey = Database.lockKey(hash)
    pool.use { s =>
      s.prepare(lockQ)
        .flatMap(_.unique(lockKey))
        .flatMap { _ => body(s) }
        .guarantee {
          s.prepare(unlockQ).flatMap(_.unique(lockKey)).map(_ => ())
        }
    }
  }

  val mappingQ: Query[String *: String *: String *: EmptyTuple, Mapping] =
    sql"""
      SELECT
          file_mappings.uuid, file_mappings.bucket, file_mappings.file_key,
          file_metadata.hash, file_metadata.md5, file_metadata.size, file_metadata.etag, file_metadata.content_type,
          file_mappings.created, file_mappings.updated
        FROM file_mappings
          INNER JOIN file_metadata ON file_metadata.hash = file_mappings.hash
        WHERE user_name = $text
          AND bucket = $text
          AND file_key = $text
    """.query(mappingD)

  def getMapping(user_name: String, bucket: String, file_key: String): IO[Option[Mapping]] =
    pool.use {
      _.prepare(mappingQ)
        .flatMap { ps =>
          ps.option(user_name, bucket, file_key)
        }
    }

  val mappingHashQ: Query[String *: String *: String *: EmptyTuple, HashCode] =
    sql"""
      SELECT hash FROM file_mappings
        WHERE user_name = $text
          AND bucket = $text
          AND file_key = $text
    """.query(hashD)

  def getMappingHash(user_name: String, bucket: String, file_key: String): IO[Option[HashCode]] =
    pool.use {
      _.prepare(mappingHashQ)
        .flatMap { ps =>
          ps.option(user_name, bucket, file_key)
        }
    }

  val putMappingC: Command[(String, String, String, HashCode, HashCode)] =
    sql"""
      INSERT INTO file_mappings (user_name, bucket, file_key, hash) VALUES ($text, $text, $text, $hashE)
        ON CONFLICT (user_name, bucket, file_key) DO UPDATE SET hash = $hashE, updated = now();
    """.command

  def putMapping(user_name: String, bucket: String, file_key: String, hash: HashCode): IO[Completion] = {
    pool.use {
      _.prepare(putMappingC)
        .flatMap { pc =>
          pc.execute(user_name, bucket, file_key, hash, hash)
        }
    }
  }

  def delMappingsC(count: Int): Command[List[(String, String, String)]] = {
    val enc = (text *: text *: text).values.list(count)
    sql"""
      DELETE FROM file_mappings WHERE (user_name, bucket, file_key) = ANY(Array[$enc])
    """.command
  }

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

  def delMappingKeysC(count: Int): Command[(String, String, List[String])] = {
    sql"""
      DELETE FROM file_mappings
        WHERE user_name = $text
          AND bucket = $text
          AND file_key IN (${text.list(count)})
    """.command
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

  val delMappingsBucketC: Command[(String, String)] = {
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND bucket = $text
    """.command
  }

  def delMappings(user_name: String, bucket: String): IO[Int] = {
    pool.use {
      _.prepare(delMappingsBucketC)
        .flatMap { pc => pc.execute(user_name, bucket) }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMappings execution should only return Delete")
        }
    }
  }

  val delMappingsPrefixC: Command[(String, String, String)] = {
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND bucket = $text AND starts_with(file_key, $text)
    """.command
  }

  def delMappings(user_name: String, bucket: String, prefix: String): IO[Int] = {
    pool.use {
      _.prepare(delMappingsPrefixC)
        .flatMap { pc => pc.execute(user_name, bucket, prefix) }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMappings execution should only return Delete")
        }
    }
  }

  val countMappingsQ: Query[HashCode, Long] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE hash = $hashE
    """.query(int8)

  def countMappings(hash: HashCode): IO[Long] =
    pool.use {
      _.prepare(countMappingsQ)
        .flatMap { ps =>
          ps.unique(hash)
        }
    }

  val countMappingsBucketQ: Query[(String, String), Long] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE user_name = $text AND bucket = $text
    """.query(int8)

  def countMappings(user_name: String, bucket: String): IO[Long] =
    pool.use {
      _.prepare(countMappingsBucketQ)
        .flatMap { ps => ps.unique(user_name, bucket) }
    }

  val putMetadataC: Command[(HashCode, HashCode, Long, String, String, String, String)] =
    sql"""
      INSERT INTO file_metadata (hash, md5, size, etag, content_type) VALUES ($hashE, $hashE, $int8, $text, $text)
        ON CONFLICT (hash) DO UPDATE SET etag= $text, content_type = $text, updated = now(), checked = now();
    """.command

  def putMetadata(hash: HashCode, md5: HashCode, size: Long, eTag: String, contentType: String)(
      session: Session[IO]
  ): IO[Completion] = {
    session
      .prepare(putMetadataC)
      .flatMap { pc =>
        pc.execute(hash, md5, size, eTag, contentType, eTag, contentType)
      }
  }

  val checkMetadataQ: Query[HashCode, Metadata] =
    sql"""
      UPDATE file_metadata
        SET checked = now()
        WHERE hash = $hashE
        RETURNING size, etag, content_type
    """.query(metadataD)

  def checkMetadata(hashCode: HashCode)(session: Session[IO]): IO[Option[Metadata]] = {
    session.prepare(checkMetadataQ).flatMap { ps => ps.option(hashCode) }
  }

  def delDanglingMetadatasQ(count: Int): Query[List[HashCode], HashCode] = {
    sql"""
      DELETE FROM file_metadata
      WHERE file_metadata.hash IN (${hashE.list(count)})
        AND NOT EXISTS (
          SELECT 1 FROM file_mappings WHERE file_mappings.hash = file_metadata.hash
        )
      RETURNING hash
    """.query(hashD)
  }

  def delDanglingMetadatas(hashes: List[HashCode])(session: Session[IO]): IO[List[HashCode]] = {
    if (hashes.nonEmpty) {
      session
        .prepare(delDanglingMetadatasQ(hashes.size))
        .flatMap { pq => pq.stream(hashes, 1024).compile.toList }
    } else IO.pure(List.empty)
  }

  val getDanglingQ: Query[Int, (HashCode, Void)] =
    sql"""
      WITH dangling as (
        SELECT file_metadata.hash
          FROM file_metadata
            LEFT JOIN file_mappings ON file_mappings.hash = file_metadata.hash
          WHERE file_mappings.uuid IS NULL
            AND file_metadata.checked < NOW() - INTERVAL '1 hour'
          ORDER BY file_metadata.checked ASC
          LIMIT $int4
      )
      SELECT hash, pg_advisory_lock(hash_key(hash)) from dangling
    """.query(hashD ~ void)

  def getDangling(limit: Int)(session: Session[IO]): IO[List[HashCode]] =
    session
      .prepare(getDanglingQ)
      .flatMap { pc => pc.stream(limit, limit).map(_._1).compile.toList }

  val releaseLockQ: Query[Void, Void] = sql"SELECT pg_advisory_unlock_all()".query(void)

  def releaseLocks(s: Session[IO]): IO[Unit] = {
    s.prepare(releaseLockQ).flatMap(_.option(skunk.Void)).map(_ => ())
  }

  def locksReleaseBlock[A](body: Session[IO] => IO[A]): IO[A] = {
    pool.use { s =>
      body(s).guarantee {
        releaseLocks(s).map(_ => ())
      }
    }
  }

  def withMaker[T](maxResults: Int)(last: T => String)(mappings: List[T]): (List[T], Option[String]) = {
    if (mappings.size == maxResults) {
      (mappings, mappings.lastOption.map(last))
    } else (mappings, None)
  }

  val getContainersQ: Query[String, String] =
    sql"""
      SELECT distinct file_mappings.bucket
        FROM file_mappings
        WHERE user_name = $text
        ORDER BY bucket ASC
    """.query(text)

  def getContainers(user_name: String): IO[List[String]] = {
    pool
      .use {
        _.prepare(getContainersQ)
          .flatMap { pc => pc.stream(user_name, PAGE_SIZE).compile.toList }
      }
  }

  val getMappingsQ: Query[(String, String, Prefix, Marker, Int), Mapping] =
    sql"""
      SELECT
          file_mappings.uuid, file_mappings.bucket, file_mappings.file_key,
          file_metadata.hash, file_metadata.md5, file_metadata.size, file_metadata.etag, file_metadata.content_type,
          file_mappings.created, file_mappings.updated
        FROM file_mappings
          INNER JOIN file_metadata ON file_metadata.hash = file_mappings.hash
        WHERE user_name = $text
          AND file_mappings.bucket = $text
          AND starts_with(file_mappings.file_key, $prefix)
          AND file_mappings.file_key > $marker
        ORDER BY file_mappings.file_key ASC
        LIMIT $int4
    """.query(mappingD)

  def getMappings(
      user_name: String,
      bucket: String,
      prefix: Option[Prefix] = None,
      marker: Option[Marker] = None,
      maxResults: Option[Int] = None
  ): IO[(List[Mapping], Option[String])] = {
    val after = marker.getOrElse(Marker.empty)
    val pre   = prefix.getOrElse(Prefix.empty)
    val limit = maxResults.getOrElse(PAGE_SIZE)

    pool
      .use {
        _.prepare(getMappingsQ)
          .flatMap { pc => pc.stream((user_name, bucket, pre, after, limit), limit).compile.toList }
      }
      .map(withMaker(limit)(_.key))
  }

  val getDelimitedQ: Query[(Prefix, Int, Delimiter, String, String, Prefix, Marker, Int), ((String, Option[Mapping]), String)] =
    sql"""
    WITH delimited as (
      SELECT
          $prefix || split_part(substring(file_mappings.file_key from  $int4), $delim, 1) as delimited,
          file_mappings.file_key, file_mappings.uuid , file_mappings.bucket ,
          file_metadata.hash, file_metadata.md5, file_metadata.size, file_metadata.etag, file_metadata.content_type,
          file_mappings.created, file_mappings.updated
        FROM file_mappings
          INNER JOIN file_metadata ON file_metadata.hash = file_mappings.hash
        WHERE user_name = $text
          AND file_mappings.bucket = $text
          AND starts_with(file_mappings.file_key, $prefix)
          AND file_mappings.file_key > $marker
      )

      SELECT delimited,
          CASE WHEN delimited=file_key THEN uuid ELSE null END AS uuid,
          max(CASE WHEN delimited=file_key THEN bucket ELSE null END) AS bucket,
          max(CASE WHEN delimited=file_key THEN file_key ELSE null END) AS file_key,
          CASE WHEN delimited=file_key THEN hash ELSE null END AS hash,
          CASE WHEN delimited=file_key THEN md5 ELSE null END AS md5,
          max(CASE WHEN delimited=file_key THEN size ELSE null END) AS size,
          max(CASE WHEN delimited=file_key THEN etag ELSE null END) AS etag,
          max(CASE WHEN delimited=file_key THEN content_type ELSE null END) AS content_type,
          max(CASE WHEN delimited=file_key THEN created ELSE null END) AS created,
          max(CASE WHEN delimited=file_key THEN updated ELSE null END) AS updated,
          max(file_key) AS full_key
        FROM delimited
        GROUP BY
          delimited,
          CASE WHEN delimited=file_key THEN uuid ELSE null END,
          CASE WHEN delimited=file_key THEN hash ELSE null END,
          CASE WHEN delimited=file_key THEN md5 ELSE null END
        ORDER BY file_key ASC
        LIMIT $int4
    """.query(text ~ mappingD.opt ~ text)

  def getDelimitedMappings(
      user_name: String,
      bucket: String,
      delimiter: Delimiter,
      prefix: Option[Prefix] = None,
      marker: Option[Marker] = None,
      maxResults: Option[Int] = None
  ): IO[(List[(String, Option[Mapping], String)], Option[String])] = {
    val after = marker.getOrElse(Marker.empty)
    val pre   = prefix.getOrElse(Prefix.empty)
    val limit = maxResults.getOrElse(PAGE_SIZE)

    pool
      .use {
        _.prepare(getDelimitedQ).flatMap { pc =>
          pc.stream((pre, Prefix.len(pre) + 1, delimiter, user_name, bucket, pre, after, limit), limit)
            .map {
              case ((del, None), key)    => (del + delimiter, None, key)
              case ((del, mapping), key) => (del, mapping, key)
            }
            .compile
            .toList
        }
      }
      .map(withMaker(limit)(_._3))
  }

}
