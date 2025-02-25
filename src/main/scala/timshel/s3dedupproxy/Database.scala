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
  val UUID_ZERO                = new UUID(0L, 0L)
  val PAGE_SIZE                = 100
}

case class Database(
    pool: Resource[IO, Session[IO]]
)(implicit runtime: IORuntime) {
  import Database.*

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
    pool.use {
      _.prepare(delMappingsC(mappings.size))
        .flatMap { pc => pc.execute(mappings) }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMappings execution should only return Delete")
        }
    }
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
    pool.use {
      _.prepare(delMappingKeysC(keys.size))
        .flatMap { pc => pc.execute(user_name, bucket, keys) }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMappingKeys execution should only return Delete")
        }
    }
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
        ON CONFLICT (hash) DO UPDATE SET etag= $text, content_type = $text, updated = now();
    """.command

  def putMetadata(hash: HashCode, md5: HashCode, size: Long, eTag: String, contentType: String): IO[Completion] = {
    pool.use {
      _.prepare(putMetadataC)
        .flatMap { pc =>
          pc.execute(hash, md5, size, eTag, contentType, eTag, contentType)
        }
    }
  }

  val getMetadataQ: Query[HashCode, Metadata] =
    sql"""
      SELECT size, etag, content_type FROM file_metadata WHERE hash = $hashE
    """
      .query(int8 ~ text ~ text)
      .map { case s ~ e ~ ct => Metadata(s, e, ct) }

  def getMetadata(hashCode: HashCode): IO[Option[Metadata]] =
    pool.use {
      _.prepare(getMetadataQ)
        .flatMap { ps => ps.option(hashCode) }
    }

  val delMetadataC: Command[HashCode] =
    sql"""
      DELETE FROM file_metadata WHERE hash = $hashE
    """.command

  def delMetadata(hash: HashCode): IO[Int] = {
    pool.use {
      _.prepare(delMetadataC)
        .flatMap { pc =>
          pc.execute(hash)
        }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMapping execution should only return Delete")
        }
    }
  }

  def withMaker(mappings: List[Mapping]): (List[Mapping], Option[UUID]) = {
    if (mappings.size == PAGE_SIZE) {
      (mappings, mappings.lastOption.map(_.uuid))
    } else (mappings, None)
  }

  val getContainersQ: Query[String, String] =
    sql"""
      SELECT distinct file_mappings.bucket
        FROM file_mappings
        WHERE user_name = $text
        ORDER BY bucket ASC
    """
      .query(text)

  def getContainers(user_name: String): IO[List[String]] = {
    pool
      .use {
        _.prepare(getContainersQ)
          .flatMap { pc => pc.stream(user_name, PAGE_SIZE).compile.toList }
      }
  }

  val getMappingsQ: Query[(String, String, String, UUID, Int), Mapping] =
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
          AND file_mappings.uuid > $uuid
        ORDER BY file_mappings.file_key ASC
        LIMIT $int4
    """
      .query(uuid ~ text ~ text ~ hashD ~ hashD ~ int8 ~ text ~ text ~ timestamptz ~ timestamptz)
      .map { case uu ~ b ~ k ~ h ~ m ~ s ~ e ~ ct ~ c ~ u => Mapping(uu, b, k, h, m, s, e, ct, c, u) }

  def getMappings(
      user_name: String,
      bucket: String,
      prefix: Option[String] = None,
      marker: Option[UUID] = None
  ): IO[(List[Mapping], Option[UUID])] = {
    val after = marker.getOrElse(UUID_ZERO)
    val pre   = prefix.getOrElse("")

    pool
      .use {
        _.prepare(getMappingsQ)
          .flatMap { pc => pc.stream((user_name, bucket, pre, UUID_ZERO, PAGE_SIZE), PAGE_SIZE).compile.toList }
      }
      .map(withMaker)
  }

}
