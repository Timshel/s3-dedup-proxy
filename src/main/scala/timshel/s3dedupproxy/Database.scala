package timshel.s3dedupproxy

import cats.effect._
import cats.effect.unsafe.IORuntime
import skunk._
import skunk.data.Completion
import skunk.implicits._
import skunk.codec.all._

import com.google.common.hash.HashCode;

case class Metadata(
    size: Long,
    eTag: String
)

case class Database(
    pool: Resource[IO, Session[IO]]
)(implicit runtime: IORuntime) {

  val hashD: Decoder[HashCode] = bytea.map(HashCode.fromBytes(_))
  val hashE: Encoder[HashCode] = bytea.contramap(_.asBytes())

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

  val delMappingC: Command[(String, String, String)] =
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND bucket = $text AND file_key = $text
    """.command

  def delMapping(user_name: String, bucket: String, file_key: String): IO[Int] = {
    pool.use {
      _.prepare(delMappingC)
        .flatMap { pc =>
          pc.execute(user_name, bucket, file_key)
        }
        .map {
          case Completion.Delete(count) => count
          case _                        => throw new AssertionError("delMapping execution should only return Delete")
        }
    }
  }

  val countMappingQ: Query[HashCode, Long] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE hash = $hashE
    """.query(int8)

  def countMappings(hash: HashCode): IO[Long] =
    pool.use {
      _.prepare(countMappingQ)
        .flatMap { ps =>
          ps.unique(hash)
        }
    }

  val putMetadataC: Command[(HashCode, Long, String, Long, String)] =
    sql"""
      INSERT INTO file_metadata (hash, size, etag) VALUES ($hashE, $int8, $text)
        ON CONFLICT (hash) DO UPDATE SET size = $int8, etag= $text, updated = now();
    """.command

  def putMetadata(hash: HashCode, size: Long, eTag: String): IO[Completion] = {
    pool.use {
      _.prepare(putMetadataC)
        .flatMap { pc =>
          pc.execute(hash, size, eTag, size, eTag)
        }
    }
  }

  val getMetadataQ: Query[HashCode, Metadata] =
    sql"""
      SELECT size, etag FROM file_metadata WHERE hash = $hashE
    """
      .query(int8 ~ text)
      .map { case s ~ e => Metadata(s, e) }

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
}
