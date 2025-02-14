package timshel.s3dedupproxy

import cats.effect._
import cats.effect.unsafe.IORuntime
import skunk._
import skunk.data.Completion
import skunk.implicits._
import skunk.codec.all._

import com.google.common.hash.HashCode;

case class Database(
    session: Session[IO]
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
    session
      .prepare(mappingHashQ)
      .flatMap { ps =>
        ps.option(user_name, bucket, file_key)
      }

  def getMappingHashU(user_name: String, bucket: String, file_key: String): HashCode =
    getMappingHash(user_name, bucket, file_key).unsafeRunSync().getOrElse(throw new IllegalArgumentException("Not found"));

  val putMappingC: Command[(String, String, String, HashCode, HashCode)] =
    sql"""
      INSERT INTO file_mappings (user_name, bucket, file_key, hash) VALUES ($text, $text, $text, $hashE)
        ON CONFLICT (user_name, bucket, file_key) DO UPDATE SET hash = $hashE, updated = now();
    """.command

  def putMapping(user_name: String, bucket: String, file_key: String, hash: HashCode): IO[Completion] = {
    session
      .prepare(putMappingC)
      .flatMap { pc =>
        pc.execute(user_name, bucket, file_key, hash, hash)
      }
  }

  def putMappingU(user_name: String, bucket: String, file_key: String, hash: HashCode): Completion =
    putMapping(user_name, bucket, file_key, hash).unsafeRunSync()

  val delMappingC: Command[(String, String, String)] =
    sql"""
      DELETE FROM file_mappings WHERE user_name = $text AND bucket = $text AND file_key = $text
    """.command

  def delMapping(user_name: String, bucket: String, file_key: String): IO[Int] = {
    session
      .prepare(delMappingC)
      .flatMap { pc =>
        pc.execute(user_name, bucket, file_key)
      }
      .map {
        case Completion.Delete(count) => count
        case _                        => throw new AssertionError("delMapping execution should only return Delete")
      }
  }

  def delMappingU(user_name: String, bucket: String, file_key: String): Boolean =
    delMapping(user_name, bucket, file_key).unsafeRunSync() > 0

  val countMappingQ: Query[HashCode, Long] =
    sql"""
      SELECT COUNT(1) FROM file_mappings WHERE hash = $hashE
    """.query(int8)

  def countMappings(hash: HashCode): IO[Long] =
    session
      .prepare(countMappingQ)
      .flatMap { ps =>
        ps.unique(hash)
      }

  def countMappingsU(hash: HashCode): Long =
    countMappings(hash).unsafeRunSync()

  def isMapped(hash: HashCode): Boolean = countMappingsU(hash) > 0

  val putMetadataC: Command[(HashCode, Long, Long)] =
    sql"""
      INSERT INTO file_metadata (hash, size) VALUES ($hashE, $int8)
        ON CONFLICT (hash) DO UPDATE SET size = $int8, updated = now();
    """.command

  def putMetadata(hash: HashCode, size: Long): IO[Completion] = {
    session
      .prepare(putMetadataC)
      .flatMap { pc =>
        pc.execute(hash, size, size)
      }
  }

  def putMetadataU(hash: HashCode, size: Long): Completion =
    putMetadata(hash, size).unsafeRunSync()

  val delMetadataC: Command[HashCode] =
    sql"""
      DELETE FROM file_metadata WHERE hash = $hashE
    """.command

  def delMetadata(hash: HashCode): IO[Int] = {
    session
      .prepare(delMetadataC)
      .flatMap { pc =>
        pc.execute(hash)
      }
      .map {
        case Completion.Delete(count) => count
        case _                        => throw new AssertionError("delMapping execution should only return Delete")
      }
  }

  def delMetadataU(hash: HashCode): Int =
    delMetadata(hash).unsafeRunSync()

  val putPendingC: Command[HashCode] =
    sql"""
      INSERT INTO pending_backup (hash) VALUES ($hashE)
    """.command

  def putPending(hash: HashCode): IO[Completion] = {
    session
      .prepare(putPendingC)
      .flatMap { pc =>
        pc.execute(hash)
      }
  }

  def putPendingU(hash: HashCode): Completion =
    putPending(hash).unsafeRunSync()

  val delPendingC: Command[HashCode] =
    sql"""
      DELETE FROM pending_backup WHERE hash = $hashE
    """.command

  def delPending(hash: HashCode): IO[Int] = {
    session
      .prepare(delPendingC)
      .flatMap { pc =>
        pc.execute(hash)
      }
      .map {
        case Completion.Delete(count) => count
        case _                        => throw new AssertionError("delMapping execution should only return Delete")
      }
  }

  def delPendingU(hash: HashCode): Int =
    delPending(hash).unsafeRunSync()

  val putMultipartC: Command[(String, String, String, String, String)] =
    sql"""
      INSERT INTO multipart_uploads (user_name, bucket, file_key, tempfile) VALUES ($text, $text, $text, $text)
        ON CONFLICT (user_name, bucket, file_key) DO UPDATE SET tempfile = $text, updated = now();
    """.command

  def putMultipart(user_name: String, bucket: String, file_key: String, temp_file: String): IO[Completion] = {
    session
      .prepare(putMultipartC)
      .flatMap { pc =>
        pc.execute(user_name, bucket, file_key, temp_file, temp_file)
      }
  }

  def putMultipartU(user_name: String, bucket: String, file_key: String, temp_file: String): Completion =
    putMultipart(user_name, bucket, file_key, temp_file).unsafeRunSync()

  val multipartFileQ: Query[String *: String *: String *: EmptyTuple, String] =
    sql"""
      SELECT tempfile FROM multipart_uploads
        WHERE user_name = $text
          AND bucket = $text
          AND file_key = $text
    """.query(text)

  def getMultipartFile(user_name: String, bucket: String, file_key: String): IO[Option[String]] =
    session
      .prepare(multipartFileQ)
      .flatMap { ps =>
        ps.option(user_name, bucket, file_key)
      }

  def getMultipartFileU(user_name: String, bucket: String, file_key: String): String =
    getMultipartFile(user_name, bucket, file_key).unsafeRunSync().getOrElse(throw new IllegalArgumentException("Not found"));

  val multipartKeyQ: Query[String, String ~ String] =
    sql"""
      SELECT bucket, file_key FROM multipart_uploads WHERE tempfile = $text
    """.query(text ~ text)

  def getMultipartKey(tempfile: String): IO[Option[(String, String)]] =
    session
      .prepare(multipartKeyQ)
      .flatMap { ps =>
        ps.option(tempfile)
      }

  def getMultipartKeyU(tempfile: String): (String, String) =
    getMultipartKey(tempfile).unsafeRunSync().getOrElse(throw new IllegalArgumentException("Not found"));

  val delMultipartC: Command[String] =
    sql"""
      DELETE FROM multipart_uploads WHERE tempfile = $text
    """.command

  def delMultipart(tempfile: String): IO[Int] = {
    session
      .prepare(delMultipartC)
      .flatMap { pc =>
        pc.execute(tempfile)
      }
      .map {
        case Completion.Delete(count) => count
        case _                        => throw new AssertionError("delMapping execution should only return Delete")
      }
  }

  def delMultipartU(tempfile: String): Int =
    delMultipart(tempfile).unsafeRunSync()

}
