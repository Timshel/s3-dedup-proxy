import cats.effect._
import timshel.s3dedupproxy.{Application, Metadata}
import munit.CatsEffectSuite

import com.google.common.hash.HashCode;

class PgIntegrationTests extends CatsEffectSuite {

  val app = ResourceSuiteLocalFixture(
    "application",
    Application.default()
  )

  override def munitFixtures = List(app)

  def use[T](f: Application => IO[T]): IO[T] = {
    IO(app()).flatMap(f)
  }

  test("Query file_metadata run against db") {
    use { a =>
      val hashCode = HashCode.fromInt(12);

      for {
        _ <- a.database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _ <- a.database.putMetadata(hashCode, hashCode, 12L, "B", "CT")
        _ <- assertIO(a.database.getMetadata(hashCode), Some(Metadata(10L, "B", "CT")))
        _ <- assertIO(a.database.getDangling(1000), List(hashCode))
        _ <- assertIO(a.database.delMetadata(hashCode), 1)
        _ <- a.database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _ <- assertIO(a.database.delMetadatas(List(hashCode)), 1)
      } yield ()
    }
  }

  test("Query file_mappings run against db") {
    use { a =>
      val hashCode = HashCode.fromInt(12);

      for {
        _ <- a.database.putMetadata(hashCode, hashCode, 10L, "A", "CT")
        _ <- a.database.putMapping("A", "bucket", "B", hashCode)
        _ <- assertIO(a.database.getMappings("A", "bucket").map(_._1.map(_.hash)), List(hashCode))
        _ <- a.database.putMapping("A", "bucket", "B", hashCode)
        _ <- assertIO(a.database.getMappingHash("A", "bucket", "B"), Some(hashCode))
        _ <- assertIO(a.database.countMappings(hashCode), 1L)
        _ <- assertIO(a.database.delMapping("A", "bucket", "B"), 1)
        _ <- assertIO(a.database.countMappings(hashCode), 0L)

        _ <- a.database.putMapping("A", "bucket", "B", hashCode)
        _ <- a.database.putMapping("A", "bucket", "C", hashCode)
        _ <- assertIO(a.database.delMappingKeys("A", "bucket", List("B", "C")), 2)

        _ <- a.database.putMapping("A", "bucket", "B", hashCode)
        _ <- a.database.putMapping("B", "bucket", "B", hashCode)
        _ <- assertIO(a.database.countMappings("A", "bucket"), 1L)
        _ <- assertIO(a.database.delMappings(List(("A", "bucket", "B"), ("B", "bucket", "B"))), 2)

        _ <- a.database.putMapping("A", "bucket", "B", hashCode)
        _ <- a.database.putMapping("A", "bucket", "C", hashCode)
        _ <- assertIO(a.database.delMappings("A", "bucket"), 2)

        _ <- a.database.putMapping("A", "bucket", "prefix1", hashCode)
        _ <- a.database.putMapping("A", "bucket", "prefix2", hashCode)
        _ <- a.database.putMapping("A", "bucket", "wrefix2", hashCode)
        _ <- assertIO(a.database.countMappings("A", "bucket"), 3L)
        _ <- assertIO(a.database.delMappings("A", "bucket", "prefix"), 2)
        _ <- assertIO(a.database.delMappings("A", "bucket", "wrefix"), 1)

        _ <- assertIO(a.database.delMetadata(hashCode), 1)
        _ <- assertIO(a.database.countMappings(hashCode), 0L)
      } yield ()
    }
  }
}
