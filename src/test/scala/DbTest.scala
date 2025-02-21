import cats.effect._
import timshel.s3dedupproxy.{Application, Metadata}
import munit.CatsEffectSuite

import com.google.common.hash.HashCode;

class PgIntegrationTests extends CatsEffectSuite {

  val app = ResourceSuiteLocalFixture(
    "application",
    Application.default().evalMap { a =>
      a.migrate().map { mr => a }
    }
  )

  override def munitFixtures = List(app)

  def use[T](f: Application => IO[T]): IO[T] = {
    IO(app()).flatMap(f)
  }

  test("Query file_metadata run against db") {
    use { a =>
      val hashCode = HashCode.fromInt(12);

      for {
        _ <- a.database.putMetadata(hashCode, 10L, "A")
        _ <- a.database.putMetadata(hashCode, 12L, "B")
        _ <- assertIO(a.database.getMetadata(hashCode), Some(Metadata(12L, "B")))
        _ <- assertIO(a.database.delMetadata(hashCode), 1)
      } yield ()
    }
  }

  test("Query file_mappings run against db") {
    use { a =>
      val hashCode = HashCode.fromInt(12);

      for {
        _ <- a.database.putMetadata(hashCode, 10L, "A")
        _ <- a.database.putMapping("A", "bucket", "B", hashCode)
        _ <- a.database.putMapping("A", "bucket", "B", hashCode)
        _ <- assertIO(a.database.getMappingHash("A", "bucket", "B"), Some(hashCode))
        _ <- assertIO(a.database.countMappings(hashCode), 1L)
        _ <- assertIO(a.database.delMapping("A", "bucket", "B"), 1)
        _ <- assertIO(a.database.delMetadata(hashCode), 1)
        _ <- assertIO(a.database.countMappings(hashCode), 0L)
      } yield ()
    }
  }
}
