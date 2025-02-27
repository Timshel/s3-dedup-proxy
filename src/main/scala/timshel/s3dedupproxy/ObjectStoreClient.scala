package timshel.s3dedupproxy

import cats.effect._
import io.minio.{MinioClient, RemoveObjectsArgs};
import io.minio.messages.DeleteObject;
import com.google.common.hash.HashCode;

case class ObjectStoreClient(
    config: BackendConfig
) {
  import ObjectStoreClient._

  val client = MinioClient
    .builder()
    .endpoint(config.endpoint)
    .credentials(config.accessKeyId, config.secretAccessKey)
    .build()

  def deleteKeys(hashes: List[HashCode]): IO[Unit] = IO {
    if (hashes.nonEmpty) {
      val objects = new java.util.LinkedList[DeleteObject]();

      hashes.foreach { h =>
        objects.add(new DeleteObject(ProxyBlobStore.hashToKey(h)))
      }

      client.removeObjects(RemoveObjectsArgs.builder().bucket(config.bucket).objects(objects).build()).forEach { r =>
        val e = r.get()
        log.error(s"Failed to delete ${e.objectName}, err ${e.code}: ${e.message}")
      }
    }
  }
}

object ObjectStoreClient {
  val log = com.typesafe.scalalogging.Logger(classOf[ObjectStoreClient])
}
