package timshel.s3dedupproxy

import cats.effect._
import cats.effect.std.Dispatcher
import com.google.common.collect.{ImmutableList, Lists, Maps};
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.ForwardingBlobStore;
import org.jclouds.ContextBuilder;
import org.jclouds.domain.internal.LocationImpl;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationScope;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.FilePayload;
import scala.util.Using
import scala.collection.concurrent.TrieMap

import timshel.s3dedupproxy.Database;

object ProxyBlobStore {
  val log = com.typesafe.scalalogging.Logger(classOf[Application])

  @scala.annotation.nowarn("cat=deprecation")
  val MD5 = Hashing.md5()

  def hashToKey(hc: HashCode): String = {
    val hash = hc.toString();
    "blobs/" + hash.substring(0, 1) + "/" + hash.substring(1, 4) + "/" + hash;
  }

  def bufferStorePath(identity: String): File = {
    new File(s"/tmp/s3dedupproxy-buffer/$identity/")
  }

  private def createBufferStore(identity: String): BlobStore = {
    val blobPath = bufferStorePath(identity)

    val overrides = new java.util.Properties()
    overrides.setProperty(org.jclouds.filesystem.reference.FilesystemConstants.PROPERTY_BASEDIR, blobPath.getPath())

    org.jclouds.ContextBuilder
      .newBuilder("filesystem-nio2")
      .credentials("identity", "credential")
      .modules(ImmutableList.of(new org.jclouds.logging.slf4j.config.SLF4JLoggingModule()))
      .overrides(overrides)
      .build(classOf[org.jclouds.blobstore.BlobStoreContext])
      .getBlobStore()
  }

  private def createBlobStore(conf: BackendConfig): BlobStore = {
    val overrides = new java.util.Properties()
    overrides.setProperty(org.jclouds.s3.reference.S3Constants.PROPERTY_S3_VIRTUAL_HOST_BUCKETS, conf.virtualHost.toString());

    val blobStore = ContextBuilder
      .newBuilder(if ("s3".equals(conf.protocol)) "aws-s3" else conf.protocol)
      .credentials(conf.accessKeyId, conf.secretAccessKey)
      .modules(ImmutableList.of(new org.jclouds.logging.slf4j.config.SLF4JLoggingModule()))
      .endpoint(conf.endpoint)
      .overrides(overrides)
      .build(classOf[org.jclouds.blobstore.BlobStoreContext])
      .getBlobStore()

    if (!blobStore.containerExists(conf.bucket)) {
      blobStore.createContainerInLocation(null, conf.bucket)
    }

    blobStore
  }

  /** S3Proxy will throw if it sees an X-Amz header it doesn't recognize
    */
  def createProxy(
      config: GlobalConfig,
      users: UserRegistry,
      db: Database,
      dispatcher: Dispatcher[IO]
  ): Resource[IO, S3Proxy] = {
    val s3Proxy = S3Proxy
      .builder()
      .awsAuthentication(org.gaul.s3proxy.AuthenticationType.AWS_V2_OR_V4, "DUMMY", "DUMMY")
      .endpoint(config.proxy.uri)
      .jettyMaxThreads(24)
      .v4MaxNonChunkedRequestSize(128L * 1024L * 1024L)
      .ignoreUnknownHeaders(true)
      .build()

    val blobStore  = createBlobStore(config.backend)
    val proxyCache = TrieMap.empty[String, ProxyBlobStore]

    s3Proxy.setBlobStoreLocator((identity, container, blob) => {
      users.get(identity) match {
        case Some(secret) =>
          val proxyBlobStore = proxyCache.getOrElseUpdate(
            identity, {
              log.debug(s"Creating store path for $identity")
              bufferStorePath(identity).mkdirs()

              log.debug(s"Creating proxyBlobStore for ${identity}")
              ProxyBlobStore(createBufferStore(identity), blobStore, identity, config.backend.bucket, db, dispatcher)
            }
          )
          Maps.immutableEntry(secret, proxyBlobStore);
        case None => {
          proxyCache.remove(identity)
          null
        }
      }
    });

    Resource.make {
      IO.blocking {
        s3Proxy.start()
        log.info(s"Object proxy running on ${config.proxy.uri}")
        s3Proxy
      }
    }(proxy =>
      IO.blocking {
        log.info("Objcet proxy is stopping")
        proxy.stop()
      }
    )
  }

  object Delimiter {
    def apply(lco: ListContainerOptions): Option[Database.Delimiter] =
      Option(lco.getDelimiter()).filter(_.nonEmpty).map(Database.Delimiter.apply)
  }
  object Marker {
    def apply(lco: ListContainerOptions): Option[Database.Marker] = Option(lco.getMarker()).map(Database.Marker.apply)
  }
  object Prefix {
    def apply(lco: ListContainerOptions): Option[Database.Prefix] = Option(lco.getPrefix()).map(Database.Prefix.apply)
  }
}

class ProxyBlobStore(
    bufferStore: BlobStore,
    blobStore: BlobStore,
    identity: String,
    bucket: String,
    db: Database,
    dispatcher: Dispatcher[IO]
) extends ForwardingBlobStore(blobStore) {
  import ProxyBlobStore.*

  def getMapKey(container: String, name: String): IO[Option[String]] = {
    db.getMappingHash(identity, container, name).map { hco =>
      hco.map(ProxyBlobStore.hashToKey)
    }
  }

  def getMappingKey(container: String, name: String): IO[Option[(Mapping, String)]] = {
    db.getMapping(identity, container, name).map { m =>
      m.map { m => m -> ProxyBlobStore.hashToKey(m.hash) }
    }
  }

  override def getContext(): BlobStoreContext = {
    delegate().getContext();
  }

  override def blobBuilder(name: String): BlobBuilder = {
    delegate().blobBuilder(name);
  }

  override def getBlob(container: String, name: String): Blob = {
    log.debug(s"getBlob($container, $name)")
    val p = getMappingKey(container, name).flatMap {
      case Some((m, key)) =>
        IO.blocking {
          val blob = delegate().getBlob(bucket, key)
          Option(blob).foreach { b => setMetadata(m, b.getMetadata()) }
          blob
        }
      case None => IO.pure(null)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def getBlob(container: String, name: String, getOptions: GetOptions): Blob = {
    log.debug(s"getBlob($container, $name, $getOptions)")
    val p = getMappingKey(container, name).flatMap {
      case Some((m, key)) =>
        IO.blocking {
          val blob = delegate().getBlob(bucket, key, getOptions)
          Option(blob).foreach { b => setMetadata(m, b.getMetadata()) }
          blob
        }
      case None => IO.pure(null)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def downloadBlob(container: String, name: String, destination: File): Unit = {
    log.debug(s"downloadBlob($container, $name, $destination)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO.blocking(delegate().downloadBlob(bucket, key, destination))
      case None      => IO.pure[Unit](())
    }
    dispatcher.unsafeRunSync(p)
  }

  override def downloadBlob(container: String, name: String, destination: File, executor: ExecutorService): Unit = {
    log.debug(s"downloadBlob($container, $name, $destination, ES)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO.blocking(delegate().downloadBlob(bucket, key, destination, executor))
      case None      => IO.pure[Unit](())
    }
    dispatcher.unsafeRunSync(p)
  }

  override def streamBlob(container: String, name: String): InputStream = {
    log.debug(s"streamBlob($container, $name)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO.blocking(delegate().streamBlob(bucket, key))
      case None      => IO.pure(null)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def streamBlob(container: String, name: String, executor: ExecutorService): InputStream = {
    log.debug(s"streamBlob($container, $name, ES)")
    val p = getMapKey(container, name).flatMap {
      case Some(key) => IO.blocking(delegate().streamBlob(bucket, key, executor))
      case None      => IO.pure(null)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def getBlobAccess(container: String, name: String): BlobAccess = {
    log.debug(s"getBlobAccess($container, $name)")
    BlobAccess.PUBLIC_READ;
  }

  override def getContainerAccess(container: String): ContainerAccess = {
    log.debug(s"getContainerAccess($container)")
    ContainerAccess.PUBLIC_READ;
  }

  override def blobExists(container: String, name: String): Boolean = {
    log.debug(s"blobExists($container, $name)")
    val p = getMapKey(container, name).map { k => k.isDefined }
    dispatcher.unsafeRunSync(p)
  }

  override def blobMetadata(container: String, name: String): BlobMetadata = {
    log.debug(s"blobMetadata($container, $name)")
    val p = db.getMapping(identity, container, name).map {
      case Some(m) => mapMetadata(m)
      case None    => null
    }
    dispatcher.unsafeRunSync(p)
  }

  override def directoryExists(container: String, directory: String): Boolean = {
    log.debug(s"directoryExists($container, $directory)")
    true
  }

  override def getMaximumNumberOfParts(): Int = {
    log.debug(s"getMaximumNumberOfParts()")
    delegate().getMaximumNumberOfParts();
  }

  override def getMinimumMultipartPartSize(): Long = {
    log.debug(s"getMinimumMultipartPartSize()")
    delegate().getMinimumMultipartPartSize();
  }

  override def getMaximumMultipartPartSize(): Long = {
    log.debug(s"getMaximumMultipartPartSize()")
    delegate().getMaximumMultipartPartSize();
  }

  private def ensureContainerExists(container: String): IO[Unit] = IO.blocking {
    bufferStore.createContainerInLocation(null, container)
  }.void

  override def putBlob(container: String, blob: Blob): String = {
    log.debug(s"putBlob($container, $blob)")
    val name       = blob.getMetadata().getName()
    val contenType = Option(blob.getMetadata().getContentMetadata().getContentType()).getOrElse("application/octet-stream")

    val p = (for {
      _ <- ensureContainerExists(container)
      (size, hash, md5) <- IO.blocking {
        Using(blob.getPayload().openStream()) { is =>
          val counter = new com.google.common.io.CountingInputStream(is);
          val his     = new com.google.common.hash.HashingInputStream(Hashing.sha512(), counter)
          val md5     = new com.google.common.hash.HashingInputStream(ProxyBlobStore.MD5, his)
          blob.setPayload(md5)
          blob.getMetadata().getContentMetadata().setContentType(contenType)
          bufferStore.putBlob(container, blob)
          (counter.getCount(), his.hash(), md5.hash())
        }.get
      }
      eTag <- processBufferDedup(container, name, hash, md5, size, contenType)
    } yield eTag)
      .onError { e =>
        IO.blocking {
          log.error(s"Failed to putBlob($container, $blob): $e")
          bufferStore.removeBlob(container, name)
        }
      }

    dispatcher.unsafeRunSync(p)
  }

  def processBufferDedup(
      container: String,
      name: String,
      hash: HashCode,
      md5: HashCode,
      size: Long,
      contenType: String
  ): IO[String] = {
    db.withAdvisoryLock(hash) { s =>
      db.checkMetadata(hash)(s).flatMap {
        case Some(metadata) =>
          for {
            _ <- db.putMapping(identity, container, name, hash)(s)
          } yield metadata.eTag
        case None =>
          for {
            eTag <- IO.blocking {
              val blob     = bufferStore.getBlob(container, name)
              val metadata = blob.getMetadata()
              metadata.setContainer(bucket)
              metadata.setName(ProxyBlobStore.hashToKey(hash))
              metadata.getContentMetadata().setContentType(contenType)
              delegate().putBlob(
                bucket,
                blob,
                new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ).multipart(size > 5 * 1024 * 1024)
              );
            }
            _ <- db.putMetadata(hash, md5, size, eTag, contenType)(s)
            _ <- db.putMapping(identity, container, name, hash)(s)
          } yield eTag
      }
    }.flatMap { eTag =>
      for {
        _ <- IO.blocking(bufferStore.removeBlob(container, name))
      } yield eTag
    }
  }

  /** javadoc says options are ignored, so we ignore them too
    */
  override def copyBlob(
      fromContainer: String,
      fromName: String,
      toContainer: String,
      toName: String,
      options: CopyOptions
  ): String = {
    val p = for {
      mapping <- db.getMapping(identity, fromContainer, fromName).map {
        case Some(m) => m
        case None    => throw new IllegalArgumentException("Not found")
      }
      _ <- db.putMapping(identity, toContainer, toName, mapping.hash)
    } yield mapping.eTag

    dispatcher.unsafeRunSync(p)
  }

  override def initiateMultipartUpload(container: String, blobMetadata: BlobMetadata, options: PutOptions): MultipartUpload = {
    log.debug(s"initiateMultipartUpload($container, $blobMetadata, $options)")

    val p = for {
      _ <- ensureContainerExists(container)
      mu <- IO.blocking(
        bufferStore.initiateMultipartUpload(container, blobMetadata, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ))
      )
    } yield mu

    dispatcher.unsafeRunSync(p)
  }

  override def abortMultipartUpload(mpu: MultipartUpload): Unit = {
    log.debug(s"abortMultipartUpload($mpu)")
    bufferStore.abortMultipartUpload(mpu);
  }

  def bufferStoreBlobHash(container: String, name: String): IO[(Long, HashCode, HashCode)] = IO.blocking {
    Using(bufferStore.getBlob(container, name).getPayload().openStream()) { stream =>
      val counter = new com.google.common.io.CountingOutputStream(java.io.OutputStream.nullOutputStream());
      val hos     = new com.google.common.hash.HashingOutputStream(Hashing.sha512(), counter);
      val md5     = new com.google.common.hash.HashingOutputStream(ProxyBlobStore.MD5, hos);
      stream.transferTo(md5)
      (counter.getCount(), hos.hash(), md5.hash())
    }.get
  }

  // Not the most efficient since we read the file to compute the size and hash
  override def completeMultipartUpload(mpu: MultipartUpload, parts: java.util.List[MultipartPart]): String = {
    log.debug(s"completeMultipartUpload($mpu, $parts)")

    val container  = mpu.containerName()
    val name       = mpu.blobName()
    val contenType = Option(mpu.blobMetadata().getContentMetadata().getContentType()).getOrElse("application/octet-stream")

    val p = (for {
      completed <- IO.blocking(bufferStore.completeMultipartUpload(mpu, parts))
      _ = log.debug(s"Completed upload to bufferStore: $completed")
      (size, hash, md5) <- bufferStoreBlobHash(container, name)
      eTag              <- processBufferDedup(container, name, hash, md5, size, contenType)
    } yield eTag)
      .onError { e =>
        IO.blocking {
          log.error(s"Failed to completeMultipartUpload(${mpu.id()}): $e")
          bufferStore.removeBlob(container, name)
        }
      }

    dispatcher.unsafeRunSync(p)
  }

  override def uploadMultipartPart(mpu: MultipartUpload, partNumber: Int, payload: Payload): MultipartPart = {
    log.debug(s"uploadMultipartPart($mpu, $partNumber, $payload)")
    bufferStore.uploadMultipartPart(mpu, partNumber, payload)
  }

  override def listMultipartUpload(mpu: MultipartUpload): java.util.List[MultipartPart] = {
    log.debug(s"listMultipartUpload($mpu)")
    bufferStore.listMultipartUpload(mpu)
  }

  override def listMultipartUploads(container: String): java.util.List[MultipartUpload] = {
    log.debug(s"listMultipartUploads($container)")
    bufferStore.listMultipartUploads(container)
  }

  override def putBlob(container: String, blob: Blob, putOptions: PutOptions): String = {
    putBlob(container, blob);
  }

  // TODO cleanup will be handled separatly
  override def removeBlob(container: String, name: String): Unit = {
    log.debug(s"removeBlob($container, $name)")
    val p = db.delMapping(identity, container, name)
    dispatcher.unsafeRunSync(p)
  }

  override def removeBlobs(container: String, iterable: java.lang.Iterable[String]): Unit = {
    log.debug(s"removeBlobs($container, $iterable)")
    import scala.jdk.CollectionConverters._
    val p = db.delMappingKeys(identity, container, iterable.asScala.toList)
    dispatcher.unsafeRunSync(p)
  }

  override def listAssignableLocations(): java.util.Set[Location] = {
    log.debug(s"listAssignableLocations()")
    java.util.Collections.emptySet[Location]()
  }

  override def createContainerInLocation(location: Location, container: String): Boolean = {
    log.debug(s"createContainerInLocation($location, $container)")
    true
  }

  override def createContainerInLocation(
      location: Location,
      container: String,
      createContainerOptions: CreateContainerOptions
  ): Boolean = {
    log.debug(s"createContainerInLocation($location, $container, $createContainerOptions)")
    true
  }

  override def containerExists(container: String): Boolean = {
    log.debug(s"containerExists($container)")
    true
  }

  override def setContainerAccess(container: String, containerAccess: ContainerAccess): Unit = {
    log.debug(s"setContainerAccess($container, $containerAccess)")
  }

  override def setBlobAccess(container: String, name: String, access: BlobAccess): Unit = {
    log.debug(s"setBlobAccess($container, $name, $access)")
  }

  override def clearContainer(container: String): Unit = {
    log.debug(s"clearContainer($container)")
    val p = db.delMappings(identity, container)
    dispatcher.unsafeRunSync(p)
  }

  override def clearContainer(container: String, options: ListContainerOptions): Unit = {
    log.debug(s"clearContainer($container, $options)")
    val p = Option(options.getPrefix()) match {
      case Some(prefix) => db.delMappings(identity, container, prefix)
      case None         => db.delMappings(identity, container)
    }
    dispatcher.unsafeRunSync(p)
  }

  override def deleteContainer(container: String): Unit = {
    log.debug(s"deleteContainer($container)")
    val p = db.delMappings(identity, container)
    dispatcher.unsafeRunSync(p)
  }

  override def deleteContainerIfEmpty(container: String): Boolean = {
    log.debug(s"deleteContainerIfEmpty($container)")
    val p = db.countMappings(identity, container).map { c => c == 0 }
    dispatcher.unsafeRunSync(p)
  }

  override def list(): PageSet[StorageMetadata] = {
    log.debug(s"list()")
    val p = db.getContainers(identity).map { containers =>
      import scala.jdk.CollectionConverters._

      val iter: java.lang.Iterable[StorageMetadata] = containers.map { c =>
        val sm = org.jclouds.blobstore.domain.internal.MutableStorageMetadataImpl()

        sm.setName(c)
        sm.setSize(0L);

        sm
      }.asJava

      new org.jclouds.blobstore.domain.internal.PageSetImpl[StorageMetadata](iter, null)
    }
    dispatcher.unsafeRunSync(p)
  }

  def setMetadata(mapping: Mapping, bm: MutableBlobMetadata) = {
    bm.setId(mapping.uuid.toString)
    bm.setContainer(mapping.bucket)
    bm.setName(mapping.key)
    bm.setETag(mapping.eTag)
    bm.setSize(mapping.size)
    bm.setType(org.jclouds.blobstore.domain.StorageType.BLOB)
    bm.setCreationDate(java.util.Date.from(mapping.created.toInstant()))
    bm.setLastModified(java.util.Date.from(mapping.updated.toInstant()))

    val mcm = bm.getContentMetadata()
    mcm.setContentType(mapping.contentType)
    mcm.setContentMD5(mapping.md5)
    mcm.setContentLength(mapping.size)
  }

  def mapMetadata(mapping: Mapping): BlobMetadata = {
    val bm = new org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl()
    bm.setContentMetadata(new org.jclouds.io.payloads.BaseMutableContentMetadata())
    setMetadata(mapping, bm)
    bm
  }

  def mapDelimited: PartialFunction[(String, Option[Mapping], String), BlobMetadata] = {
    case (_, Some(mapping), _) => mapMetadata(mapping)
    case (delimited, None, _) =>
      val bm = new org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl()
      bm.setContentMetadata(new org.jclouds.io.payloads.BaseMutableContentMetadata())
      bm.setType(org.jclouds.blobstore.domain.StorageType.RELATIVE_PATH)
      bm.setName(delimited)
      bm
  }

  def mapMetadatas[T](mapMetadata: T => BlobMetadata): PartialFunction[(List[T], Option[String]), PageSet[BlobMetadata]] = {
    case (mappings, marker) =>
      import scala.jdk.CollectionConverters._
      val iter: java.lang.Iterable[BlobMetadata] = mappings.map(mapMetadata).asJava
      new org.jclouds.blobstore.domain.internal.PageSetImpl[BlobMetadata](iter, marker.map(_.toString).getOrElse(null))
  }

  override def list(container: String): PageSet[BlobMetadata] = {
    log.debug(s"list($container)")
    val p = db.getMappings(identity, container).map(mapMetadatas(mapMetadata))
    dispatcher.unsafeRunSync(p)
  }

  override def list(container: String, options: ListContainerOptions): PageSet[BlobMetadata] = {
    log.debug(s"list($container, $options)")

    val p = Delimiter(options) match {
      case None =>
        db
          .getMappings(identity, container, Prefix(options), Marker(options), Option(options.getMaxResults))
          .map(mapMetadatas(mapMetadata))

      case Some(deli) =>
        db
          .getDelimitedMappings(identity, container, deli, Prefix(options), Marker(options), Option(options.getMaxResults))
          .map(mapMetadatas(mapDelimited))
    }

    dispatcher.unsafeRunSync(p)
  }

  override def createDirectory(container: String, directory: String) = {
    log.debug(s"createDirectory($container, $directory)")
  }

  override def deleteDirectory(container: String, directory: String) = {
    log.debug(s"deleteDirectory($container, $directory)")
  }

}
