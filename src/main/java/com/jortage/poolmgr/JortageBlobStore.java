package com.jortage.poolmgr;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.sql.DataSource;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.util.ForwardingBlobStore;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationScope;
import org.jclouds.domain.internal.LocationImpl;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.FilePayload;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingOutputStream;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;

import timshel.s3dedupproxy.Database;

public class JortageBlobStore extends ForwardingBlobStore {
	private final String identity;
	private final String bucket;
	private final Database db;

	public JortageBlobStore(BlobStore blobStore, String bucket, String identity, Database db) {
		super(blobStore);
		this.bucket = bucket;
		this.identity = identity;
		this.db = db;
	}

	public static String hashToPath(HashCode hc) {
		String hash = hc.toString();
		return "blobs/"+hash.substring(0, 1)+"/"+hash.substring(1, 4)+"/"+hash;
	}

	private String getMapPath(String container, String name) {
		return hashToPath(db.getMappingHashU(identity, container, name));
	}

	@Override
	public BlobStoreContext getContext() {
		return delegate().getContext();
	}

	@Override
	public BlobBuilder blobBuilder(String name) {
		return delegate().blobBuilder(name);
	}

	@Override
	public Blob getBlob(String container, String name) {
		return delegate().getBlob(bucket, getMapPath(container, name));
	}

	@Override
	public Blob getBlob(String container, String name, GetOptions getOptions) {
		return delegate().getBlob(bucket, getMapPath(container, name), getOptions);
	}

	@Override
	public void downloadBlob(String container, String name, File destination) {
		delegate().downloadBlob(bucket, getMapPath(container, name), destination);
	}

	@Override
	public void downloadBlob(String container, String name, File destination, ExecutorService executor) {
		delegate().downloadBlob(bucket, getMapPath(container, name), destination, executor);
	}

	@Override
	public InputStream streamBlob(String container, String name) {
		return delegate().streamBlob(bucket, getMapPath(container, name));
	}

	@Override
	public InputStream streamBlob(String container, String name, ExecutorService executor) {
		return delegate().streamBlob(bucket, getMapPath(container, name), executor);
	}

	@Override
	public BlobAccess getBlobAccess(String container, String name) {
		return BlobAccess.PUBLIC_READ;
	}

	@Override
	public ContainerAccess getContainerAccess(String container) {
		return ContainerAccess.PUBLIC_READ;
	}

	@Override
	public boolean blobExists(String container, String name) {
		return delegate().blobExists(bucket, getMapPath(container, name));
	}

	@Override
	public BlobMetadata blobMetadata(String container, String name) {
		return delegate().blobMetadata(bucket, getMapPath(container, name));
	}

	@Override
	public boolean directoryExists(String container, String directory) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getMaximumNumberOfParts() {
		return delegate().getMaximumNumberOfParts();
	}

	@Override
	public long getMinimumMultipartPartSize() {
		return delegate().getMinimumMultipartPartSize();
	}

	@Override
	public long getMaximumMultipartPartSize() {
		return delegate().getMaximumMultipartPartSize();
	}

	@Override
	public String putBlob(String container, Blob blob) {
		String blobName = blob.getMetadata().getName();
		File tempFile = null;

		try {
			File f = File.createTempFile("jortage-proxy-", ".dat");
			tempFile = f;
			String contentType = blob.getPayload().getContentMetadata().getContentType();
			HashCode hash;
			try (InputStream is = blob.getPayload().openStream();
				OutputStream fos = Files.newOutputStream(Paths.get(f.toURI()))) {
				HashingOutputStream hos = new HashingOutputStream(Hashing.sha512(), fos);
				FileReprocessor.reprocess(is, hos);
				hash = hos.hash();
			}

			try (Payload payload = new FilePayload(f)) {
				String hash_path = hashToPath(hash);

				payload.getContentMetadata().setContentType(contentType);
				BlobMetadata meta = delegate().blobMetadata(bucket, hash_path);
				if (meta != null) {
					String etag = meta.getETag();
					db.putMappingU(identity, container, blobName, hash);
					return etag;
				}
				Blob blob2 = blobBuilder(hash_path)
						.payload(payload)
						.userMetadata(blob.getMetadata().getUserMetadata())
						.build();
				String etag = delegate().putBlob(bucket, blob2, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ).multipart());
				db.putPendingU(hash);
				db.putMappingU(identity, container, blobName, hash);
				db.putMetadataU(hash, f.length());
				return etag;
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new UncheckedIOException(e);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (tempFile != null) tempFile.delete();
		}
	}

	@Override
	public String copyBlob(String fromContainer, String fromName, String toContainer, String toName, CopyOptions options) {
		// javadoc says options are ignored, so we ignore them too
		HashCode hash = db.getMappingHashU(identity, fromContainer, fromName);
		db.putMappingU(identity, toContainer, toName, hash);
		return blobMetadata(bucket, hashToPath(hash)).getETag();
	}

	@Override
	public MultipartUpload initiateMultipartUpload(String container, BlobMetadata blobMetadata, PutOptions options) {
		MutableBlobMetadata mbm = new MutableBlobMetadataImpl(blobMetadata);
		String tempfile = "multitmp/"+identity+"-"+System.currentTimeMillis()+"-"+System.nanoTime();
		mbm.setName(tempfile);
		mbm.getUserMetadata().put("jortage-creator", identity);
		mbm.getUserMetadata().put("jortage-originalname", blobMetadata.getName());
		db.putMultipartU(identity, container, blobMetadata.getName(), tempfile);
		return delegate().initiateMultipartUpload(bucket, mbm, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
	}

	private MultipartUpload mask(MultipartUpload mpu) {
		return MultipartUpload.create(bucket, db.getMultipartFileU(identity, mpu.containerName(), mpu.blobName()), mpu.id(), mpu.blobMetadata(), new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
	}

	private MultipartUpload revmask(MultipartUpload mpu) {
		scala.Tuple2<String, String> t = db.getMultipartKeyU(mpu.blobName());
		return MultipartUpload.create(t._1(), t._2(), mpu.id(), mpu.blobMetadata(), new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
	}

	@Override
	public void abortMultipartUpload(MultipartUpload mpu) {
		delegate().abortMultipartUpload(mask(mpu));
	}

	@Override
	public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
		try {
			mpu = mask(mpu);
			// TODO this is a bit of a hack and isn't very efficient
			String etag = delegate().completeMultipartUpload(mpu, parts);
			try (InputStream stream = delegate().getBlob(mpu.containerName(), mpu.blobName()).getPayload().openStream()) {
				CountingOutputStream counter = new CountingOutputStream(ByteStreams.nullOutputStream());
				HashingOutputStream hos = new HashingOutputStream(Hashing.sha512(), counter);
				FileReprocessor.reprocess(stream, hos);
				HashCode hash = hos.hash();
				String path = hashToPath(hash);
				// we're about to do a bunch of stuff at once
				// sleep so we don't fall afoul of request rate limits
				// (causes intermittent 429s on at least DigitalOcean)
				Thread.sleep(100);
				BlobMetadata meta = delegate().blobMetadata(mpu.containerName(), mpu.blobName());
				BlobMetadata targetMeta = delegate().blobMetadata(bucket, path);
				if (targetMeta == null) {
					Thread.sleep(100);
					etag = delegate().copyBlob(mpu.containerName(), mpu.blobName(), bucket, path, CopyOptions.builder().contentMetadata(meta.getContentMetadata()).build());
					Thread.sleep(100);
					try {
						delegate().setBlobAccess(bucket, path, BlobAccess.PUBLIC_READ);
					} catch (UnsupportedOperationException ignore) {}
					db.putPendingU(hash);
				} else {
					Thread.sleep(100);
					etag = targetMeta.getETag();
				}
				db.putMappingU(identity, mpu.containerName(), Preconditions.checkNotNull(meta.getUserMetadata().get("jortage-originalname")), hash);
				db.putMetadataU(hash, counter.getCount());
				db.delMultipartU(mpu.blobName());
				Thread.sleep(100);
				delegate().removeBlob(mpu.containerName(), mpu.blobName());
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return etag;
		} catch (Error | RuntimeException e) {
			e.printStackTrace();
			throw e;
		}
	}

	@Override
	public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
		return delegate().uploadMultipartPart(mask(mpu), partNumber, payload);
	}

	@Override
	public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
		return delegate().listMultipartUpload(mask(mpu));
	}

	@Override
	public List<MultipartUpload> listMultipartUploads(String container) {
		List<MultipartUpload> out = Lists.newArrayList();
		for (MultipartUpload mpu : delegate().listMultipartUploads(bucket)) {
			if (Objects.equal(mpu.blobMetadata().getUserMetadata().get("jortage-creator"), identity)) {
				MultipartUpload revMpu = revmask(mpu);
				if ( container == revMpu.containerName() ) {
					out.add(revmask(revMpu));
				}
			}
		}
		return out;
	}

	@Override
	public String putBlob(String containerName, Blob blob, PutOptions putOptions) {
		return putBlob(containerName, blob);
	}

	@Override
	public void removeBlob(String container, String name) {
		HashCode hc = db.getMappingHashU(identity, container, name);
		if( db.delMappingU(identity, container, name) ){
			long rc = db.countMappingsU(hc);
			if (rc == 0L) {
				String path = hashToPath(hc);
				delegate().removeBlob(bucket, path);
				db.delMetadataU(hc);
				db.delPendingU(hc);
			}
		}
	}

	@Override
	public void removeBlobs(String container, Iterable<String> iterable) {
		for (String s : iterable) {
			removeBlob(container, s);
		}
	}

	@Override
	public Set<? extends Location> listAssignableLocations() {
		return Collections.singleton(new LocationImpl(LocationScope.PROVIDER, "jort", "jort", null, Collections.emptySet(), Collections.emptyMap()));
	}

	@Override
	public boolean createContainerInLocation(Location location, String container) {
		return true;
	}

	@Override
	public boolean createContainerInLocation(Location location,
			String container, CreateContainerOptions createContainerOptions) {
		return true;
	}

	@Override
	public boolean containerExists(String container) {
		return identity.equals(container);
	}

	private static final String NO_DIR_MSG = "Directories are an illusion";
	private static final String NO_BULK_MSG = "Bulk operations are not implemented by Jortage for safety and speed";
	
	@Override
	public void setContainerAccess(String container, ContainerAccess containerAccess) {
	}
	
	@Override
	public void setBlobAccess(String container, String name, BlobAccess access) {
	}

	@Override
	public void clearContainer(String container) {
		throw new UnsupportedOperationException(NO_BULK_MSG);
	}

	@Override
	public void clearContainer(String container, ListContainerOptions options) {
		throw new UnsupportedOperationException(NO_BULK_MSG);
	}

	@Override
	public void deleteContainer(String container) {
		throw new UnsupportedOperationException(NO_BULK_MSG);
	}

	@Override
	public boolean deleteContainerIfEmpty(String container) {
		throw new UnsupportedOperationException(NO_BULK_MSG);
	}

	@Override
	public PageSet<? extends StorageMetadata> list() {
		throw new UnsupportedOperationException(NO_BULK_MSG);
	}

	@Override
	public PageSet<? extends StorageMetadata> list(String container) {
		throw new UnsupportedOperationException(NO_BULK_MSG);
	}

	@Override
	public PageSet<? extends StorageMetadata> list(String container, ListContainerOptions options) {
		throw new UnsupportedOperationException(NO_BULK_MSG);
	}
	
	@Override
	public void createDirectory(String container, String directory) {
		throw new UnsupportedOperationException(NO_DIR_MSG);
	}

	@Override
	public void deleteDirectory(String container, String directory) {
		throw new UnsupportedOperationException(NO_DIR_MSG);
	}

}
