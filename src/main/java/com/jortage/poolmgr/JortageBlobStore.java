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
	private final BlobStore dumpsStore;
	private final String identity;
	private final String bucket;
	private final Database db;

	public JortageBlobStore(BlobStore blobStore, BlobStore dumpsStore, String bucket, String identity, Database db) {
		super(blobStore);
		this.dumpsStore = dumpsStore;
		this.bucket = bucket;
		this.identity = identity;
		this.db = db;
	}

	private void checkContainer(String container) {
		if (!Objects.equal(container, identity)) {
			throw new IllegalArgumentException("Bucket name must match your access ID");
		}
	}

	private String getMapPath(String container, String name) {
		checkContainer(container);
		return Poolmgr.hashToPath(db.getMappingHashU(container, name));
	}

	private boolean isDump(String name) {
		return name.startsWith("backups/dumps") || name.startsWith("/backups/dumps");
	}

	@Override
	public BlobStoreContext getContext() {
		return delegate().getContext();
	}

	@Override
	public BlobBuilder blobBuilder(String name) {
		if (isDump(name)) return dumpsStore.blobBuilder(name);
		return delegate().blobBuilder(name);
	}

	@Override
	public Blob getBlob(String container, String name) {
		if (isDump(name)) {
			checkContainer(container);
			return dumpsStore.getBlob(container, name);
		}
		return delegate().getBlob(bucket, getMapPath(container, name));
	}

	@Override
	public Blob getBlob(String container, String name, GetOptions getOptions) {
		if (isDump(name)) {
			checkContainer(container);
			return dumpsStore.getBlob(container, name, getOptions);
		}
		return delegate().getBlob(bucket, getMapPath(container, name), getOptions);
	}

	@Override
	public void downloadBlob(String container, String name, File destination) {
		if (isDump(name)) {
			checkContainer(container);
			dumpsStore.downloadBlob(container, name, destination);
			return;
		}
		delegate().downloadBlob(bucket, getMapPath(container, name), destination);
	}

	@Override
	public void downloadBlob(String container, String name, File destination, ExecutorService executor) {
		if (isDump(name)) {
			checkContainer(container);
			dumpsStore.downloadBlob(container, name, destination, executor);
			return;
		}
		delegate().downloadBlob(bucket, getMapPath(container, name), destination, executor);
	}

	@Override
	public InputStream streamBlob(String container, String name) {
		if (isDump(name)) {
			checkContainer(container);
			return dumpsStore.streamBlob(container, name);
		}
		return delegate().streamBlob(bucket, getMapPath(container, name));
	}

	@Override
	public InputStream streamBlob(String container, String name, ExecutorService executor) {
		if (isDump(name)) {
			checkContainer(container);
			return dumpsStore.streamBlob(container, name, executor);
		}
		return delegate().streamBlob(bucket, getMapPath(container, name), executor);
	}

	@Override
	public BlobAccess getBlobAccess(String container, String name) {
		checkContainer(container);
		return BlobAccess.PUBLIC_READ;
	}

	@Override
	public ContainerAccess getContainerAccess(String container) {
		checkContainer(container);
		return ContainerAccess.PUBLIC_READ;
	}

	@Override
	public boolean blobExists(String container, String name) {
		if (isDump(name)) {
			checkContainer(container);
			return dumpsStore.blobExists(container, name);
		}
		return delegate().blobExists(bucket, getMapPath(container, name));
	}

	@Override
	public BlobMetadata blobMetadata(String container, String name) {
		if (isDump(name)) {
			checkContainer(container);
			return dumpsStore.blobMetadata(container, name);
		}
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
		Poolmgr.checkReadOnly();
		checkContainer(container);
		String blobName = blob.getMetadata().getName();
		if (isDump(blobName)) {
			return dumpsStore.putBlob(container, blob, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
		}
		File tempFile = null;
		Object mutex = new Object();
		synchronized (Poolmgr.provisionalMaps) {
			Poolmgr.provisionalMaps.put(identity, blobName, mutex);
		}
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
				String hash_path = Poolmgr.hashToPath(hash);

				payload.getContentMetadata().setContentType(contentType);
				BlobMetadata meta = delegate().blobMetadata(bucket, hash_path);
				if (meta != null) {
					String etag = meta.getETag();
					db.putMappingU(identity, blobName, hash);
					return etag;
				}
				Blob blob2 = blobBuilder(hash_path)
						.payload(payload)
						.userMetadata(blob.getMetadata().getUserMetadata())
						.build();
				String etag = delegate().putBlob(bucket, blob2, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ).multipart());
				db.putPendingU(hash);
				db.putMappingU(identity, blobName, hash);
				db.putMetadataU(hash, f.length());
				return etag;
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} finally {
			if (tempFile != null) tempFile.delete();
			synchronized (Poolmgr.provisionalMaps) {
				Poolmgr.provisionalMaps.remove(identity, blobName);
			}
			synchronized (mutex) {
				mutex.notifyAll();
			}
		}
	}

	@Override
	public String copyBlob(String fromContainer, String fromName, String toContainer, String toName, CopyOptions options) {
		Poolmgr.checkReadOnly();
		checkContainer(fromContainer);
		checkContainer(toContainer);
		if (isDump(fromName)) {
			if (!isDump(toName)) throw new UnsupportedOperationException();
			return dumpsStore.copyBlob(fromContainer, fromName, toContainer, toName, options);
		}
		// javadoc says options are ignored, so we ignore them too
		HashCode hash = db.getMappingHashU(identity, fromName);
		db.putMappingU(identity, toName, hash);
		return blobMetadata(bucket, Poolmgr.hashToPath(hash)).getETag();
	}

	@Override
	public MultipartUpload initiateMultipartUpload(String container, BlobMetadata blobMetadata, PutOptions options) {
		Poolmgr.checkReadOnly();
		checkContainer(container);
		if (isDump(blobMetadata.getName())) {
			return dumpsStore.initiateMultipartUpload(container, blobMetadata, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
		}
		MutableBlobMetadata mbm = new MutableBlobMetadataImpl(blobMetadata);
		String tempfile = "multitmp/"+identity+"-"+System.currentTimeMillis()+"-"+System.nanoTime();
		mbm.setName(tempfile);
		mbm.getUserMetadata().put("jortage-creator", identity);
		mbm.getUserMetadata().put("jortage-originalname", blobMetadata.getName());
		db.putMultipartU(identity, blobMetadata.getName(), tempfile);
		return delegate().initiateMultipartUpload(bucket, mbm, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
	}

	private MultipartUpload mask(MultipartUpload mpu) {
		checkContainer(mpu.containerName());
		return MultipartUpload.create(bucket, db.getMultipartFileU(identity, mpu.blobName()), mpu.id(), mpu.blobMetadata(), new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
	}

	private MultipartUpload revmask(MultipartUpload mpu) {
		checkContainer(mpu.containerName());
		return MultipartUpload.create(bucket, db.getMultipartKeyU(mpu.blobName()), mpu.id(), mpu.blobMetadata(), new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
	}

	@Override
	public void abortMultipartUpload(MultipartUpload mpu) {
		Poolmgr.checkReadOnly();
		if (isDump(mpu.blobName())) {
			checkContainer(mpu.containerName());
			dumpsStore.abortMultipartUpload(mpu);
			return;
		}
		delegate().abortMultipartUpload(mask(mpu));
	}

	@Override
	public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
		try {
			Poolmgr.checkReadOnly();
			if (isDump(mpu.blobName())) {
				checkContainer(mpu.containerName());
				return dumpsStore.completeMultipartUpload(mpu, parts);
			}
			mpu = mask(mpu);
			// TODO this is a bit of a hack and isn't very efficient
			String etag = delegate().completeMultipartUpload(mpu, parts);
			try (InputStream stream = delegate().getBlob(mpu.containerName(), mpu.blobName()).getPayload().openStream()) {
				CountingOutputStream counter = new CountingOutputStream(ByteStreams.nullOutputStream());
				HashingOutputStream hos = new HashingOutputStream(Hashing.sha512(), counter);
				FileReprocessor.reprocess(stream, hos);
				HashCode hash = hos.hash();
				String path = Poolmgr.hashToPath(hash);
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
				db.putMappingU(identity, Preconditions.checkNotNull(meta.getUserMetadata().get("jortage-originalname")), hash);
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
		Poolmgr.checkReadOnly();
		if (isDump(mpu.blobName())) {
			checkContainer(mpu.containerName());
			return dumpsStore.uploadMultipartPart(mpu, partNumber, payload);
		}
		return delegate().uploadMultipartPart(mask(mpu), partNumber, payload);
	}

	@Override
	public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
		if (isDump(mpu.blobName())) {
			checkContainer(mpu.containerName());
			return dumpsStore.listMultipartUpload(mpu);
		}
		return delegate().listMultipartUpload(mask(mpu));
	}

	@Override
	public List<MultipartUpload> listMultipartUploads(String container) {
		checkContainer(container);
		List<MultipartUpload> out = Lists.newArrayList();
		for (MultipartUpload mpu : delegate().listMultipartUploads(bucket)) {
			if (Objects.equal(mpu.blobMetadata().getUserMetadata().get("jortage-creator"), identity)) {
				out.add(revmask(mpu));
			}
		}
		out.addAll(dumpsStore.listMultipartUploads(container));
		return out;
	}

	@Override
	public String putBlob(String containerName, Blob blob, PutOptions putOptions) {
		return putBlob(containerName, blob);
	}

	@Override
	public void removeBlob(String container, String name) {
		Poolmgr.checkReadOnly();
		checkContainer(container);
		if (isDump(name)) {
			dumpsStore.removeBlob(container, name);
			return;
		}
		HashCode hc = db.getMappingHashU(identity, name);
		if( db.delMappingU(identity, name) ){
			long rc = db.countMappingsU(hc);
			if (rc == 0L) {
				String path = Poolmgr.hashToPath(hc);
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
		Poolmgr.checkReadOnly();
		checkContainer(container);
		return true;
	}

	@Override
	public boolean createContainerInLocation(Location location,
			String container, CreateContainerOptions createContainerOptions) {
		Poolmgr.checkReadOnly();
		checkContainer(container);
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
