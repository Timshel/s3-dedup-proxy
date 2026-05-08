
# S3-Dedup-Proxy

This project allows to run a deduplication proxy in front of an S3 compatible object store.
\
Heavily inspired by https://github.com/jortage/poolmgr and use https://github.com/gaul/s3proxy for the proxy part.

When a client send a file, it will be downloaded locally, hashed (sha512) and then stored in the object store only if not already present.
\
The client view/buckets are virtual and stored in Postgres.

## Usage

### Build

You will need a Java JDK installed and [sbt](https://www.scala-sbt.org/download/).

```bash
> sbt stage
...
> ls -l target/universal/stage
drwxr-xr-x 2  4096 Feb 25 18:37 bin
drwxr-xr-x 2 20480 Feb 25 18:37 lib
```

### Running

You first will need to create a config file, you can take inspiration from [application.conf](docker.application.conf).

```bash
> ls -l
-rw-r--r-- 1   641 Feb 25 18:41 application.conf
drwxr-xr-x 2  4096 Feb 25 18:37 bin
drwxr-xr-x 2 20480 Feb 25 18:37 lib
> ./bin/s3-dedup-proxy -Dconfig.file=application.conf
```

Log backend use slf4j simple logger and can be configured using parameters such as `-Dorg.slf4j.simpleLogger.log.timshel.s3dedupproxy=info`.

### Redirection API

In addition to the object store proxy an http server is started (default port `23279`).
It allows to obtain the backing store resource url with an client resource, Ex:

```bash
> curl -v http://127.0.0.1:23279/proxy/tenant1/s3dedup/main/resources/application.conf
...
>
< HTTP/1.1 308 Permanent Redirect
...
<
* Connection #0 to host 127.0.0.1 left intact
http://127.0.0.1/mastodon/blobs/f/b83/fb83d051f2a1da53b43edee3986c21ca7c31f78bb956eb5b6bfad92ea741def9051d09e1d287aca74d22bd03db73fd49b350a4d5e1df8fe494dd01904996ec04
```

### Purge API

When a user delete a file only the database mapping is removed, the file stored in the object store is not removed even if it was the last reference.
Periodically a purge job is run to remove the dangling files (config `proxy.purge` key).

It's possible to trigger the purge by and HTTP call too:

```bash
curl --request DELETE http://127.0.0.1:23279/api/purge
0 deleted
```

It deletes up to 1000 files (the maximum for a single call of `RemoveObjects`).

## Docker demo

The `--build` ensure that the image is up to date.

```bash
> docker compose up --build S3DedupProxy
```

This will run three services:

- `S3DedupProxy`: the proxy itself bind 23278
- `Postgres`: the database to store the file metadata (binded on 3306)
- `S3Proxy`: used as local S3 store (binded on 8080)

You can then interact with the proxy and store using [rclone](https://min.io/docs/minio/linux/reference/minio-mc.html).

With the following `~/.config/rclone/rclone.conf`

```config
[store]
type = s3
provider = Other
endpoint = http://127.0.0.1:80
acl = private

[tenant1]
type = s3
provider = Other
access_key_id = tenant1
secret_access_key = ff54fae017ebe20356223ea016d1e2e867b7f73aaba775fe48ed65d1f1fecb87
endpoint = http://127.0.0.1:23278

[tenant2]
type = s3
provider = Other
access_key_id = tenant2
secret_access_key = 25ca6d3f04906bfc05a3f2c7ce6b5569ec841ee0ef241a886f34687bbafee7cc
endpoint = http://127.0.0.1:23278
```

Example to show deduplication:

```bash
> rclone ls store:mastodon
> rclone copy LICENSE tenant1:bucket1
> rclone ls tenant1:bucket1
    34523 LICENSE
> rclone ls store:mastodon
    34523 blobs/5/c4c/5c4c4e351be428767a0f30cde8bf2687bc31580c408c4b8c6e3a6cf9e4fa0412aabb9037f216e46ad93b85481941dcabe726c1da3714ab5a737be71b12d2ff2d
> sha512sum LICENSE
    5c4c4e351be428767a0f30cde8bf2687bc31580c408c4b8c6e3a6cf9e4fa0412aabb9037f216e46ad93b85481941dcabe726c1da3714ab5a737be71b12d2ff2d  LICENSE
> rclone copy LICENSE tenant2:bucket2
> rclone ls tenant2:bucket2
    34523 LICENSE
> rclone ls store:mastodon
    34523 blobs/5/c4c/5c4c4e351be428767a0f30cde8bf2687bc31580c408c4b8c6e3a6cf9e4fa0412aabb9037f216e46ad93b85481941dcabe726c1da3714ab5a737be71b12d2ff2d
> rclone copy .dockerignore tenant1:bucket2
> rclone ls tenant1:bucket2
    90 .dockerignore
> rclone ls store:mastodon
    34523 blobs/5/c4c/5c4c4e351be428767a0f30cde8bf2687bc31580c408c4b8c6e3a6cf9e4fa0412aabb9037f216e46ad93b85481941dcabe726c1da3714ab5a737be71b12d2ff2d
       90 blobs/a/3e5/a3e5ca067fc82837a4ae4de26c0c41a7a498173b07f3bb01d5667df4d050c225c3fcd403dd1f324e5559a1d7086885f9b98a1965f3dad787bb0df55403bededa
```

Have fun
