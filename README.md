
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

## Docker demo

The `--build` ensure that the image is up to date (In case of `Could not connect to 127.0.0.1:3306` error just retry, the DB was just not completely up).

```bash
> docker compose up --build S3DedupProxy
```

This will run three services:

- `S3DedupProxy`: the proxy itself bind 23278
- `Postgres`: the database to store the file metadata (binded on 3306)
- `S3Proxy`: used as local S3 store (binded on 8080)

You will need to create the `mastodon` bucket in the `S3Proxy` store, since it's run without authentication it can be done with `curl`:

```bash
curl --request PUT http://127.0.0.1:8080/mastodon
```

You can then interact with the proxy and store using [MinIO client](https://min.io/docs/minio/linux/reference/minio-mc.html).

```config
{
    "version": "10",
    "aliases": {
        "store": {
            "url": "http://127.0.0.1:80",
            "api": "S3v4",
            "path": "auto"
        },
        "tenant1": {
            "url": "http://127.0.0.1:23278",
            "accessKey": "tenant1",
            "secretKey": "ff54fae017ebe20356223ea016d1e2e867b7f73aaba775fe48ed65d1f1fecb87",
            "api": "S3v4",
            "path": "auto"
        },
        "tenant2": {
            "url": "http://127.0.0.1:23278",
            "accessKey": "tenant2",
            "secretKey": "25ca6d3f04906bfc05a3f2c7ce6b5569ec841ee0ef241a886f34687bbafee7cc",
            "api": "S3v4",
            "path": "auto"
        },
    }
}
```

By default the `test` user must write in the `test` bucket, ex:

```bash
> mc ls store/mastodon/
> mc cp file1 tenant1/bucket1/
> mc ls -r store/mastodon/
[2025-02-13 22:53:04 CET]   151B STANDARD blobs/a/c52/ac52fd97d34cef83527d3b5022775db50b961127ab01b8f646b5040e6f42db02f7d1a6f46bdcd69527d0dbd7d7ee1d92c9681e60b604e2603986516b68541471
> mc cp file1 tenant2/bucket1/
> mc ls -r store/mastodon/
[2025-02-13 22:53:04 CET]   151B STANDARD blobs/a/c52/ac52fd97d34cef83527d3b5022775db50b961127ab01b8f646b5040e6f42db02f7d1a6f46bdcd69527d0dbd7d7ee1d92c9681e60b604e2603986516b68541471
> mc cp file2 tenant1/bucket2/
> mc ls -r store/mastodon/
[2025-02-13 22:55:20 CET]  34KiB STANDARD blobs/8/bb7/8bb7bc575a4a5c18fe537e913f9869bcc016925fdf7c6fbedd3602915cb8341bd609c059f0397f6a42c89bc17baa294f432c3d7983d524d84a8749fd40d1d917
[2025-02-13 22:53:04 CET]   151B STANDARD blobs/a/c52/ac52fd97d34cef83527d3b5022775db50b961127ab01b8f646b5040e6f42db02f7d1a6f46bdcd69527d0dbd7d7ee1d92c9681e60b604e2603986516b68541471
```

Have fun
