# Changelog

## 0.0.3

- Multiple bugfixes and improvement thx to @shleeable
  - Add support for a watchable user conf file
  - Restrict api (purge) to local and private addresses
  - Resource cleanups on failures
  - Limit concurrent race conditions with advisory lock and state checks
  - More coherent Metadata return between endpoints
  - Fix uri handling
- Upgrade dependencies
- Add delimiter support for list endpoint (used by `rclone` for `sync` and `copy`).

## 0.0.2

- Use multipart only for files over 5MB
- Try to create the backing store bucket if missing

## 0.0.1

Initial release

- Rewritten in Scala
- Switch database to Postgres
- Use migration to setup database
- Update to s3proxy 2.6.0
- Use a `filesystem-nio2` local buffer store to compute hash
- Dangling dedup files are now removed using scheduled job

