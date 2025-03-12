# Changelog

## 0.0.1

Initial release

- Rewritten in Scala
- Switch database to Postgres
- Use migration to setup database
- Update to s3proxy 2.6.0
- Use a `filesystem-nio2` local buffer store to compute hash
- Dangling dedup files are now removed using scheduled job

