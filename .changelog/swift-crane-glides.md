---
tidx: minor
---

Support PostgreSQL password via environment variable. Add `pg_password_env` config option to inject the password from an env var into `pg_url` at runtime, avoiding plaintext passwords in config files. Existing configs without `pg_password_env` work unchanged.
