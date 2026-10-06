---
tidx: patch
---

Fixed ClickHouse query validation accepting calls to functions that reach external systems, other servers, local files, or programs, such as `gcs`, `azureBlobStorage`, `iceberg`, `cluster`, and `executable`. These are now rejected like `url` and `s3`.
