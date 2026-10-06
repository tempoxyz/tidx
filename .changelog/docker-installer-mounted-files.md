---
tidx: patch
---

Fixed the Docker installer skipping `clickhouse-config.xml`, `prometheus.yml` and `alerts.yml`, which the production compose file bind-mounts and Docker otherwise creates as empty directories. Failed downloads now abort the install instead of being saved as the file.
