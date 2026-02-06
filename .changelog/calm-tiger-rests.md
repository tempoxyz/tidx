---
tidx: patch
---

Handle SIGTERM for graceful container shutdown. Previously only SIGINT (ctrl-c) triggered graceful shutdown; now SIGTERM from Kubernetes/Docker also triggers the same broadcast for clean connection draining.
