---
tidx: patch
---

Fixed PostgreSQL `/query` validation accepting unbounded `lpad` and `rpad` lengths. The length must now be an integer literal no larger than 1024, so oversized padding is rejected before the database builds it.
