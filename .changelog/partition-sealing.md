---
tidx: minor
---

Added partition lifecycle maintenance: partitions were pre-created ahead of the chain head, and partitions leaving the hot window (`hot_window_blocks`, default 2M) were sealed — clustered by primary key, analyzed, and frozen.
