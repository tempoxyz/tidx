-- Async background mutation; existing parts gain the projections progressively.
ALTER TABLE blocks
    MATERIALIZE PROJECTION prj_hash,
    MATERIALIZE PROJECTION prj_timestamp_position;
