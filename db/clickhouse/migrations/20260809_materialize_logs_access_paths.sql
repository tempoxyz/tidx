-- Async background mutation; existing parts gain the access paths progressively.
ALTER TABLE logs
    MATERIALIZE INDEX idx_selector_topic1,
    MATERIALIZE INDEX idx_selector_topic2,
    MATERIALIZE INDEX idx_selector_topic3,
    MATERIALIZE PROJECTION prj_address_position,
    MATERIALIZE PROJECTION prj_selector_address_position,
    MATERIALIZE PROJECTION prj_selector_topic1_position,
    MATERIALIZE PROJECTION prj_selector_topic2_position,
    MATERIALIZE PROJECTION prj_selector_topic3_position,
    MATERIALIZE PROJECTION prj_tx_hash;
