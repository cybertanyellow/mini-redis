CREATE TABLE error (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    -- [fault_type]:
    --  0x80 for first record after calibration,
    fault_type BINARY NOT NULL,
    fault_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    fault_end TIMESTAMP DEFAULT NULL,
    velocity_id INTEGER DEFAULT NULL,
    FOREIGN KEY (velocity_id) REFERENCES velocity (id) ON DELETE SET NULL
);

CREATE TABLE event (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    --TODO, event_type BIT(8) NOT NULL,
    -- [event_type]:
    --  0x01 for power-supply
    --  0x02 for safety-attack
    -- ...
    event_type BINARY NOT NULL,
    event_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_end TIMESTAMP DEFAULT NULL,
    velocity_id INTEGER DEFAULT NULL,
    FOREIGN KEY (velocity_id) REFERENCES velocity (id) ON DELETE SET NULL
);
