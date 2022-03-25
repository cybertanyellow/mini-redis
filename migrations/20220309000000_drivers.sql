-- Add migration script here
CREATE TABLE driver (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    account TEXT NOT NULL,
    action INTEGER NOT NULL, --TODO/XXX, 1/2/3/4 for driving/stopping/standby/resting
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    travel_threshold_id INTEGER DEFAULT NULL, --TODO, auto completed or manual assign?
    rest_threshold_id INTEGER DEFAULT NULL, --TODO, auto completed or manual assign?

    FOREIGN KEY (travel_threshold_id) REFERENCES travel_threshold (id) ON DELETE SET NULL,
    FOREIGN KEY (rest_threshold_id) REFERENCES rest_threshold (id) ON DELETE SET NULL
);

CREATE TABLE travel_threshold (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    timestamp_before TIMESTAMP DEFAULT NULL, --TODO, not sure
    timestamp_after TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    threshold_before FLOAT DEFAULT 4.00,
    threshold_after FLOAT NOT NULL
);

CREATE TABLE rest_threshold (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    timestamp_before TIMESTAMP DEFAULT NULL, --TODO
    timestamp_after TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    threshold_before FLOAT DEFAULT 0.30,
    threshold_after FLOAT NOT NULL
);
