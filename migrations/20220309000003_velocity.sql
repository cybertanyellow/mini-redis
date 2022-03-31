-- Add migration script here
CREATE TABLE velocity (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    speed Float DEFAULT NULL,
    odo REAL(8) DEFAULT NULL,
    timestamp TIMESTAMP DATETIME DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    location_id INTEGER DEFAULT NULL,
    driver_id INTEGER DEFAULT NULL,
    
    FOREIGN KEY (location_id) REFERENCES location (id) ON DELETE SET NULL,
    FOREIGN KEY (driver_id) REFERENCES driver (id) ON DELETE SET NULL
);
-- INSERT INTO velocity (speed, timestamp) VALUES (200, strftime("%Y-%m-%d %H:%M:%f", "now"));

CREATE TABLE location (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    logitude REAL NOT NULL,
    latitude REAL NOT NULL,
    altitude REAL NOT NULL, --TODO, f64?
    speed Float NOT NULL,

    -- SELECT timestamp, AVG(logitude) AS logitude, AVG(latitude) AS latitude, AVG(altitude) AS
    -- altitude, AVG(speed) AS speed FROM location GROUP BY timestamp;
    timestamp TIMESTAMP DATETIME DEFAULT(STRFTIME('%Y-%m-%d %H:%M', 'NOW'))
);
