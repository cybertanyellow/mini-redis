-- Add migration script here
CREATE TABLE velocity (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    speed Float DEFAULT NULL,
    odo Double DEFAULT NULL,
    timestamp TIMESTAMP DATETIME DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW')),
    location_id INTEGER DEFAULT NULL,
    driver_id INTEGER DEFAULT NULL,
    
    FOREIGN KEY (location_id) REFERENCES location (id) ON DELETE SET NULL,
    FOREIGN KEY (driver_id) REFERENCES driver (id) ON DELETE SET NULL
);
-- INSERT INTO velocity (speed, timestamp) VALUES (200, strftime("%Y-%m-%d %H:%M:%f", "now"));

CREATE TABLE location (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    logitude Double NOT NULL,
    latitude Double NOT NULL,
    altitude Double NOT NULL, --TODO, f64?
    speed_avg Float NOT NULL,

    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP --reserved
);
