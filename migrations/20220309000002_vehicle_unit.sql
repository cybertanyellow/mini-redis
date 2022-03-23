-- Add migration script here
CREATE TABLE vehicle_unit (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    manufacture_name CHAR(36) NOT NULL,
    manufacture_address CHAR(36) NOT NULL,
    serial_number CHAR(8) NOT NULL,
    sw_version CHAR(4) NOT NULL, --TODO
    install_timestamp TIMESTAMP DEFAULT NULL, --DEFAULT CURRENT_TIMESTAMP,
    manufacture_date DATE DEFAULT CURRENT_DATE,
    certification_number CHAR(8) NOT NULL,
    car_number CHAR(17) DEFAULT NULL,
    calibration_id INTEGER DEFAULT NULL,
    time_date_id INTEGER DEFAULT NULL,
    FOREIGN KEY (calibration_id) REFERENCES calibration (id) ON DELETE SET NULL
    FOREIGN KEY (time_date_id) REFERENCES time_date (id) ON DELETE SET NULL
);

CREATE TABLE calibration (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    user CHAR(36) NOT NULL,
    odo_before REAL DEFAULT NULL, --TODO, using Double(f64)?
    odo_after REAL NOT NULL, --TODO, using Double(f64)?
    odo_unit Float NOT NULL, --TODO, hidden column, using f32?
    timestamp_before TIMESTAMP DEFAULT NULL, --TODO, ??
    timestamp_after TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE time_date (
    id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    timestamp_before TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    timestamp_after TIMESTAMP DEFAULT NULL
);
