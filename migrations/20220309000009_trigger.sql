-- 30(d) * 24(h) * 60(m) * 60(s) * 2(1/2s) = 5184000
--CREATE TRIGGER velocity_rows_5184000 AFTER INSERT ON velocity
--    WHEN (SELECT COUNT(*) FROM velocity) > 5184000
--    BEGIN
--        DELETE FROM velocity WHERE id = (SELECT MIN(id) FROM velocity);
--    END;
--END;

-- remove last month velocity's rows
-- test:
--      INSERT INTO velocity (speed, odo, timestamp) VALUES (11.1, 99999.9, strftime("%Y-%m-%d %H:%M:%f", "now", "-3 month"));
--      INSERT INTO velocity (speed, odo, timestamp) VALUES (11.1, 99999.9, strftime("%Y-%m-%d %H:%M:%f", "now", "-2 month"));
--      INSERT INTO velocity (speed, odo, timestamp) VALUES (11.1, 99999.9, strftime("%Y-%m-%d %H:%M:%f", "now", "-2 minute"));
--      INSERT INTO velocity (speed, odo) VALUES (11.1, 99999.9);
CREATE TRIGGER velocity_rows_limit_day30 AFTER INSERT ON velocity
    WHEN (SELECT COUNT(*) FROM velocity AS v WHERE date(v.timestamp) <  date('now', '-1 month')) > 0
    BEGIN
        --XXX not good, DELETE FROM velocity WHERE id = (SELECT MIN(id) FROM velocity);
        DELETE FROM velocity WHERE date(timestamp) < date('now', '-1 month');
    END;
END;

-- autocomplete velocity's driver&location
-- test: INSERT INTO velocity (speed, odo) VALUES (99, 300);
CREATE TRIGGER velocity_autocomplete_driver_foreign_key AFTER INSERT ON velocity
    WHEN NEW.driver_id IS NULL
BEGIN
    UPDATE velocity SET driver_id = (SELECT id from driver WHERE action BETWEEN 1 AND 2 ORDER BY id DESC LIMIT 1) WHERE id = NEW.id;
END;
CREATE TRIGGER velocity_autocomplete_location_foreign_key AFTER INSERT ON velocity
    WHEN NEW.location_id IS NULL
BEGIN
    UPDATE velocity SET location_id = (SELECT id from location ORDER BY id DESC LIMIT 1) WHERE id = NEW.id;
END;

-- SPEC 5.2.5
CREATE TRIGGER velocity_stop_driver_stop_auto AFTER INSERT ON velocity
    WHEN NEW.speed = 0.0 AND (SELECT action from driver ORDER BY id DESC LIMIT 1) = 1
BEGIN
    INSERT INTO driver (account, action, travel_threshold_id, rest_threshold_id)
    SELECT account, 2, travel_threshold_id, rest_threshold_id from driver WHERE action = 1 ORDER BY id DESC LIMIT 1;
END;

-- 365(d) * 24(h) * 60(m) = 525600
--CREATE TRIGGER location_rows_525600 AFTER INSERT ON location
--    WHEN (SELECT COUNT(*) FROM location) > 525600
--    BEGIN
--        DELETE FROM location WHERE id = (SELECT MIN(id) FROM location);
--    END;
--END;

-- remove older than one year location's rows
-- test
--      INSERT INTO location (logitude, latitude, altitude, timestamp, speed) VALUES (33.3, 44.4, 55.5, strftime("%Y-%m-%d %H:%M:%f", "now", "-2 minute"), 66.66);
--      INSERT INTO location (logitude, latitude, altitude, timestamp, speed) VALUES (33.4, 44.4, 55.6, strftime("%Y-%m-%d %H:%M:%f", "now", "-1 minute"), 10.1);
--      INSERT INTO location (logitude, latitude, altitude, timestamp, speed) VALUES (33.5, 44.4, 55.7, CURRENT_TIMESTAMP, 5.0);
--      INSERT INTO location (logitude, latitude, altitude, timestamp, speed) VALUES (33.4, 44.4, 55.6, strftime("%Y-%m-%d %H:%M:%f", "now", "-1 year", "-1 month"), 10.1);
CREATE TRIGGER location_rows_limit_day365 AFTER INSERT ON location
    WHEN (SELECT COUNT(*) FROM location WHERE date(timestamp) < date('now', '-1 year')) > 0
    BEGIN
        --XXX not good, DELETE FROM location WHERE id = (SELECT MIN(id) FROM location);
        DELETE FROM location WHERE date(timestamp) < date('now', '-1 year');
    END;
END;

--TODO, WTF syntax error
--CREATE TRIGGER driver_action_changed BEFORE UPDATE ON driver
--    WHEN old.action <> new.action
--    BEGIN
--        INSERT INTO driver (account, action, travel_threshold_id, rest_threshold_id) VALUES
--            (old.account, new.action, old.travel_threshold_id, old.rest_threshold_id);
--        RAISE (IGNORE)
--    END;
--END;
--TODO, WTF not work
--CREATE TRIGGER driver_action_changed BEFORE INSERT ON driver
--    WHEN (SELECT action FROM driver WHERE account = NEW.account ORDER BY id DESC LIMIT 1) = NEW.action
--    BEGIN
--        SELECT CASE WHEN NEW.account <> NULL THEN RAISE (IGNORE)
--    END;
--END;

-- limit per driver's record in 365day
CREATE TRIGGER driver_rows_limit_day365 AFTER INSERT ON driver
    WHEN (SELECT COUNT(*) FROM driver AS v WHERE date(v.timestamp) <  date('now', '-1 year')) > 0
    BEGIN
        --DELETE FROM driver WHERE id = (SELECT MIN(id) FROM driver WHERE account = NEW.account);
        DELETE FROM driver WHERE date(timestamp) < date('now', '-1 year');
    END;
END;

-- autocomplete travel/rest-threshold
-- last_insert_rowid()
-- test: 
--      INSERT INTO travel_threshold (timestamp_before, threshold_after) VALUES (datetime('now', '-1 month'), 5.00);
--      INSERT INTO rest_threshold (timestamp_before, threshold_after) VALUES (datetime('now', '-1 month'), 0.45);
--      INSERT INTO driver (account, action) VALUES ('aaaa', 1);
--      INSERT INTO driver (account, action) VALUES ('dddd', 3);
--      INSERT INTO driver (account, action) VALUES ('ccc', 2);
CREATE TRIGGER driver_autocomplete_threshold_foreign_key AFTER INSERT ON driver
    WHEN NEW.travel_threshold_id IS NULL OR NEW.rest_threshold_id IS NULL
BEGIN
    UPDATE driver SET (travel_threshold_id,rest_threshold_id) = (SELECT travel_threshold.id,rest_threshold.id from travel_threshold,rest_threshold
        ORDER BY travel_threshold.id + rest_threshold.id DESC LIMIT 1) WHERE id = NEW.id;
END;

-- SPEC 5.2.4
-- manual insert driver/driving after velocity ??/5s pass
--CREATE TRIGGER driver_driving_force_other_standby BEFORE INSERT ON driver
--    WHEN NEW.action = 1
--BEGIN
--    TODO
--END;

-- autocomplete event's velocity
-- test:
--      INSERT INTO velocity (speed, odo) VALUES (20.1, 100000.9);
--      INSERT INTO error (fault_type) VALUES (0x11);
--      INSERT INTO event (event_type) VALUES (0x12);
CREATE TRIGGER event_autocomplete_foreign_velocity_foreign_key AFTER INSERT ON event
    WHEN NEW.velocity_id IS NULL
BEGIN
    UPDATE event SET velocity_id = (SELECT id from velocity ORDER BY id DESC LIMIT 1) WHERE id = NEW.id;
END;
CREATE TRIGGER error_autocomplete_foreign_velocity AFTER INSERT ON error
    WHEN NEW.velocity_id IS NULL
BEGIN
    UPDATE error SET velocity_id = (SELECT id from velocity ORDER BY id DESC LIMIT 1) WHERE id = NEW.id;
END;
-- keep 10 rows for the same error type
-- test:
--      INSERT INTO error (fault_type) VALUES (0x11);
--      (x10)
--      INSERT INTO error (fault_type) VALUES (0x11);
CREATE TRIGGER error_type_keep10 AFTER INSERT ON error
    WHEN (SELECT COUNT(*) FROM error WHERE fault_type = NEW.fault_type) > 10
    BEGIN
        DELETE FROM error WHERE id = (SELECT MIN(id) FROM error);
    END;
END;

-- keep latest row after calibration
-- test:
--      INSERT INTO calibration (user, odo_after, odo_unit) VALUES ('inesta', 3000.1, 20.0);
--      INSERT INTO error (fault_type) VALUES (0x02);
CREATE TRIGGER error_type_after_calibration AFTER INSERT ON error
    WHEN (SELECT COUNT(*) FROM error WHERE fault_type & 0x80) = 0
    BEGIN
        UPDATE error SET fault_type = (0x80 | NEW.fault_type) WHERE id = NEW.id;
    END;
END;
CREATE TRIGGER calibration_clean_error AFTER INSERT ON calibration
    WHEN (SELECT COUNT(*) FROM error WHERE fault_type & 0x80) > 0
    BEGIN
        DELETE FROM error WHERE fault_type & 0x80;
    END;
END;

-- rule: for power supply, keep longest in 10day. keep 5 longest in 365d
CREATE TRIGGER event_type_power_supply AFTER INSERT ON event
    WHEN NEW.event_type = 0x01
    BEGIN
        --SELECT CASE WHEN NEW.account <> NULL THEN RAISE (IGNORE)
        --DELETE FROM event WHERE event_type = 0x01 AND date(timestamp) < date('now', '-1 year');
        --DELETE FROM event WHERE id IN (SELECT id FROM event WHERE id = 0x01 AND date(timestamp) < date('now', '-1 year'));
        --DELETE FROM time_date WHERE id NOT IN (SELECT id FROM time_date ORDER BY id DESC LIMIT 5);

        --DELETE FROM event WHERE event_type = 0x01 AND date(event_start) < date('now', '-1 year');
        DELETE FROM event WHERE event_type = 0x01 AND id NOT IN (SELECT id FROM event WHERE event_type = 0x01 ORDER BY event_end - event_start DESC LIMIT 5);
    END;
END;

-- except power-supply, keep 10 rows for the same event type
-- test:
--      INSERT INTO error (fault_type) VALUES (0x11);
--      (x10)
--      INSERT INTO error (fault_type) VALUES (0x11);
CREATE TRIGGER event_type_keep10 AFTER INSERT ON event
    WHEN NEW.event_type <> 0x01 AND (SELECT COUNT(*) FROM event WHERE event_type = NEW.event_type) > 10
    BEGIN
        DELETE FROM event WHERE id = (SELECT MIN(id) FROM error);
    END;
END;

-- autocomplete vehicle_unit's calibration/time_date foreign key
-- last_insert_rowid()
-- test:
--      INSERT INTO calibration (user, odo_after, odo_unit) VALUES ('xavi', '1000.1', 10.2);
--      INSERT INTO calibration (user, odo_after, odo_unit) VALUES ('messi', 2000.1, 10.9);
--      INSERT INTO time_date (timestamp_after) VALUES (date('now', '+1 hour'));
--      INSERT INTO time_date (timestamp_after) VALUES (date('now', '-1 hour'));
--      INSERT INTO time_date (timestamp_after) VALUES (date('now', '-30 min'));
--      INSERT INTO vehicle_unit (manufacture_name, manufacture_address, serial_number, sw_version, certification_number)
--            VALUES ('arcadyan', '3-1-21, shibaura, minato-ku, Tokyo, Japen', 'TT2B000000001', '01.00.00.04', '000000001');
CREATE TRIGGER vehicle_unit_autocomplete_foreign_key AFTER INSERT ON vehicle_unit
    WHEN NEW.calibration_id IS NULL OR NEW.time_date_id IS NULL 
BEGIN
    UPDATE vehicle_unit SET (calibration_id,time_date_id) = (SELECT calibration.id,time_date.id from calibration,time_date
        ORDER BY calibration.id + time_date.id DESC LIMIT 1) WHERE id = NEW.id;
END;

-- rule: limit last 5 row in threshold/time_date
-- test: 
--      INSERT INTO travel_threshold (timestamp_before, threshold_after) VALUES (datetime('now', '-1 month'), 5.00);
--      (x 6)
CREATE TRIGGER travel_threshold_limit_rows5 AFTER INSERT ON travel_threshold
    WHEN (SELECT COUNT(*) FROM travel_threshold) > 5
    BEGIN
        DELETE FROM travel_threshold WHERE id NOT IN (SELECT id FROM travel_threshold ORDER BY id DESC LIMIT 5);
    END;
END;
CREATE TRIGGER rest_threshold_limit_rows5 AFTER INSERT ON rest_threshold
    WHEN (SELECT COUNT(*) FROM rest_threshold) > 5
    BEGIN
        DELETE FROM rest_threshold WHERE id NOT IN (SELECT id FROM rest_threshold ORDER BY id DESC LIMIT 5);
    END;
END;
CREATE TRIGGER time_date_limit_rows5 AFTER INSERT ON time_date
    WHEN (SELECT COUNT(*) FROM time_date) > 5
    BEGIN
        DELETE FROM time_date WHERE id NOT IN (SELECT id FROM time_date ORDER BY id DESC LIMIT 5);
    END;
END;
