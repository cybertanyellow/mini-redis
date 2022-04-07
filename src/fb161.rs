use tracing::{debug, info, warn};
use std::env;
use bytes::Bytes;
use sqlx::sqlite::SqlitePool;
use sqlx::pool::PoolConnection;
use sqlx::types::chrono::{DateTime, Utc, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use chrono::serde::ts_seconds;
use anyhow::Result;
use tokio::fs::File;
use tokio::io::{SeekFrom, AsyncSeekExt};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::io::{AsyncReadExt, BufReader};
use sqlx::Sqlite;
use chrono::{Timelike, Datelike};
use tokio_stream::StreamExt;
use std::convert::TryInto;

use serde_big_array::BigArray;
use anyhow::anyhow;
use std::mem;
use dotenv::dotenv;

#[derive(Debug)]
pub(crate) struct Fb161Sql {
    odo: Option<f32>, /*TODO for f64 */
    velocity_id: Option<i64>,
    location_id: Option<i64>,
    driver_id: Option<i64>,
    conn: PoolConnection<Sqlite>,
}

#[derive(Serialize, Deserialize, Debug)]
enum DriverAction {
    Driving(i32),
    Stopping(i32),
    Standby(i32),
    Resting(i32),
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheCalibration {
    user: String,
    odo_after: f64,
    odo_unit: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheTimeDate {
    timestamp_before: Option<DateTime<Utc>>,
    //#[serde(with = "ts_seconds")]
    timestamp_after: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheVehicleUnit {
    manufacture_name: String,
    manufacture_address: String,
    serial_number: String,
    sw_version: String,
    #[serde(with = "ts_seconds")]
    install_timestamp: DateTime<Utc>,
    manufacture_date: NaiveDate,
    certification_number: String,
    car_number: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheError {
    fault_type: u16,
    //#[serde(with = "ts_seconds")]
    fault_start: Option<DateTime<Utc>>,
    //#[serde(with = "ts_seconds")]
    fault_end: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheEvent {
    event_type: u16,
    //#[serde(with = "ts_seconds")]
    event_start: Option<DateTime<Utc>>,
    //#[serde(with = "ts_seconds")]
    event_end: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheDriver {
    account: String,
    //action: DriverAction,
    action: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheTravelThreshold {
    //#[serde(with = "ts_seconds")]
    timestamp_before: Option<DateTime<Utc>>,
    //#[serde(with = "ts_seconds")]
    timestamp_after: Option<DateTime<Utc>>,
    threshold_before: Option<f32>,
    threshold_after: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheRestThreshold {
    //#[serde(with = "ts_seconds")]
    timestamp_before: Option<DateTime<Utc>>,
    //#[serde(with = "ts_seconds")]
    timestamp_after: Option<DateTime<Utc>>,
    threshold_before: Option<f32>,
    threshold_after: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct CacheVelocity {
    speed: f32,
    odo: Option<f32>, /*TODO for f64 */
    timestamp: Option<DateTime<Utc>>,
}

/*#[derive(Debug)]
struct SqlVelocity {
    id: i64,
    speed: Option<f32>,
    odo: Option<f32>,
    timestamp: Option<String>,
    location_id: Option<i64>,
    driver_id: Option<i64>,
}*/

#[derive(Serialize, Deserialize, Debug)]
struct CacheLocation {
    logitude: f64,
    latitude: f64,
    altitude: f64,
    speed: f32,
}

impl Fb161Sql {
    pub async fn new() -> Self {
        let url: String = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://:memory:".to_string());
        let pool: SqlitePool = SqlitePool::connect(&url).await.unwrap();
        let mut conn = pool.acquire().await.unwrap();

        let mut odo = None;
        let mut velocity_id = None;
        let mut location_id = None;
        let mut driver_id = None;
        if let Ok(record) = sqlx::query!(r#"select id,odo,location_id,driver_id from velocity order by id desc limit 1"#).fetch_one(&mut conn).await {
            odo = record.odo;
            velocity_id = Some(record.id);
            location_id = record.location_id;
            driver_id = record.driver_id;
        }

        Fb161Sql {
            odo,
            velocity_id,
            location_id,
            driver_id,
            conn,
        }

    }

    async fn _insert_velocity(&mut self, value: Option<Bytes>) -> Result<i64> {
        if let Some(value) = value {
            self.update_velocity(&value).await
        }
        else {
            let velocity = CacheVelocity {
                speed: 0.0,
                odo: None,
                timestamp: None,
            };

            let r = sqlx::query!(r#"INSERT INTO velocity ( speed, odo, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4 )"#,
            velocity.speed, self.odo, self.location_id, self.driver_id).execute(&mut self.conn).await?;
            self.velocity_id = Some(r.last_insert_rowid());
            info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
            Ok(r.last_insert_rowid())
        }
        // TODO, SPEC 5.1.7.1 & 5.2.4, if moving(3km/hr, >5s),
        // current driver action is driving,
        // other driver(s) action is standby
    }

    async fn insert_location(&mut self, value: &Bytes) -> Result<i64> {
        let mut location = CacheLocation {
            logitude: 0.0,
            latitude: 0.0,
            altitude: 0.0,
            speed: 0.0,
        };
        if let Ok(json) = serde_json::from_slice(&value) {
            location = json;
        }
        let r = sqlx::query!(r#"INSERT INTO location (logitude, latitude, altitude, speed) VALUES (?1, ?2, ?3, ?4)"#,
        location.logitude, location.latitude, location.altitude, location.speed).execute(&mut self.conn).await?;
        self.location_id = Some(r.last_insert_rowid());
        info!("INSERT location with {:?} OK {:?}", location, self.location_id);
        Ok(r.last_insert_rowid())
    }

    async fn update_driver(&mut self, value: &Bytes) -> Result<i64> {
        let driver: CacheDriver = serde_json::from_slice(value)?;
        //println!("driver {:?}", driver);

        let r = sqlx::query!(r#"INSERT INTO driver ( account, action ) VALUES ( ?1, ?2 )"#,
        driver.account, driver.action).execute(&mut self.conn).await?;
        let rowid = r.last_insert_rowid();
        debug!("INSERT driver with {:?} OK {:?}", driver, rowid);
        if driver.action == 1 || driver.action == 2 {
            self.driver_id = Some(rowid);
        }
        Ok(rowid)
    }

    async fn update_vehicle_unit(&mut self, _value: &Bytes) -> Result<()> {
        anyhow::bail!("TODO");
        /*warn!("TODO");
        Ok(-1)*/
    }

    async fn update_calibration(&mut self, value: &Bytes) -> Result<i64> {
        /* TODO change odo/mileage */
        let cal: CacheCalibration = serde_json::from_slice(value)?;
        println!("calibration {:?}", cal);

        let r = sqlx::query!(r#"INSERT INTO calibration ( user, odo_after, odo_unit ) VALUES ( ?1, ?2, ?3 )"#,
        cal.user, cal.odo_after, cal.odo_unit).execute(&mut self.conn).await?;
        let rowid = r.last_insert_rowid();
        debug!("INSERT calibration with {:?} OK {:?}", cal, rowid);
        Ok(rowid)
    }

    async fn update_time_date(&mut self, value: &Bytes) -> Result<i64> {
        /* TODO change system time */
        let tm: CacheTimeDate = serde_json::from_slice(value)?;

        //println!("date-time {:?}", tm);
        match (tm.timestamp_before, tm.timestamp_after) {
            (Some(before), Some(after)) => {
                let r =  sqlx::query!(r#"INSERT INTO time_date ( timestamp_before, timestamp_after ) VALUES ( ?1, ?2 )"#,
                before, after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT date-time {:?} OK ID-{}", tm, rowid);
                Ok(rowid)
            }
            (Some(before), None) => {
                let r = sqlx::query!(r#"INSERT INTO time_date ( timestamp_before ) VALUES ( ?1 )"#,
                before).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT date-time {:?} OK ID-{}", tm, rowid);
                Ok(rowid)
            }
            (None, Some(after)) => {
                let r = sqlx::query!(r#"UPDATE time_date SET timestamp_after = ?1 WHERE timestamp_after IS NULL"#,
                                     after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("UPDATE date-time {:?} OK ID-{}", tm, rowid);
                Ok(rowid)
            }
            (None, None) => {
                let r = sqlx::query!(r#"INSERT INTO time_date ( timestamp_after ) VALUES ( NULL )"#).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT date-time {:?} OK ID-{}", tm, rowid);
                Ok(rowid)
            }
        }
    }

    async fn update_event(&mut self, value: &Bytes) -> Result<i64> {
        let event = serde_json::from_slice::<CacheEvent>(value)?;
        debug!("event {:?}", event);

        if event.event_end.is_some() {
            let r = sqlx::query!(r#"UPDATE event SET event_end = ?2 WHERE event_type = ?1"#,
                                 event.event_type, event.event_end).execute(&mut self.conn).await?;
            let rowid = r.last_insert_rowid();
            info!("UPDATE event {:?} OK by id-{}", event, rowid);
            Ok(rowid)
        } else {
            let r = sqlx::query!(r#"INSERT INTO event (event_type, velocity_id) VALUES ( ?1, ?2)"#,
            event.event_type, self.velocity_id).execute(&mut self.conn).await?;
            let rowid = r.last_insert_rowid();
            debug!("INSERT event - {:?} OK by id-{}", event, rowid);
            Ok(rowid)
        }
    }

    async fn update_error(&mut self, value: &Bytes) -> Result<i64> {
        let error = serde_json::from_slice::<CacheError>(value)?;
        match (error.fault_start, error.fault_end) {
            (Some(fault_start), Some(fault_end)) => {
                let r = sqlx::query!(r#"INSERT INTO error ( fault_type, fault_start, fault_end, velocity_id ) VALUES ( ?1, ?2, ?3, ?4)"#,
                error.fault_type, fault_start, fault_end, self.velocity_id).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", error, rowid);
                Ok(rowid)
            }
            (Some(fault_start), None) => {
                let r = sqlx::query!(r#"INSERT INTO error ( fault_type, fault_start, velocity_id ) VALUES ( ?1, ?2, ?3 )"#,
                error.fault_type, fault_start, self.velocity_id).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", error, rowid);
                Ok(rowid)
            }
            (None, Some(fault_end)) => {
                let r = sqlx::query!(r#"UPDATE error SET fault_end = ?2 WHERE fault_type = ?1 AND fault_end IS NULL"#,
                                     error.fault_type, fault_end).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", error, rowid);
                Ok(rowid)
            }
            (None, None) => {
                let r = sqlx::query!(r#"INSERT INTO error ( velocity_id ) VALUES ( ?1 )"#, self.velocity_id)
                    .execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", error, rowid);
                Ok(rowid)
            }
        }
    }

    async fn update_travel_threshold(&mut self, value: &Bytes) -> Result<i64> {
        let thresh = serde_json::from_slice::<CacheTravelThreshold>(value)?;
        match (thresh.timestamp_before, thresh.timestamp_after, thresh.threshold_before) {
            (Some(ts_before), Some(ts_after), Some(before)) => {
                let r = sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_before, timestamp_after, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3, ?4)"#,
                ts_before, ts_after, before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (Some(ts_before), Some(ts_after), None) => {
                let r = sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_before, timestamp_after, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                ts_before, ts_after, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (Some(ts_before), None, Some(before)) => {
                let r = sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_before, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                ts_before, before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (None, Some(ts_after), Some(before)) => {
                let r = sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_after, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                ts_after, before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            ///////////// two NONE ///////////
            (Some(ts_before), None, None) => {
                let r = sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_before, threshold_after ) VALUES ( ?1, ?2 )"#,
                ts_before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (None, None, Some(before)) => {
                let r = sqlx::query!(r#"INSERT INTO travel_threshold ( threshold_before, threshold_after ) VALUES ( ?1, ?2 )"#,
                before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (None, Some(ts_after), None) => {
                let r = sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_after, threshold_after ) VALUES ( ?1, ?2 )"#,
                ts_after, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (None, None, None) => {
                let r = sqlx::query!(r#"INSERT INTO travel_threshold ( threshold_after ) VALUES ( ?1 )"#,
                thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
        }
    }

    async fn update_rest_threshold(&mut self, value: &Bytes) -> Result<i64> {
        let thresh = serde_json::from_slice::<CacheRestThreshold>(value)?;
        match (thresh.timestamp_before, thresh.timestamp_after, thresh.threshold_before) {
            (Some(ts_before), Some(ts_after), Some(before)) => {
                let r = sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_before, timestamp_after, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3, ?4)"#,
                ts_before, ts_after, before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (Some(ts_before), Some(ts_after), None) => {
                let r = sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_before, timestamp_after, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                ts_before, ts_after, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (Some(ts_before), None, Some(before)) => {
                let r = sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_before, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                ts_before, before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (None, Some(ts_after), Some(before)) => {
                let r = sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_after, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                ts_after, before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            ///////////// two NONE ///////////
            (Some(ts_before), None, None) => {
                let r = sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_before, threshold_after ) VALUES ( ?1, ?2 )"#,
                ts_before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (None, None, Some(before)) => {
                let r = sqlx::query!(r#"INSERT INTO rest_threshold ( threshold_before, threshold_after ) VALUES ( ?1, ?2 )"#,
                before, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (None, Some(ts_after), None) => {
                let r = sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_after, threshold_after ) VALUES ( ?1, ?2 )"#,
                ts_after, thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
            (None, None, None) => {
                let r = sqlx::query!(r#"INSERT INTO rest_threshold ( threshold_after ) VALUES ( ?1 )"#,
                thresh.threshold_after).execute(&mut self.conn).await?;
                let rowid = r.last_insert_rowid();
                info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                Ok(rowid)
            },
        }
    }

    async fn update_velocity(&mut self, value: &Bytes) -> Result<i64> {
        let velocity = serde_json::from_slice::<CacheVelocity>(value)?;

        match (velocity.odo, velocity.timestamp) {
            (Some(_odo), Some(_timestamp)) => {
                self.odo = velocity.odo;
                let r = sqlx::query!(r#"INSERT INTO velocity ( speed, odo, timestamp, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4, ?5 )"#,
                velocity.speed, velocity.odo, velocity.timestamp, self.location_id, self.driver_id).execute(&mut self.conn).await?;
                self.velocity_id = Some(r.last_insert_rowid());
                info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                Ok(r.last_insert_rowid())
            },
            (Some(_odo), None) => {
                self.odo = velocity.odo;
                let r = sqlx::query!(r#"INSERT INTO velocity ( speed, odo, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4 )"#,
                velocity.speed, velocity.odo, self.location_id, self.driver_id).execute(&mut self.conn).await?;
                self.velocity_id = Some(r.last_insert_rowid());
                info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                Ok(r.last_insert_rowid())
            },
            (None, Some(_timestamp)) => {
                self.odo = self.odo.map(|x| x + velocity.speed / 7200.0);
                println!("None/Some => odo {:?}", self.odo);
                let r = sqlx::query!(r#"INSERT INTO velocity ( speed, odo, timestamp, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4, ?5 )"#,
                velocity.speed, self.odo, velocity.timestamp, self.location_id, self.driver_id).execute(&mut self.conn).await?;
                self.velocity_id = Some(r.last_insert_rowid());
                info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                Ok(r.last_insert_rowid())
            }
            (None, None) => {
                //self.odo = self.odo.map(|x| x + velocity.speed * 0.5);
                self.odo = self.odo.map(|x| x + velocity.speed / 7200.0);
                println!("None/None => odo {:?}", self.odo);
                let r = sqlx::query!(r#"INSERT INTO velocity ( speed, odo, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4 )"#,
                velocity.speed, self.odo, self.location_id, self.driver_id).execute(&mut self.conn).await?;
                self.velocity_id = Some(r.last_insert_rowid());
                info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                Ok(r.last_insert_rowid())
            }
        }
    }

    pub(crate) async fn commit(&mut self, key: &str, val: &Bytes) -> Result<i64> {
        match key {
            "velocity" => { self.update_velocity(val).await?; },
            "location" => { self.insert_location(val).await?; },
            "driver" => { self.update_driver(val).await?; },
            "event" => { self.update_event(val).await?; },
            "error" => { self.update_error(val).await?; },
            "travel-threshold" => { self.update_travel_threshold(val).await?; },
            "rest-threshold" => { self.update_rest_threshold(val).await?; },
            "vehicle-unit" => { self.update_vehicle_unit(val).await?; },
            "calibration" => { self.update_calibration(val).await?; },
            "time-date" => { self.update_time_date(val).await?; },
            _ => { warn!("Unknown key-{} with value-{:?}", key, val)},
        }
        Ok(-1)
    }
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadEntryLocationPer {
    logitude: f32,
    latitude: f32,
    altitude: u16, /* TODO */
    speed: u8,
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
struct DownloadEntryLocationHour {
    start: [u8; 7],
    #[serde(with = "BigArray")]
    hour: [DownloadEntryLocationPer; 60],
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockGeneric {
    code: u8,
    name: [u8; 18],
    len: u32,
    num: u16,
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadEntryDriver {
    account: [u8; 18],
    action: u8,
    dtime: [u8; 7],
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockCalibration {
    #[serde(with = "BigArray")]
    user: [u8; 36],
    odo_before: [u8; 4], // TODO
    odo_after: [u8; 4], // TODO
    timestamp_before: [u8; 7],
    timestamp_after: [u8; 7],
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockTimeDate {
    timestamp_before: [u8; 7],
    timestamp_after: [u8; 7],
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockTravelThreshold {
    timestamp_before: [u8; 7],
    timestamp_after: [u8; 7],
    threshold_before: [u8; 2], // BCD
    threshold_after: [u8; 2], // BCD
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockRestThreshold {
    timestamp_before: [u8; 7],
    timestamp_after: [u8; 7],
    threshold_before: [u8; 2], // BCD
    threshold_after: [u8; 2], // BCD
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockMeta {
    code: u8,
    name: [u8; 18],
    len: u32,
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockFactory {
    #[serde(with = "BigArray")]
    manufacture_name: [u8; 36],
    #[serde(with = "BigArray")]
    manufacture_address: [u8; 36],
    serial_number: [u8; 8],
    sw_version: [u8; 4],
    install_timestamp: [u8; 7],
    manufacture_date: [u8; 4],
    certification_number: [u8; 8],
    car_number: [u8; 17],
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct DownloadEntryVelocity {
    date_time: [u8; 7],
    #[serde(with = "BigArray")]
    speed: [u8; 120],
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct DownloadEntryError {
    etype: u8,
    start_ts: [u8; 7],
    end_ts: [u8; 7],
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct DownloadEntryEvent {
    etype: u8,
    start_ts: [u8; 7],
    end_ts: [u8; 7],
    related: u8,
}

struct SqlVelocity {
    speed: Option<f32>,
    timestamp: Option<NaiveDateTime>,
}

pub struct Fb161Downloader {
    db_url: Option<String>,
    start_time: Option<NaiveDateTime>,
    dst: String,

    writer: BufWriter<File>,
    //reader: BufReader<File>,
}

impl Fb161Downloader {
    pub async fn new(db_url: Option<String>, start_time: Option<NaiveDateTime>, dst: String) -> Self {
        if let Ok(file) = File::create(&dst).await {
            Self {
                writer: BufWriter::new(file),

                db_url,
                start_time,
                dst,
            }
        }
        else {
            panic!("Fb161Downloader::new");
        }
    }

    async fn db_connect(&self) -> Result<PoolConnection<Sqlite>> {
        dotenv().ok();

        let mut url: String = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://:memory:".to_string());
        if let Some(db_url) = &self.db_url {
            url = db_url.clone();
        }
        let pool: SqlitePool = SqlitePool::connect(&url).await?;
        Ok(pool.acquire().await?)
    }

    async fn header(&mut self, block_num: u16) -> Result<()> {
        //let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;

        //self.writer.seek(SeekFrom::Start(0)).await?;
        self.writer.write(&block_num.to_le_bytes()).await?;

        //self.writer.seek(SeekFrom::Start(orig_pos)).await?;

        Ok(())
    }

    async fn tail(&mut self, block_checksum: u8) -> Result<()> {
        //let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;

        //self.writer.seek(SeekFrom::End(0)).await?;
        self.writer.write(&block_checksum.to_le_bytes()).await?;

        //self.writer.seek(SeekFrom::Start(orig_pos)).await?;

        Ok(())
    }

    pub async fn download_event(&mut self) -> Result<()> {
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        let event_num: u8;
        let error_num: u8;
        let start_time = self.start_time.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));

        let seek_it = SeekFrom::Current(mem::size_of::<DownloadBlockMeta>().try_into().unwrap());
        self.writer.seek(seek_it).await?;

        {
            let mut conn = self.db_connect().await?;

            let row = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM error WHERE fault_start >= ?"#, start_time).fetch_one(&mut conn).await?;
            error_num = row.cnt as u8;
            self.writer.write(&error_num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT fault_type, fault_start, fault_end FROM error WHERE fault_start >= ? LIMIT ?"#,
                                     start_time, error_num).fetch(&mut conn);

            let mut entry = DownloadEntryError {
                etype: 0x00,
                start_ts: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                end_ts: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
            };
            while let Ok(Some(row)) = s.try_next().await {
                entry.etype = (row.fault_type & 0x0000000F) as u8;
                if let Some(timestamp) = row.fault_start {
                    bcd_timestamp(&mut entry.start_ts, timestamp)?;
                }
                if let Some(timestamp) = row.fault_end {
                    bcd_timestamp(&mut entry.end_ts, timestamp)?;
                }
                let bentry = bincode::serialize(&entry)?;
                self.writer.write_all(&bentry).await?;
            }
        }

        {
            let mut conn = self.db_connect().await?; 

            let row = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM event WHERE event_start >= ?"#, start_time).fetch_one(&mut conn).await?;
            event_num = row.cnt as u8;
            self.writer.write(&event_num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"select event_type, event_start, event_end from event WHERE event_start >= ? LIMIT ?"#,
                                     start_time, event_num).fetch(&mut conn);

            let mut entry = DownloadEntryEvent {
                etype: 0x00,
                start_ts: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                end_ts: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                related: 0x00, //TODO
            };
            while let Ok(Some(row)) = s.try_next().await {
                entry.etype = (row.event_type & 0x0000000F) as u8;
                if let Some(timestamp) = row.event_start {
                    bcd_timestamp(&mut entry.start_ts, timestamp)?;
                }
                if let Some(timestamp) = row.event_end {
                    bcd_timestamp(&mut entry.end_ts, timestamp)?;
                }

                let bentry = bincode::serialize(&entry)?;
                self.writer.write_all(&bentry).await?;
            }
        }
        {
            let curr_pos = self.writer.seek(SeekFrom::Current(0)).await?;
            self.writer.seek(SeekFrom::Start(orig_pos)).await?;

            let block: DownloadBlockMeta = DownloadBlockMeta {
                code: 0x00,
                name: String::from("事件故障資料").as_bytes().try_into().unwrap(),
                len: (mem::size_of::<DownloadEntryEvent>() as u32 * event_num as u32)
                    + (mem::size_of::<DownloadEntryError>() as u32 * error_num as u32),
            };
            let block = bincode::serialize(&block)?;
            self.writer.write_all(&block).await?;
            self.writer.seek(SeekFrom::Start(curr_pos)).await?;
        }

        Ok(())
    }

    async fn download_velocity(&mut self) -> Result<()> {
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        let start_time = self.start_time.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));

        let mut block: DownloadBlockGeneric = DownloadBlockGeneric {
            code: 0x01,
            name: *b"velocity-informati",
            len: 0x00000000,
            num: 0x0000,
        };

        {
            let seek_it = SeekFrom::Current(mem::size_of::<DownloadBlockMeta>().try_into().unwrap());
            self.writer.seek(seek_it).await?;

            let mut conn = self.db_connect().await?;
            let mut s = sqlx::query_as!(SqlVelocity, r#"select speed,timestamp as "timestamp: _" from velocity WHERE timestamp >= ?"#,
                                        start_time).fetch(&mut conn);

            let mut prev_timestamp = NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0);
            let mut index: usize = 0;
            let mut entry = DownloadEntryVelocity {
                date_time: [0; 7],
                speed: [0; 120],
            };
            while let Ok(Some(row)) = s.try_next().await {
                /*force_cnt -= 1;
                  if force_cnt == 0 {
                  break;
                  }*/

                let SqlVelocity {
                    speed,
                    timestamp,
                } = row;
                match (speed, timestamp) {
                    (Some(speed), Some(timestamp)) => {
                        if index != 0 {
                            let diff = timestamp.signed_duration_since(prev_timestamp);

                            if diff.num_seconds() > 1 {
                                let lost = (diff.num_milliseconds() as f32 / 550.0).round() as usize;
                                //debug!("[debug] {:?} - {:?} = {:?} => {} {}", timestamp, prev_timestamp, diff, lost, index);

                                if lost >= (120 - index) {
                                    let entryb = bincode::serialize(&entry)?;
                                    self.writer.write_all(&entryb).await?;

                                    index = 0;
                                    entry = DownloadEntryVelocity {
                                        date_time: *b"\x19\x70\x01\x01\x00\x00\x00",
                                        speed: [0; 120],
                                    };
                                }
                                else {
                                    index += lost;
                                }
                            }
                        }
                        if index == 0 {
                            debug!("{:?} {:?}", speed, timestamp);
                            block.num += 1;
                            bcd_timestamp(&mut entry.date_time, timestamp)?;
                        }
                        prev_timestamp = timestamp;
                        entry.speed[index] = speed as u8;
                        index += 1;
                    },
                    (Some(speed), None) => {
                        entry.speed[index] = speed as u8;
                        index += 1;
                    },
                    _ => {
                        warn!("{:?} {:?}", speed, timestamp);
                    },
                }
                if index == 120 {
                    let entryb = bincode::serialize(&entry)?;
                    self.writer.write_all(&entryb).await?;

                    index = 0;
                    entry = DownloadEntryVelocity {
                        date_time: [0; 7],
                        speed: [0; 120],
                    };
                }
            }
            // flush non-full pending entry
            if index != 0 {
                let entryb = bincode::serialize(&entry)?;
                self.writer.write_all(&entryb).await?;
            }
        }

        {
            let curr_pos = self.writer.seek(SeekFrom::Current(0)).await?;
            self.writer.seek(SeekFrom::Start(orig_pos)).await?;

            block.len = 2/*block.num*/ + (block.num as u32) * (mem::size_of::<DownloadEntryVelocity>()) as u32;
            let block= bincode::serialize(&block)?;
            self.writer.write_all(&block).await?;

            self.writer.seek(SeekFrom::Start(curr_pos)).await?;
            Ok(())
        }
    }

    async fn download_vu_calibration(&mut self) -> Result<u32> {
        let start_time = self.start_time.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut conn = self.db_connect().await?;
            let num = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM calibration WHERE timestamp_before >= ?"#, start_time)
                .fetch_one(&mut conn).await?.cnt;
            self.writer.write(&num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT user, odo_before, odo_after, timestamp_before, timestamp_after FROM calibration WHERE timestamp_before >= ?"#,
                                     start_time).fetch(&mut conn);
            while let Ok(Some(row)) = s.try_next().await {
                debug!("[debug][calibration] {:?}", row);

                let mut entry = DownloadBlockCalibration {
                    user: [0; 36],
                    odo_before: [0; 4], // TODO
                    odo_after: [0; 4], // TODO
                    timestamp_before: [0; 7],
                    timestamp_after: [0; 7],
                };

                if let Some(ts) = row.timestamp_before {
                    bcd_timestamp(&mut entry.timestamp_before, ts)?;
                }
                if let Some(ts) = row.timestamp_after {
                    bcd_timestamp(&mut entry.timestamp_after, ts)?;
                }

                if let Some(odo) = row.odo_before {
                    let mut odo = (odo / 0.1) as u32;
                    entry.odo_before[0] = bcd((odo / 1_000_000) as u8);
                    odo %= 1_000_000;
                    entry.odo_before[1] = bcd((odo / 10000) as u8);
                    odo %= 10000;
                    entry.odo_before[2] = bcd((odo / 100) as u8);
                    odo %= 100;
                    entry.odo_before[3] = bcd(odo as u8);
                }

                let odo = row.odo_after;
                let mut odo = (odo / 0.1) as u32;
                entry.odo_after[0] = bcd((odo / 1_000_000) as u8);
                odo %= 1_000_000;
                entry.odo_after[1] = bcd((odo / 10000) as u8);
                odo %= 10000;
                entry.odo_after[2] = bcd((odo / 100) as u8);
                odo %= 100;
                entry.odo_after[3] = bcd(odo as u8);

                copy_slice(&mut entry.user, row.user.as_bytes());

                let bentry = bincode::serialize(&entry)?;
                self.writer.write_all(&bentry).await?;
            }
        }

        //Ok(0)
        Ok((self.writer.seek(SeekFrom::Current(0)).await? - orig_pos) as u32)
    }

    async fn download_vu_timedate(&mut self) -> Result<u32> {
        let start_time = self.start_time.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut conn = self.db_connect().await?;
            let num = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM time_date WHERE timestamp_before >= ?"#, start_time)
                .fetch_one(&mut conn).await?.cnt;
            self.writer.write(&num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT timestamp_before AS "timestamp_before: NaiveDateTime", timestamp_after AS "timestamp_after: NaiveDateTime" FROM time_date WHERE timestamp_before >= ?"#, start_time)
                .fetch(&mut conn);
            while let Ok(Some(row)) = s.try_next().await {
                debug!("[debug][time-date] {:?}", row);

                let mut entry = DownloadBlockTimeDate {
                    timestamp_before: [0; 7],
                    timestamp_after: [0; 7],
                };

                if let Some(ts) = row.timestamp_before {
                    bcd_timestamp(&mut entry.timestamp_before, ts)?;
                }
                if let Some(ts) = row.timestamp_after {
                    bcd_timestamp(&mut entry.timestamp_after, ts)?;
                }
                let bentry = bincode::serialize(&entry)?;
                self.writer.write_all(&bentry).await?;
            }
        }

        Ok((self.writer.seek(SeekFrom::Current(0)).await? - orig_pos) as u32)
    }

    async fn download_vu_travel_threshold(&mut self) -> Result<u32> {
        let start_time = self.start_time.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut conn = self.db_connect().await?;
            let num = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM travel_threshold WHERE timestamp_before >= ?"#, start_time)
                .fetch_one(&mut conn).await?.cnt;
            self.writer.write(&num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT timestamp_before, timestamp_after, threshold_before, threshold_after FROM travel_threshold WHERE timestamp_before >= ?"#,
                                     start_time).fetch(&mut conn);
            while let Ok(Some(row)) = s.try_next().await {
                debug!("[debug][travel-threshold] {:?}", row);

                let mut entry = DownloadBlockTravelThreshold {
                    timestamp_before: [0; 7],
                    timestamp_after: [0; 7],
                    threshold_before: [0; 2],
                    threshold_after: [0; 2],
                };

                if let Some(ts) = row.timestamp_before {
                    bcd_timestamp(&mut entry.timestamp_before, ts)?;
                }
                if let Some(ts) = row.timestamp_after {
                    bcd_timestamp(&mut entry.timestamp_after, ts)?;
                }
                if let Some(mut thd) = row.threshold_before {
                    entry.threshold_before[0] = bcd(thd.trunc() as u8);
                    thd = thd.fract() * 100.0;
                    entry.threshold_before[1] = bcd(thd as u8);
                }
                let mut thd = row.threshold_after;
                entry.threshold_after[0] = bcd(thd.trunc() as u8);
                thd = thd.fract() * 100.0;
                entry.threshold_after[1] = bcd(thd as u8);

                let bentry = bincode::serialize(&entry)?;
                self.writer.write_all(&bentry).await?;
            }
        }

        Ok((self.writer.seek(SeekFrom::Current(0)).await? - orig_pos) as u32)
    }

    async fn download_vu_rest_threshold(&mut self) -> Result<u32> {
        let start_time = self.start_time.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut conn = self.db_connect().await?;
            let num = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM rest_threshold WHERE timestamp_before >= ?"#, start_time)
                .fetch_one(&mut conn).await?.cnt;
            self.writer.write(&num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT timestamp_before, timestamp_after, threshold_before, threshold_after FROM rest_threshold WHERE timestamp_before >= ?"#,
                                     start_time).fetch(&mut conn);
            while let Ok(Some(row)) = s.try_next().await {
                debug!("[debug][rest-threshold] {:?}", row);

                let mut entry = DownloadBlockTravelThreshold {
                    timestamp_before: [0; 7],
                    timestamp_after: [0; 7],
                    threshold_before: [0; 2],
                    threshold_after: [0; 2],
                };

                if let Some(ts) = row.timestamp_before {
                    bcd_timestamp(&mut entry.timestamp_before, ts)?;
                }
                if let Some(ts) = row.timestamp_after {
                    bcd_timestamp(&mut entry.timestamp_after, ts)?;
                }
                if let Some(mut thd) = row.threshold_before {
                    entry.threshold_before[0] = bcd(thd.trunc() as u8);
                    thd = thd.fract() * 100.0;
                    entry.threshold_before[1] = bcd(thd as u8);
                }
                let mut thd = row.threshold_after;
                entry.threshold_after[0] = bcd(thd.trunc() as u8);
                thd = thd.fract() * 100.0;
                entry.threshold_after[1] = bcd(thd as u8);

                let bentry = bincode::serialize(&entry)?;
                self.writer.write_all(&bentry).await?;
            }
        }

        Ok((self.writer.seek(SeekFrom::Current(0)).await? - orig_pos) as u32)
    }

    async fn download_vu_factory(&mut self) -> Result<u32> {
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut entry: DownloadBlockFactory = DownloadBlockFactory {
                manufacture_name: [0; 36],
                manufacture_address: [0; 36],
                serial_number: [0; 8],
                sw_version: [0; 4],
                install_timestamp: [0; 7],
                manufacture_date: [0; 4],
                certification_number: [0; 8],
                car_number: [0; 17],
            };

            let mut conn = self.db_connect().await?;
            if let Ok(row) = sqlx::query!(r#"SELECT manufacture_name, manufacture_address, serial_number, sw_version, install_timestamp, manufacture_date as "manufacture_date: NaiveDate", certification_number, car_number FROM vehicle_unit"#).fetch_one(&mut conn).await {
                copy_slice(&mut entry.manufacture_name, row.manufacture_name.as_bytes());
                copy_slice(&mut entry.manufacture_address, row.manufacture_address.as_bytes());
                copy_slice(&mut entry.serial_number, row.serial_number.as_bytes());
                copy_slice(&mut entry.sw_version, row.sw_version.as_bytes());
                copy_slice(&mut entry.certification_number, row.certification_number.as_bytes());

                if let Some(car_number) = row.car_number {
                    copy_slice(&mut entry.car_number, car_number.as_bytes());
                }

                if let Some(ts) = row.install_timestamp {
                    bcd_timestamp(&mut entry.install_timestamp, ts)?;
                }
                if let Some(ts) = row.manufacture_date {
                    entry.manufacture_date[0] = bcd((ts.year() / 100) as u8);
                    entry.manufacture_date[1] = bcd((ts.year() % 100) as u8);
                    entry.manufacture_date[2] = bcd(ts.month() as u8);
                    entry.manufacture_date[3] = bcd(ts.day() as u8);
                }
            }
            let bentry = bincode::serialize(&entry)?;
            self.writer.write_all(&bentry).await?;
        }

        Ok((self.writer.seek(SeekFrom::Current(0)).await? - orig_pos) as u32)
    }

    async fn download_vu(&mut self) -> Result<()> {
        //Err(anyhow!("not implemented"))
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;

        let seek_it = SeekFrom::Current(mem::size_of::<DownloadBlockMeta>().try_into().unwrap());
        self.writer.seek(seek_it).await?;

        let f_len = self.download_vu_factory().await?;
        let clen = self.download_vu_calibration().await?;
        let tlen = self.download_vu_timedate().await?;
        let tt_len = self.download_vu_travel_threshold().await?;
        let rt_len = self.download_vu_rest_threshold().await?;

        //self.download_vu_meta().await?;
        {
            let curr_pos = self.writer.seek(SeekFrom::Current(0)).await?;
            self.writer.seek(SeekFrom::Start(orig_pos)).await?;

            let block: DownloadBlockMeta = DownloadBlockMeta {
                code: 0x02,
                name: String::from("技術資料格式").as_bytes().try_into().unwrap(),
                len: clen + tlen + tt_len + rt_len + f_len,
            };
            let block = bincode::serialize(&block)?;
            self.writer.write_all(&block).await?;
            self.writer.seek(SeekFrom::Start(curr_pos)).await?;
        }

        Ok(())
    }

    async fn download_driver(&mut self) -> Result<()> {
        let start_time = self.start_time.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        let driver_num;

        let seek_it = SeekFrom::Current(mem::size_of::<DownloadBlockMeta>().try_into().unwrap());
        self.writer.seek(seek_it).await?;

        {
            let mut conn = self.db_connect().await?;

            let row = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM driver WHERE timestamp >= ?"#, start_time).fetch_one(&mut conn).await?;
            driver_num = row.cnt as u8;
            self.writer.write(&driver_num.to_le_bytes()).await?;

            let mut entry: DownloadEntryDriver = DownloadEntryDriver {
                account: [0; 18],
                action: 0,
                dtime: [0; 7],
            };

            let mut s = sqlx::query!(r#"SELECT account, action, timestamp FROM driver WHERE timestamp >= ?"#,
                                     start_time).fetch(&mut conn);
            while let Ok(Some(row)) = s.try_next().await {
                debug!("[debug][driver] {:?}", row);

                if let Some(ts) = row.timestamp {
                    bcd_timestamp(&mut entry.dtime, ts)?;

                    copy_slice(&mut entry.account, row.account.as_bytes());

                    entry.action = row.action as u8;
                    let bentry = bincode::serialize(&entry)?;
                    self.writer.write_all(&bentry).await?;
                }
            }
        }

        {
            let curr_pos = self.writer.seek(SeekFrom::Current(0)).await?;
            self.writer.seek(SeekFrom::Start(orig_pos)).await?;

            let block: DownloadBlockMeta = DownloadBlockMeta {
                code: 0x03,
                name: String::from("駕駛活動類型").as_bytes().try_into().unwrap(),
                len: (mem::size_of::<DownloadEntryDriver>() as u32 * driver_num as u32),
            };
            let block = bincode::serialize(&block)?;
            self.writer.write_all(&block).await?;
            self.writer.seek(SeekFrom::Start(curr_pos)).await?;
        }

        Ok(())
    }

    async fn download_location(&mut self) -> Result<()> {
        let start_time = self.start_time.unwrap_or_else(|| NaiveDateTime::from_timestamp(0, 0));
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        let mut block: DownloadBlockGeneric = DownloadBlockGeneric {
            code: 0x04,
            name: String::from("定位資料點點").as_bytes().try_into().unwrap(),
            len: 0x00,
            num: 0x00,
        };

        let seek_it = SeekFrom::Current(mem::size_of::<DownloadBlockGeneric>().try_into().unwrap());
        self.writer.seek(seek_it).await?;

        {
            let mut conn = self.db_connect().await?;

            let mut entry: DownloadEntryLocationHour = DownloadEntryLocationHour {
                start: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                hour: [ DownloadEntryLocationPer {
                    logitude: 0.0,
                    latitude: 0.0,
                    altitude: 0x00,
                    speed: 0,
                }; 60],
            };
            let mut minutes = 0;

            let mut s = sqlx::query!(r#"SELECT timestamp, AVG(logitude) AS logi, AVG(latitude) AS lati, AVG(altitude) AS alti, AVG(speed) AS spd FROM location GROUP BY timestamp"#)
                .fetch(&mut conn);
            while let Ok(Some(row)) = s.try_next().await {
                debug!("[debug][location] {:?}", row);

                match (row.timestamp, row.logi, row.lati, row.alti, row.spd) {
                    (Some(ts), Some(logi), Some(lati), Some(alti), Some(spd)) => {
                        let ts = NaiveDateTime::parse_from_str(&ts, "%Y-%m-%d %H:%M")?;
                        if ts.timestamp() < start_time.timestamp() {
                            continue;
                        }

                        if minutes == 0 {
                            bcd_timestamp(&mut entry.start, ts)?;
                        }

                        /* TODO convert to meter from degree */
                        entry.hour[minutes].logitude = logi as f32 * 111.12;
                        entry.hour[minutes].latitude = lati as f32 * 111.12;
                        entry.hour[minutes].altitude = (alti as f32 * 0.01) as u16;
                        entry.hour[minutes].speed = spd as u8;

                        minutes += 1;

                        if minutes == 60 {
                            let bentry = bincode::serialize(&entry)?;
                            self.writer.write_all(&bentry).await?;

                            block.num += 1;

                            entry = DownloadEntryLocationHour {
                                start: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                                hour: [ DownloadEntryLocationPer {
                                    logitude: 0.0,
                                    latitude: 0.0,
                                    altitude: 0x00,
                                    speed: 0,
                                }; 60],
                            };
                            minutes = 0;
                        }
                    },
                    _ => {
                        continue;
                    }
                }
            }
            if minutes != 0 {
                let bentry = bincode::serialize(&entry)?;
                self.writer.write_all(&bentry).await?;
                block.num += 1;
            }

        }

        {
            let curr_pos = self.writer.seek(SeekFrom::Current(0)).await?;
            self.writer.seek(SeekFrom::Start(orig_pos)).await?;

            block.len = mem::size_of::<DownloadEntryLocationHour>() as u32 * block.num as u32;
            let block = bincode::serialize(&block)?;
            self.writer.write_all(&block).await?;
            self.writer.seek(SeekFrom::Start(curr_pos)).await?;
        }

        Ok(())
    }

    pub async fn download(&mut self) -> Result<()> {
        let _start_pos = self.writer.seek(SeekFrom::Current(0)).await?;

        self.header(0x05).await?;
        self.download_event().await?;
        self.download_velocity().await?;
        self.download_vu().await?;
        self.download_driver().await?;
        self.download_location().await?;
        self.writer.flush().await?;

        let _end_pos = self.writer.seek(SeekFrom::Current(0)).await?;

        self.tail(0xFA).await?;
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn check(&mut self, _block_item: u8) -> Result<u8> {
        fn parity_bits(data: &[u8]) -> u8 {
            let mut parity = 0;
            for b in data {
                parity ^= *b;
            }
            parity
        }

        //Err(anyhow!("not implemented"))
        let file = File::open(&self.dst).await?;
        let mut reader = BufReader::new(file);
        // read all file into BigArray
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        // check parity bits
        let parity = parity_bits(&buf[0..0x10]);
        Ok(parity)
    }
}

#[tokio::test]
async fn test_fb161_oo() {
    let mut fb161 = Fb161Downloader::new(None, None, "./test_fb161_oo.vdr".to_string()).await;
    fb161.download().await.unwrap();
    assert!(fb161.check(5).await.is_ok());
}

pub fn copy_slice<'a>(a: &'a mut [u8], s: &[u8]) -> &'a[u8] {
    let sn = s.len();
    let an = a.len();
    if an > sn {
        a[..sn].copy_from_slice(s);
    }
    else {
        a[..an].copy_from_slice(&s[..an]);
    }
    a
}

#[tokio::test]
async fn test_copy_slice() {
    let mut a = [0u8; 5];
    let s = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let r = copy_slice(&mut a, &s[1..3]);
    assert_eq!(r, [2u8, 3, 0, 0, 0]);

    let mut b = [5u8; 11];
    let r = copy_slice(&mut b, &s);
    assert_eq!(r, [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 5u8]);
}

pub fn bcd_timestamp(buff: &mut [u8; 7], ts: NaiveDateTime) -> Result<()> {
    if buff.len() >= 7 {
        buff[0] = bcd((ts.year() / 100) as u8);
        buff[1] = bcd((ts.year() % 100) as u8);
        buff[2] = bcd(ts.month() as u8);
        buff[3] = bcd(ts.day() as u8);
        buff[4] = bcd(ts.hour() as u8);
        buff[5] = bcd(ts.minute() as u8);
        buff[6] = bcd(ts.second() as u8);

        Ok(())
    }
    else {
        Err(anyhow!("current seek error"))
    }
}

pub fn bcd(n: u8) -> u8 {
    let mut r = n % 10;
    let q = n / 10;
    r += q * 16;
    r
}

