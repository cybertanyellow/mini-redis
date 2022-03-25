use tracing::{debug, error, info};
use std::env;
use bytes::Bytes;
use sqlx::sqlite::{*, SqlitePool};
use sqlx::pool::PoolConnection;
use sqlx::types::chrono::{DateTime, Utc, NaiveDate};
use serde::{Deserialize, Serialize};
use chrono::serde::{/*ts_milliseconds, */ts_seconds};

#[derive(Debug)]
pub(crate) struct Flatbread {
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
    speed_avg: f32,
}

impl Flatbread {
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

        Flatbread {
            odo,
            velocity_id,
            location_id,
            driver_id,
            conn,
        }
    }

    pub(crate) async fn insert_velocity(&mut self, value: Option<Bytes>) -> Result<i64, sqlx::Error> {
        if let Some(value) = value {
            self.update_velocity(&value).await
        }
        else {
            let velocity = CacheVelocity {
                speed: 0.0,
                odo: None,
                timestamp: None,
            };

            match sqlx::query!(r#"INSERT INTO velocity ( speed, odo, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4 )"#,
            velocity.speed, self.odo, self.location_id, self.driver_id).execute(&mut self.conn).await {
                Ok(r) => {
                    self.velocity_id = Some(r.last_insert_rowid());
                    info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                    Ok(r.last_insert_rowid())
                }
                Err(e) => {
                    error!("INSERT velocity with {:?} fail {}", velocity, e);
                    Err(e)
                }
            }
        }
        // TODO, SPEC 5.1.7.1 & 5.2.4, if moving(3km/hr, >5s),
        // current driver action is driving,
        // other driver(s) action is standby
    }

    pub(crate) async fn insert_location(&mut self, value: Option<Bytes>) -> Result<i64, sqlx::Error> {
        let mut location = CacheLocation {
            logitude: 0.0,
            latitude: 0.0,
            altitude: 0.0,
            speed_avg: 0.0,
        };
        if let Some(value) = value {
            if let Ok(json) = serde_json::from_slice(&value) {
                location = json;
            }
        }
        match sqlx::query!(r#"INSERT INTO location (logitude, latitude, altitude, speed_avg) VALUES (?1, ?2, ?3, ?4)"#,
        location.logitude, location.latitude, location.altitude, location.speed_avg).execute(&mut self.conn).await {
            Ok(r) => {
                self.location_id = Some(r.last_insert_rowid());
                info!("INSERT location with {:?} OK {:?}", location, self.location_id);
                Ok(r.last_insert_rowid())
            }
            Err(e) => {
                error!("INSERT location with {:?} fail {}", location, e);
                Err(e)
            }
        }
    }
    pub(crate) async fn update_driver(&mut self, value: &Bytes) -> Result<i64, sqlx::Error> {
        let driver: CacheDriver = serde_json::from_slice(value).unwrap();
        //println!("driver {:?}", driver);

        match sqlx::query!(r#"INSERT INTO driver ( account, action ) VALUES ( ?1, ?2 )"#,
        driver.account, driver.action).execute(&mut self.conn).await {
            Ok(r) => {
                let rowid = r.last_insert_rowid();
                debug!("INSERT driver with {:?} OK {:?}", driver, rowid);
                if driver.action == 1 || driver.action == 2 {
                    self.driver_id = Some(rowid);
                }
                Ok(rowid)
            },
            Err(e) => {
                error!("INSERT driver with {:?} fail {}", driver, e);
                Err(e)
            },
        }
    }

    pub(crate) async fn update_calibration(&mut self, value: &Bytes) -> Result<i64, sqlx::Error> {
        /* TODO change odo/mileage */
        let cal: CacheCalibration = serde_json::from_slice(value).unwrap();
        println!("calibration {:?}", cal);

        match sqlx::query!(r#"INSERT INTO calibration ( user, odo_after, odo_unit ) VALUES ( ?1, ?2, ?3 )"#,
        cal.user, cal.odo_after, cal.odo_unit).execute(&mut self.conn).await {
            Ok(r) => {
                let rowid = r.last_insert_rowid();
                debug!("INSERT calibration with {:?} OK {:?}", cal, rowid);
                Ok(rowid)
            },
            Err(e) => {
                error!("INSERT calibration with {:?} fail {}", cal, e);
                Err(e)
            },
        }
    }

    pub(crate) async fn update_time_date(&mut self, value: &Bytes) -> Result<i64, sqlx::Error> {
        /* TODO change system time */
        let tm: CacheTimeDate = serde_json::from_slice(value).unwrap();
        //println!("date-time {:?}", tm);
        match (tm.timestamp_before, tm.timestamp_after) {
            (Some(before), Some(after)) => {
                match sqlx::query!(r#"INSERT INTO time_date ( timestamp_before, timestamp_after ) VALUES ( ?1, ?2 )"#,
                before, after).execute(&mut self.conn).await {
                    Ok(r) => {
                        let rowid = r.last_insert_rowid();
                        info!("INSERT date-time {:?} OK ID-{}", tm, rowid);
                        Ok(rowid)
                    },
                    Err(e) => {
                        error!("INSERT date-time {:?} FAILED {:?}", tm, e); /*TODO*/
                        Err(e)
                    },
                }
            }
            (Some(before), None) => {
                match sqlx::query!(r#"INSERT INTO time_date ( timestamp_before ) VALUES ( ?1 )"#,
                before).execute(&mut self.conn).await {
                    Ok(r) => {
                        let rowid = r.last_insert_rowid();
                        info!("INSERT date-time {:?} OK ID-{}", tm, rowid);
                        Ok(rowid)
                    },
                    Err(e) => {
                        error!("INSERT date-time {:?} FAILED {:?}", tm, e); /*TODO*/
                        Err(e)
                    },
                }
            }
            (None, Some(after)) => {
                match sqlx::query!(r#"UPDATE time_date SET timestamp_after = ?1 WHERE timestamp_after IS NULL"#,
                                   after).execute(&mut self.conn).await {
                    Ok(r) => {
                        let rowid = r.last_insert_rowid();
                        info!("UPDATE date-time {:?} OK ID-{}", tm, rowid);
                        Ok(rowid)
                    },
                    Err(e) => {
                        error!("UPDATE date-time {:?} FAILED {:?}", tm, e); /*TODO*/
                        Err(e)
                    },
                }
            }
            (None, None) => {
                match sqlx::query!(r#"INSERT INTO time_date ( timestamp_after ) VALUES ( NULL )"#).execute(&mut self.conn).await {
                    Ok(r) => {
                        let rowid = r.last_insert_rowid();
                        info!("INSERT date-time {:?} OK ID-{}", tm, rowid);
                        Ok(rowid)
                    },
                    Err(e) => {
                        error!("INSERT date-time {:?} FAILED {:?}", tm, e); /*TODO*/
                        Err(e)
                    },
                }
            }
        }

    }

    pub(crate) async fn update_event(&mut self, value: &Bytes) -> Result<i64, sqlx::Error> {
        if let Ok(event) = serde_json::from_slice::<CacheEvent>(value) {
            debug!("event {:?}", event);

            if event.event_end.is_some() {
                match sqlx::query!(r#"UPDATE event SET event_end = ?2 WHERE event_type = ?1"#,
                                   event.event_type, event.event_end).execute(&mut self.conn).await {
                    Ok(r) => {
                        let rowid = r.last_insert_rowid();
                        info!("UPDATE event {:?} OK by id-{}", event, rowid);
                        Ok(rowid)
                    },
                    Err(e) => {
                        error!("UPDATE event {:?} FAILED {:?}", event, e); /*TODO*/
                        Err(e)
                    },
                }
            } else {
                match sqlx::query!(r#"INSERT INTO event (event_type, velocity_id) VALUES ( ?1, ?2)"#,
                event.event_type, self.velocity_id).execute(&mut self.conn).await {
                    Ok(r) => {
                        let rowid = r.last_insert_rowid();
                        debug!("INSERT event - {:?} OK by id-{}", event, rowid);
                        Ok(rowid)
                    },
                    Err(e) => {
                        error!("INSERT event error-{:?}", e);
                        Err(e)
                    },
                }
            }
        }
        else {
            error!("event format invalid - {:?}", value);
            Ok(-1) /*TODO*/
        }
    }

    pub(crate) async fn update_error(&mut self, value: &Bytes) -> Result<i64, sqlx::Error> {
        if let Ok(error) = serde_json::from_slice::<CacheError>(value) {
            match (error.fault_start, error.fault_end) {
                (Some(fault_start), Some(fault_end)) => {
                    match sqlx::query!(r#"INSERT INTO error ( fault_type, fault_start, fault_end, velocity_id ) VALUES ( ?1, ?2, ?3, ?4)"#,
                    error.fault_type, fault_start, fault_end, self.velocity_id).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", error, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", error, e); /*TODO*/
                            Err(e)
                        },
                    }
                }
                (Some(fault_start), None) => {
                    match sqlx::query!(r#"INSERT INTO error ( fault_type, fault_start, velocity_id ) VALUES ( ?1, ?2, ?3 )"#,
                    error.fault_type, fault_start, self.velocity_id).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", error, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", error, e); /*TODO*/
                            Err(e)
                        },
                    }
                }
                (None, Some(fault_end)) => {
                    match sqlx::query!(r#"UPDATE error SET fault_end = ?2 WHERE fault_type = ?1 AND fault_end IS NULL"#,
                                       error.fault_type, fault_end).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", error, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", error, e); /*TODO*/
                            Err(e)
                        },
                    }
                }
                (None, None) => {
                    match sqlx::query!(r#"INSERT INTO error ( velocity_id ) VALUES ( ?1 )"#, self.velocity_id)
                        .execute(&mut self.conn).await {
                            Ok(r) => {
                                let rowid = r.last_insert_rowid();
                                info!("INSERT error {:?} OK by rid-{}", error, rowid);
                                Ok(rowid)
                            },
                            Err(e) => {
                                error!("INSERT error {:?} FAILED {:?}", error, e); /*TODO*/
                                Err(e)
                            },
                        }
                }
            }
        }
        else {
            error!("error format invalid - {:?}", value);
            Ok(-1) /*TODO*/
        }
    }

    pub(crate) async fn update_travel_threshold(&mut self, value: &Bytes) -> Result<i64, sqlx::Error> {
        if let Ok(thresh) = serde_json::from_slice::<CacheTravelThreshold>(value) {
            match (thresh.timestamp_before, thresh.timestamp_after, thresh.threshold_before) {
                (Some(ts_before), Some(ts_after), Some(before)) => {
                    match sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_before, timestamp_after, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3, ?4)"#,
                    ts_before, ts_after, before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (Some(ts_before), Some(ts_after), None) => {
                    match sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_before, timestamp_after, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                    ts_before, ts_after, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (Some(ts_before), None, Some(before)) => {
                    match sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_before, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                    ts_before, before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (None, Some(ts_after), Some(before)) => {
                    match sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_after, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                    ts_after, before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                ///////////// two NONE ///////////
                (Some(ts_before), None, None) => {
                    match sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_before, threshold_after ) VALUES ( ?1, ?2 )"#,
                    ts_before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (None, None, Some(before)) => {
                    match sqlx::query!(r#"INSERT INTO travel_threshold ( threshold_before, threshold_after ) VALUES ( ?1, ?2 )"#,
                    before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (None, Some(ts_after), None) => {
                    match sqlx::query!(r#"INSERT INTO travel_threshold ( timestamp_after, threshold_after ) VALUES ( ?1, ?2 )"#,
                    ts_after, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (None, None, None) => {
                    match sqlx::query!(r#"INSERT INTO travel_threshold ( threshold_after ) VALUES ( ?1 )"#,
                    thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
            }
        }
        else {
            error!("travel-threshold format invalid - {:?}", value);
            Ok(-1) /*TODO*/
        }
    }

    pub(crate) async fn update_rest_threshold(&mut self, value: &Bytes) -> Result<i64, sqlx::Error> {
        if let Ok(thresh) = serde_json::from_slice::<CacheRestThreshold>(value) {
            match (thresh.timestamp_before, thresh.timestamp_after, thresh.threshold_before) {
                (Some(ts_before), Some(ts_after), Some(before)) => {
                    match sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_before, timestamp_after, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3, ?4)"#,
                    ts_before, ts_after, before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (Some(ts_before), Some(ts_after), None) => {
                    match sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_before, timestamp_after, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                    ts_before, ts_after, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (Some(ts_before), None, Some(before)) => {
                    match sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_before, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                    ts_before, before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (None, Some(ts_after), Some(before)) => {
                    match sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_after, threshold_before, threshold_after ) VALUES ( ?1, ?2, ?3)"#,
                    ts_after, before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                ///////////// two NONE ///////////
                (Some(ts_before), None, None) => {
                    match sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_before, threshold_after ) VALUES ( ?1, ?2 )"#,
                    ts_before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (None, None, Some(before)) => {
                    match sqlx::query!(r#"INSERT INTO rest_threshold ( threshold_before, threshold_after ) VALUES ( ?1, ?2 )"#,
                    before, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (None, Some(ts_after), None) => {
                    match sqlx::query!(r#"INSERT INTO rest_threshold ( timestamp_after, threshold_after ) VALUES ( ?1, ?2 )"#,
                    ts_after, thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
                (None, None, None) => {
                    match sqlx::query!(r#"INSERT INTO rest_threshold ( threshold_after ) VALUES ( ?1 )"#,
                    thresh.threshold_after).execute(&mut self.conn).await {
                        Ok(r) => {
                            let rowid = r.last_insert_rowid();
                            info!("INSERT error {:?} OK by rid-{}", thresh, rowid);
                            Ok(rowid)
                        },
                        Err(e) => {
                            error!("INSERT error {:?} FAILED {:?}", thresh, e); /*TODO*/
                            Err(e)
                        },
                    }
                },
            }
        }
        else {
            error!("rest-threshold format invalid - {:?}", value);
            Ok(-1) /*TODO*/
        }
    }

    async fn update_velocity(&mut self, value: &Bytes) -> Result<i64, sqlx::Error> {
        if let Ok(velocity) = serde_json::from_slice::<CacheVelocity>(value) {
            match (velocity.odo, velocity.timestamp) {
                (Some(_odo), Some(_timestamp)) => {
                    self.odo = velocity.odo;
                    match sqlx::query!(r#"INSERT INTO velocity ( speed, odo, timestamp, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4, ?5 )"#,
                    velocity.speed, velocity.odo, velocity.timestamp, self.location_id, self.driver_id).execute(&mut self.conn).await {
                        Ok(r) => {
                            self.velocity_id = Some(r.last_insert_rowid());
                            info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                            Ok(r.last_insert_rowid())
                        }
                        Err(e) => {
                            error!("INSERT velocity with {:?} fail {}", velocity, e);
                            Err(e)
                        }
                    }
                },
                (Some(_odo), None) => {
                    self.odo = velocity.odo;
                    match sqlx::query!(r#"INSERT INTO velocity ( speed, odo, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4 )"#,
                    velocity.speed, velocity.odo, self.location_id, self.driver_id).execute(&mut self.conn).await {
                        Ok(r) => {
                            self.velocity_id = Some(r.last_insert_rowid());
                            info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                            Ok(r.last_insert_rowid())
                        }
                        Err(e) => {
                            error!("INSERT velocity with {:?} fail {}", velocity, e);
                            Err(e)
                        }
                    }
                },
                (None, Some(_timestamp)) => {
                    self.odo = self.odo.map(|x| x + velocity.speed / 7200.0);
                    println!("None/Some => odo {:?}", self.odo);
                    match sqlx::query!(r#"INSERT INTO velocity ( speed, odo, timestamp, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4, ?5 )"#,
                    velocity.speed, self.odo, velocity.timestamp, self.location_id, self.driver_id).execute(&mut self.conn).await {
                        Ok(r) => {
                            self.velocity_id = Some(r.last_insert_rowid());
                            info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                            Ok(r.last_insert_rowid())
                        }
                        Err(e) => {
                            error!("INSERT velocity with {:?} fail {}", velocity, e);
                            Err(e)
                        }
                    }
                }
                (None, None) => {
                    //self.odo = self.odo.map(|x| x + velocity.speed * 0.5);
                    self.odo = self.odo.map(|x| x + velocity.speed / 7200.0);
                    println!("None/None => odo {:?}", self.odo);
                    match sqlx::query!(r#"INSERT INTO velocity ( speed, odo, location_id, driver_id ) VALUES ( ?1, ?2, ?3, ?4 )"#,
                    velocity.speed, self.odo, self.location_id, self.driver_id).execute(&mut self.conn).await {
                        Ok(r) => {
                            self.velocity_id = Some(r.last_insert_rowid());
                            info!("INSERT velocity with {:?} OK {:?}", velocity, self.velocity_id);
                            Ok(r.last_insert_rowid())
                        }
                        Err(e) => {
                            error!("INSERT velocity with {:?} fail {}", velocity, e);
                            Err(e)
                        }
                    }
                }
            }
        }
        else {
            error!("velocity format invalid - {:?}", value);
            Ok(-1) /*TODO*/
        }
    }
}
