use crate::cmd::{Parse/*, ParseError*/};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
//use std::time::Duration;
use tracing::{debug, warn, instrument};
use tokio::time::{self/*, Duration, Instant*/};

/// Sensor `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on successful
/// SET operation.
///
/// # Options
///
/// Currently, the following options are supported:
///
/// * EX `seconds` -- Sensor the specified policy time, in seconds.
/// * PX `milliseconds` -- Sensor the specified policy time, in milliseconds.
#[derive(Debug, Clone)]
pub struct Sensor {
    /// the lookup key
    key: String,

    /// the value to be stored
    value: Bytes,

    /// TODO, enum better?
    policy: Option<String>,
}

impl Sensor {
    /// Create a new `Sensor` command which sets `key` to `value`.
    ///
    /// If `policy` is `Some`, the value should policy after the specified
    /// duration.
    pub fn new(key: impl ToString, value: Bytes, policy: Option<String>) -> Sensor {
        Sensor {
            key: key.to_string(),
            value,
            policy,
        }
    }

    /// Get the key
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Get the value
    pub fn value(&self) -> &Bytes {
        &self.value
    }

    /// Get the policy
    /*pub fn policy(&self) -> Option<String> {
        self.policy.as_ref()
    }*/

    /// Parse a `Sensor` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SET` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Sensor` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least 3 entries.
    ///
    /// ```text
    /// SET key value [EX seconds|PX milliseconds]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sensor> {
        //use ParseError::EndOfStream;

        // Read the key to set. This is a required field
        let key = parse.next_string()?;

        // Read the value to set. This is a required field.
        let value = parse.next_bytes()?;

        // The expiration is optional. If nothing else follows, then it is
        // `None`.
        /*let mut policy = None;
        if let Ok(p) = parse.next_string() {
            policy = Some(p);
        }*/
        let policy = parse.next_string().ok();

        // Attempt to parse another string.
        /*match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // An expiration is specified in seconds. The next value is an
                // integer.
                let secs = parse.next_int()?;
                policy = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // An expiration is specified in milliseconds. The next value is
                // an integer.
                let ms = parse.next_int()?;
                policy = Some(Duration::from_millis(ms));
            }
            // Currently, flatbread does not support any of the other SET
            // options. An error here results in the connection being
            // terminated. Other connections will continue to operate normally.
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            // The `EndOfStream` error indicates there is no further data to
            // parse. In this case, it is a normal run time situation and
            // indicates there are no specified `SET` options.
            Err(EndOfStream) => {}
            // All other errors are bubbled up, resulting in the connection
            // being terminated.
            Err(err) => return Err(err.into()),
        }*/

        Ok(Sensor { key, value, policy })
    }

    /// Apply the `Sensor` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Sensor the value in the shared database state.
        /*db.set(self.key, self.value, self.policy);
         */
        match self.key.as_str() {
            "velocity" => {
                tokio::spawn(fb161_sensor_velocity_task(db.clone(), self.clone()));
            },
            "location" => {
                tokio::spawn(fb161_sensor_location_task(db.clone(), self.clone()));
            },
            _ => {
                warn!("unknown sensor key: {}", self.key);
            }
        }

        // Create a success response and write it to `dst`.
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Sensor` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("sensor".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        if let Some(policy) = self.policy {
            frame.push_bulk(Bytes::from(policy.into_bytes()));
        }
        frame
    }

}

async fn fb161_sensor_velocity_task(db: Db, s: Sensor) {
    //println!("TODO key-{:?} value-{:?} policy-{:?}", s.key, s.value, s.policy);
    let mut speed = 0.0;
    let mut odo = 100.0;

    let mut interval_100ms = time::interval(time::Duration::from_millis(100));
    /*TODO graceful shutdown?? */
    loop {
        tokio::select! {
            _ = interval_100ms.tick() => {
                odo += speed / 36000.0;
                let sspeed = format!(r#"{{"speed":{},"odo":{}}}"#, speed, odo);
                db.set(s.key.clone(), sspeed.into(), None);
                speed += 0.1;
                if speed > 200.0 {
                    speed = 0.0;
                }
            }
        }
    }
}

async fn fb161_sensor_location_task(db: Db, s: Sensor) {
    println!("TODO key-{:?} value-{:?} policy-{:?}", s.key, s.value, s.policy);
    let mut logitude = 0.0;
    let mut latitude = 0.0;
    let mut altitude = 0.0;
    let mut speed_avg = 0.0;

    let mut interval_1s = time::interval(time::Duration::from_millis(1000));
    /*TODO graceful shutdown?? */
    loop {
        tokio::select! {
            _ = interval_1s.tick() => {
                let cmd = format!(r#"{{"logitude":{},"latitude":{},"altitude":{},"speed_avg":{}}}"#,
                                     logitude, latitude, altitude, speed_avg);
                db.set(s.key.clone(), cmd.into(), None);
                logitude += 11.1;
                latitude += 22.2;
                altitude += 33.3;
                speed_avg += 11.1;
            }
        }
    }
}
