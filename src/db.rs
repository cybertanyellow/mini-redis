use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use tracing::{debug, error};
use tokio::sync::mpsc;
use std::env;
use sqlx::sqlite::SqlitePool;
use sqlx::types::chrono::{DateTime, Utc, NaiveDate};
use std::str;
use serde::{Deserialize, Serialize};
//use chrono::prelude::*;
use chrono::serde::{/*ts_milliseconds, */ts_seconds};

/// A wrapper around a `Db` instance. This exists to allow orderly cleanup
/// of the `Db` by signalling the background purge task to shut down when
/// this struct is dropped.
#[derive(Debug)]
pub(crate) struct DbDropGuard {
    /// The `Db` instance that will be shut down when this `DbHolder` struct
    /// is dropped.
    db: Db,
}

/// Server state shared across all connections.
///
/// `Db` contains a `HashMap` storing the key/value data and all
/// `broadcast::Sender` values for active pub/sub channels.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
///
/// When a `Db` value is created, a background task is spawned. This task is
/// used to expire values after the requested duration has elapsed. The task
/// runs until all instances of `Db` are dropped, at which point the task
/// terminates.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// Handle to shared state. The background task will also have an
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    /// The shared state is guarded by a mutex. This is a `std::sync::Mutex` and
    /// not a Tokio mutex. This is because there are no asynchronous operations
    /// being performed while holding the mutex. Additionally, the critical
    /// sections are very small.
    ///
    /// A Tokio mutex is mostly intended to be used when locks need to be held
    /// across `.await` yield points. All other cases are **usually** best
    /// served by a std mutex. If the critical section does not include any
    /// async operations but is long (CPU intensive or performing blocking
    /// operations), then the entire operation, including waiting for the mutex,
    /// is considered a "blocking" operation and `tokio::task::spawn_blocking`
    /// should be used.
    state: Mutex<State>,

    /// Notifies the background task handling entry expiration. The background
    /// task waits on this to be notified, then checks for expired values or the
    /// shutdown signal.
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// The key-value data. We are not trying to do anything fancy so a
    /// `std::collections::HashMap` works fine.
    entries: HashMap<String, Entry>,

    /// The pub/sub key-space. Redis uses a **separate** key space for key-value
    /// and pub/sub. `flatbread` handles this by using a separate `HashMap`.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// flatbread: persists policy for period insertion.
    //policies: HashMap<String, Policy>,
    //policy_rx: mpsc::Receiver<()>,
    request: mpsc::Sender<(String, String, Bytes)>,

    /// Tracks key TTLs.
    ///
    /// A `BTreeMap` is used to maintain expirations sorted by when they expire.
    /// This allows the background task to iterate this map to find the value
    /// expiring next.
    ///
    /// While highly unlikely, it is possible for more than one expiration to be
    /// created for the same instant. Because of this, the `Instant` is
    /// insufficient for the key. A unique expiration identifier (`u64`) is used
    /// to break these ties.
    expirations: BTreeMap<(Instant, u64), String>,

    /// Identifier to use for the next expiration. Each expiration is associated
    /// with a unique identifier. See above for why.
    next_id: u64,

    /// True when the Db instance is shutting down. This happens when all `Db`
    /// values drop. Setting this to `true` signals to the background task to
    /// exit.
    shutdown: bool,
}

/// Entry in the key-value store
#[derive(Debug)]
struct Entry {
    /// Uniquely identifies this entry.
    id: u64,

    /// Stored data
    data: Bytes,

    /// Instant at which the entry expires and should be removed from the
    /// database.
    expires_at: Option<Instant>,
}

/*#[derive(Debug)]
struct Policy {
    /// Uniquely identifies this entry.
    ///id: u64,

    /// Stored data
    _data: Bytes,

    /// Duration at which period insertions should be made to the database.
    _period: Option<Duration>,
}*/

impl DbDropGuard {
    /// Create a new `DbHolder`, wrapping a `Db` instance. When this is dropped
    /// the `Db`'s purge task will be shut down.
    pub(crate) fn new() -> DbDropGuard {
        DbDropGuard { db: Db::new() }
    }

    /// Get the shared database. Internally, this is an
    /// `Arc`, so a clone only increments the ref count.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self) {
        // Signal the 'Db' instance to shut down the task that purges expired keys
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// Create a new, empty, `Db` instance. Allocates shared state and spawns a
    /// background task to manage key expiration.
    pub(crate) fn new() -> Db {
        let (tx, rx) = mpsc::channel(32);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                //policies: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
                //policy_rx: rx,
                request: tx,
            }),
            background_task: Notify::new(),
        });

        // Start the background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));
        tokio::spawn(period_policy_tasks(shared.clone(), rx));

        Db { shared }
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if there is no value associated with the key. This may be
    /// due to never having assigned a value to the key or a previously assigned
    /// value expired.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // Acquire the lock, get the entry and clone the value.
        //
        // Because data is stored using `Bytes`, a clone here is a shallow
        // clone. Data is not copied.
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Set the value associated with a key along with an optional expiration
    /// Duration.
    ///
    /// If a value is already associated with the key, it is removed.
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // Get and increment the next insertion ID. Guarded by the lock, this
        // ensures a unique identifier is associated with each `set` operation.
        let id = state.next_id;
        state.next_id += 1;

        // If this `set` becomes the key that expires **next**, the background
        // task needs to be notified so it can update its state.
        //
        // Whether or not the task needs to be notified is computed during the
        // `set` routine.
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // `Instant` at which the key expires.
            let when = Instant::now() + duration;

            // Only notify the worker task if the newly inserted expiration is the
            // **next** key to evict. In this case, the worker needs to be woken up
            // to update its state.
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            // Track the expiration.
            state.expirations.insert((when, id), key.clone());
            when
        });

        //let _ = state.request.send("hello".to_string());
        tokio::spawn(request_tx_task(state.request.clone(), ("set".to_string(), key.clone(), value.clone())));

        // Insert the entry into the `HashMap`.
        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            },
        );

        // If there was a value previously associated with the key **and** it
        // had an expiration time. The associated entry in the `expirations` map
        // must also be removed. This avoids leaking data.
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // clear expiration
                state.expirations.remove(&(when, prev.id));
            }
        }

        // Release the mutex before notifying the background task. This helps
        // reduce contention by avoiding the background task waking up only to
        // be unable to acquire the mutex due to this function still holding it.
        drop(state);

        if notify {
            // Finally, only notify the background task if it needs to update
            // its state to reflect a new expiration.
            self.shared.background_task.notify_one();
        }
    }

    /*pub(crate) fn policy(&self, key: String, value: Bytes, period: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        // Insert the entry into the `HashMap`.
        let _prev = state.policies.insert(
            key,
            Policy {
                //id,
                data: value,
                period,
            },
        );

        // Release the mutex before notifying the background task. This helps
        // reduce contention by avoiding the background task waking up only to
        // be unable to acquire the mutex due to this function still holding it.
        drop(state);
    }*/

    /// Returns a `Receiver` for the requested channel.
    ///
    /// The returned `Receiver` is used to receive values broadcast by `PUBLISH`
    /// commands.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // Acquire the mutex
        let mut state = self.shared.state.lock().unwrap();

        // If there is no entry for the requested channel, then create a new
        // broadcast channel and associate it with the key. If one already
        // exists, return an associated receiver.
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // No broadcast channel exists yet, so create one.
                //
                // The channel is created with a capacity of `1024` messages. A
                // message is stored in the channel until **all** subscribers
                // have seen it. This means that a slow subscriber could result
                // in messages being held indefinitely.
                //
                // When the channel's capacity fills up, publishing will result
                // in old messages being dropped. This prevents slow consumers
                // from blocking the entire system.
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers
    /// listening on the channel.
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        tokio::spawn(request_tx_task(state.request.clone(), ("publish".to_string(), key.to_string(), value.clone())));

        state
            .pub_sub
            .get(key)
            // On a successful message send on the broadcast channel, the number
            // of subscribers is returned. An error indicates there are no
            // receivers, in which case, `0` should be returned.
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there is no entry for the channel key, then there are no
            // subscribers. In this case, return `0`.
            .unwrap_or(0)
    }

    /// Signals the purge background task to shut down. This is called by the
    /// `DbShutdown`s `Drop` implementation.
    fn shutdown_purge_task(&self) {
        // The background task must be signaled to shut down. This is done by
        // setting `State::shutdown` to `true` and signalling the task.
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // Drop the lock before signalling the background task. This helps
        // reduce lock contention by ensuring the background task doesn't
        // wake up only to be unable to acquire the mutex.
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Shared {
    /// Purge all expired keys and return the `Instant` at which the **next**
    /// key will expire. The background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // The database is shutting down. All handles to the shared state
            // have dropped. The background task should exit.
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `lock()`
        // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `State` outside of the loop.
        let state = &mut *state;

        // Find all keys scheduled to expire **before** now.
        let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                // Done purging, `when` is the instant at which the next key
                // expires. The worker task will wait until this instant.
                return Some(when);
            }

            // The key expired, remove it
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }

        None
    }

    /// Returns `true` if the database is shutting down
    ///
    /// The `shutdown` flag is set when all `Db` values have dropped, indicating
    /// that the shared state can no longer be accessed.
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }

    /*fn period_policy_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            // The database is shutting down. All handles to the shared state
            // have dropped. The background task should exit.
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `lock()`
        // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `State` outside of the loop.
        let state = &mut *state;

        // Find all keys scheduled to expire **before** now.
        /*let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                // Done purging, `when` is the instant at which the next key
                // expires. The worker task will wait until this instant.
                return Some(when);
            }

            // The key expired, remove it
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }*/
        let period_at = expire.map(|duration| {
            // `Instant` at which the key expires.
            let when = Instant::now() + duration;

        None
    } */
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Routine executed by the background task.
///
/// Wait to be notified. On notification, purge any expired keys from the shared
/// state handle. If `shutdown` is set, terminate the task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        // Purge all keys that are expired. The function returns the instant at
        // which the **next** key will expire. The worker should wait until the
        // instant has passed then purge again.
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires **or** until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // There are no keys expiring in the future. Wait until the task is
            // notified.
            shared.background_task.notified().await;
        }
    }

    debug!("Purge background task shut down")
}

async fn request_tx_task(ch: mpsc::Sender<(String, String, Bytes)>, msg: (String, String, Bytes)) {
    ch.send(msg).await.unwrap();
}

#[derive(Serialize, Deserialize, Debug)]
enum DriverAction {
    Driving(i32),
    Stopping(i32),
    Standby(i32),
    Resting(i32),
}

#[derive(Serialize, Deserialize, Debug)]
struct Calibration {
    user: String,
    odo_after: f64,
    odo_unit: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct TimeDate {
    #[serde(with = "ts_seconds")]
    timestamp_after: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
struct VehicleUnit {
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
struct Error {
    fault_type: Vec<u8>,
    //#[serde(with = "ts_seconds")]
    fault_start: Option<DateTime<Utc>>,
    #[serde(with = "ts_seconds")]
    fault_end: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    event_type: Vec<u8>,
    //#[serde(with = "ts_seconds")]
    event_start: Option<DateTime<Utc>>,
    //#[serde(with = "ts_seconds")]
    event_end: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Driver {
    account: String,
    //action: DriverAction,
    action: i32,
}

#[derive(Serialize, Deserialize, Debug)]
struct TravelThreshold {
    #[serde(with = "ts_seconds")]
    timestamp_before: DateTime<Utc>,
    #[serde(with = "ts_seconds")]
    timestamp_after: DateTime<Utc>,
    threshold_before: f32,
    threshold_after: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct RestThreshold {
    #[serde(with = "ts_seconds")]
    timestamp_before: DateTime<Utc>,
    #[serde(with = "ts_seconds")]
    timestamp_after: DateTime<Utc>,
    threshold_before: f32,
    threshold_after: f32,
}

#[derive(Serialize, Deserialize, Debug)]
struct Velocity {
    speed: f32,
    odo: Option<f64>,
    timestamp: Option<DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Location {
    logitude: f64,
    latitude: f64,
    altitude: f64,
    speed_avg: f32,
}

async fn period_policy_tasks(shared: Arc<Shared>, mut request: mpsc::Receiver<(String, String, Bytes)>) {
    let mut interval_500ms = time::interval(time::Duration::from_millis(500));
    let mut interval_1s = time::interval(time::Duration::from_millis(1000));
    let url: String = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://:memory:".to_string());
    let pool: SqlitePool = SqlitePool::connect(&url).await.unwrap();

    let mut conn = pool.acquire().await.unwrap();

    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        tokio::select! {
            _ = interval_500ms.tick() => {
                /*let mut spd: f64 = 100.0;
                {
                    let state = shared.state.lock().unwrap();
                    if let Some(value) = state.entries.get("velocity").map(|entry| entry.data.clone()) {
                        spd = str::from_utf8(&value).unwrap().parse::<f64>().unwrap();
                    }
                }
                let _did = sqlx::query!(r#"INSERT INTO velocity ( speed, odo ) VALUES ( ?1, ?2 )"#,
                spd, 999.9).execute(&mut conn)
                    .await.unwrap()
                    .last_insert_rowid();*/
                let mut velocity = Velocity {
                    speed: 0.0,
                    odo: None,
                    timestamp: None,
                };
                {
                    let state = shared.state.lock().unwrap();
                    if let Some(value) = state.entries.get("velocity").map(|entry| entry.data.clone()) {
                        //spd = str::from_utf8(&value).unwrap().parse::<Velocity>().unwrap();
                        velocity = serde_json::from_slice(&value).unwrap();
                    }
                }
                let _did = sqlx::query!(r#"INSERT INTO velocity ( speed, odo ) VALUES ( ?1, ?2 )"#,
                velocity.speed, velocity.odo).execute(&mut conn)
                    .await.unwrap()
                    .last_insert_rowid();
            }
            _ = interval_1s.tick() => {
                /*sqlx::query!(r#"INSERT INTO location (logitude, latitude, altitude, speed_avg) VALUES (?1, ?2, ?3, ?4)"#,
                33.3, 44.4, 55.5, 66.6).execute(&mut conn).await.unwrap();*/
                let mut location = Location {
                    logitude: 0.0,
                    latitude: 0.0,
                    altitude: 0.0,
                    speed_avg: 0.0,
                };
                {
                    let state = shared.state.lock().unwrap();
                    if let Some(value) = state.entries.get("location").map(|entry| entry.data.clone()) {
                        if let Ok(json) = serde_json::from_slice(&value) {
                            location = json;
                        }
                    }
                }
                sqlx::query!(r#"INSERT INTO location (logitude, latitude, altitude, speed_avg) VALUES (?1, ?2, ?3, ?4)"#,
                location.logitude, location.latitude, location.altitude, location.speed_avg).execute(&mut conn).await.unwrap();
            }
            Some((cmd, key, val)) = request.recv() => {
                debug!("cmd-{} key-{} value {:#?}", cmd, key, val.to_ascii_lowercase());

                if key == "driver" {
                    let driver: Driver = serde_json::from_slice(&val).unwrap();
                    println!("driver {:?}", driver);
                    let _did = sqlx::query!(r#"INSERT INTO driver ( account, action ) VALUES ( ?1, ?2 )"#,
                    driver.account, driver.action).execute(&mut conn).await.unwrap()
                        .last_insert_rowid();
                }
                if key == "calibration" {
                    let cal: Calibration = serde_json::from_slice(&val).unwrap();
                    println!("calibration {:?}", cal);
                    let _did = sqlx::query!(r#"INSERT INTO calibration ( user, odo_after, odo_unit ) VALUES ( ?1, ?2, ?3 )"#,
                    cal.user, cal.odo_after, cal.odo_unit).execute(&mut conn).await.unwrap()
                        .last_insert_rowid();
                }
                if key == "time-date" {
                    let tm: TimeDate = serde_json::from_slice(&val).unwrap();
                    println!("date-time {:?}", tm);
                    let _did = sqlx::query!(r#"INSERT INTO time_date ( timestamp_after ) VALUES ( ?1 )"#,
                    tm.timestamp_after).execute(&mut conn).await.unwrap()
                        .last_insert_rowid();
                }
                if key == "event" {
                    if let Ok(event) = serde_json::from_slice::<Event>(&val) {
                        debug!("event {:?}", event);

                        match sqlx::query!(r#"INSERT INTO event (event_type, event_start, event_end) VALUES ( ?1, ?2, ?3)"#,
                        event.event_type, event.event_start, event.event_end).execute(&mut conn).await {
                            Ok(_) => { debug!("INSERT event - {:?} OK", event)},
                            Err(e) => { error!("INSERT event error-{:?}", e)},
                        }
                    }
                    else {
                        error!("event format invalid - {:?}", val);
                    }
                }
                /*if key == "velocity" {
                    let vel: Velocity = serde_json::from_slice(&val).unwrap();
                    println!("velocity {:?}", vel);
                    if let Some(ts) = vel.timestamp {
                        if let Some(odo) = vel.odo {
                            let _did = sqlx::query!(r#"INSERT INTO velocity ( speed, odo, timestamp ) VALUES ( ?1, ?2, ?3 )"#,
                            vel.speed, odo, ts).execute(&mut conn).await.unwrap()
                                .last_insert_rowid();
                        } else {
                            let _did = sqlx::query!(r#"INSERT INTO velocity ( speed, timestamp ) VALUES ( ?1, ?2 )"#,
                            vel.speed, ts).execute(&mut conn).await.unwrap()
                                .last_insert_rowid();
                        }
                    } else {
                        if let Some(odo) = vel.odo {
                            let _did = sqlx::query!(r#"INSERT INTO velocity ( speed, odo ) VALUES ( ?1, ?2 )"#,
                            vel.speed, odo).execute(&mut conn).await.unwrap()
                                .last_insert_rowid();
                        } else {
                            let _did = sqlx::query!(r#"INSERT INTO velocity ( speed ) VALUES ( ?1 )"#,
                            vel.speed).execute(&mut conn).await.unwrap()
                                .last_insert_rowid();
                        }
                    }
                }*/
            }
        }
    }

    debug!("Policy period update/insert background task shut down")
}
