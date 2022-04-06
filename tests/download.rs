use tracing::{debug, warn};
//use tracing::{error, info};
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::fs::File;
use tokio::io::{SeekFrom, AsyncSeekExt, BufWriter};
//use tokio::prelude::Future;
use std::env;
use bytes::Bytes;
use sqlx::sqlite::SqlitePool;
use sqlx::Sqlite;
use sqlx::pool::PoolConnection;
//use sqlx::pool::PoolConnection;
//use sqlx::types::chrono::{DateTime, Utc};
use sqlx::types::chrono::{NaiveDateTime, NaiveDate};
use serde::{Deserialize, Serialize};
//use chrono::serde::{/*ts_milliseconds, */ts_seconds};
use chrono::{Timelike, Datelike};
use tokio_stream::StreamExt;
use dotenv::dotenv;
use byteorder::{BigEndian, LittleEndian, WriteBytesExt};
use std::convert::TryInto;

use serde_big_array::BigArray;
use anyhow::Result;
//use anyhow::Error;
use anyhow::anyhow;
use std::mem;

#[tokio::test]
async fn file_read_write() {
    let mut file = File::create("/tmp/foo.txt").await.unwrap();

    // Writes some prefix of the byte string, but not necessarily all of it.
    let n = file.write(b"some bytes").await.unwrap();

    /*println!("Wrote the first {} bytes of 'some bytes'.", n);
      Ok(())*/
    let mut f = File::open("/tmp/foo.txt").await.unwrap();
    let mut buffer = Vec::new();

    // read the whole file
    let m = f.read_to_end(&mut buffer).await.unwrap();
    //Ok(())

    assert_eq!(m, n);
}

#[tokio::test]
async fn download_sql_velocity() {
    dotenv().ok();
    let url: String = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://:memory:".to_string());
    let pool: SqlitePool = SqlitePool::connect(&url).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();

    let mut s = sqlx::query!(r#"select speed,timestamp from velocity limit 10"#).fetch(&mut conn);

    while let Some(row) = s.try_next().await.unwrap() {
        let speed: f32 = row.speed.unwrap();
        //let ts = DateTime::parse_from_str("12:09:14.274", "%H:%M:%S%.3f");
        //let ts = NaiveDateTime::parse_from_str("2022-03-25 08:49:47.968", "%Y-%m-%d %H:%M:%S%.3f");
        //let mut ts = NaiveDateTime::parse_from_str("1970-01-01 00:00:00.000", "%Y-%m-%d %H:%M:%S%.3f");
        if let Some(tss) = row.timestamp {
            if let Ok(ts) = NaiveDateTime::parse_from_str(&tss, "%Y-%m-%d %H:%M:%S%.3f") {
                println!("{} {}{}{}{}{}{}", speed, ts.year(), ts.month(), ts.day(), ts.hour(), ts.minute(), ts.second());
            }
        }
    }

    assert_eq!(1, 1);
}

#[tokio::test]
async fn bytes_output() {
    let mut wtr = Vec::new();
    //wtr.write_u16::<BigEndian>(4660).unwrap();
    WriteBytesExt::write_u16::<BigEndian>(&mut wtr, 4660).unwrap();
    assert_eq!(wtr, b"\x12\x34");
    let mut wtr = Vec::new();
    WriteBytesExt::write_u16::<LittleEndian>(&mut wtr, 4660).unwrap();
    assert_eq!(wtr, b"\x34\x12");

    let b = Bytes::from(wtr);
    assert_eq!(&b[..], b"\x34\x12");

    /*let b = Bytes::from(b"\xff\xff");
      assert_eq!(ReadBytesExt::read_u16::<BigEndian>(b), b"\xff\xff");*/
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct TestFoo {
    len: u16,
    date: Vec<u8>,
    speed: Vec<u8>,
}

#[tokio::test]
async fn write_struct2file() {
    let mut file = File::create("/tmp/my_foo2.txt").await.unwrap();
    let my_foo = TestFoo {
        len: 123,
        date: b"\x20\x22\x03\x25\x08\x49\x47".to_vec(),
        speed: b"\x99\x88".to_vec(),
    };

    // writ to file
    //file.write_all(&my_foo.len.to_be_bytes()).await.unwrap();
    // pack my_foo into bytes
    /*let b = Bytes::from(my_foo);
      file.write_all(&b).await.unwrap();*/
    //bincode::serialize_into(&mut file, &my_foo).unwrap();
    let b = bincode::serialize(&my_foo).unwrap();
    file.write_all(&b).await.unwrap();
    println!("{:?} => {:?}", my_foo, my_foo.len.to_be_bytes());
    file.write_all(b"=======").await.unwrap();
    file.write_all(&my_foo.date).await.unwrap();
    file.write_all(b"=======").await.unwrap();
    file.write_all(&my_foo.speed).await.unwrap();
    assert_eq!(2, 2);
}

struct SqlVelocity {
    speed: Option<f32>,
    timestamp: Option<NaiveDateTime>,
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct OutVelocity {
    start: Vec<u8>,
    speeds: Vec<u8>,
}

fn bcd(n: u8) -> u8 {
    let mut r = n % 10;
    let q = n / 10;
    r += q * 16;
    r
}

async fn select_velocity(start_datetime: Option<NaiveDateTime>, _limit: u32) -> (Option<NaiveDateTime>, Vec<OutVelocity>) {
    dotenv().ok();

    let mut prev_dt = None;
    let mut velocities: Vec<OutVelocity> = Vec::new();
    let mut cnt: u8 = 0;

    let mut timestamp: String  = NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0).to_string();
    if let Some(dt) = start_datetime {
        timestamp = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    }

    let url: String = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://:memory:".to_string());
    let pool: SqlitePool = SqlitePool::connect(&url).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();

    /*let mut s = sqlx::query_as!(SqlVelocity, r#"select speed,timestamp as "timestamp: _" from velocity where timestamp > ?1 limit ?2"#,
                                timestamp, limit).fetch(&mut conn);*/
    let mut s = sqlx::query_as!(SqlVelocity, r#"select speed,timestamp as "timestamp: _" from velocity where timestamp > ?1"#,
                                timestamp).fetch(&mut conn);

    let mut out_velocity = OutVelocity {
        start: Vec::new(),
        speeds: Vec::new(),
    };

    while let Ok(Some(row)) = s.try_next().await {
        if cnt != 0 {
            if let Some(now_ts) = row.timestamp {
                if let Some(prev_ts) = prev_dt {
                    let diff = now_ts.signed_duration_since(prev_ts);

                    if diff.num_seconds() > 0 {
                        let num_milliseconds = diff.num_milliseconds();
                        let mut cycle = (num_milliseconds as f32 / 550.0).round() as i32;

                        while cycle > 0 && cnt < 120 {
                            println!("[debug] extra num_milliseconds-{} cnt-{}", num_milliseconds, cnt);
                            WriteBytesExt::write_u8(&mut out_velocity.speeds, 0).unwrap();
                            cnt += 1;
                            cycle -= 1;
                        }
                        if cnt == 120 {
                            velocities.push(out_velocity);

                            cnt = 0;
                            out_velocity = OutVelocity {
                                start: Vec::new(),
                                speeds: Vec::new(),
                            };
                        }
                    }
                }
            }
        }

        if cnt == 0 {
            let mut year: u16 = 0;
            let mut month: u8 = 0;
            let mut day: u8 = 0;
            let mut hour: u8 = 0;
            let mut minute: u8 = 0;
            let mut second: u8 = 0;

            if let Some(ts) = row.timestamp {
                //ndt = NaiveDateTime::parse_from_str(&ts, "%Y-%m-%d %H:%M:%S%.3f").ok();
                //println!("{} {}{}{}{}{}{}", speed, ts.year(), ts.month(), ts.day(), ts.hour(), ts.minute(), ts.second());
                year = ts.year() as u16;
                month = ts.month() as u8;
                day = ts.day() as u8;
                hour = ts.hour() as u8;
                minute = ts.minute() as u8;
                second = ts.second() as u8;
            }
            //WriteBytesExt::write_u16::<BigEndian>(&mut out_velocity.start, year).unwrap();
            assert_eq!(year, 2022);
            WriteBytesExt::write_u8(&mut out_velocity.start, bcd((year / 100).try_into().unwrap())).unwrap();
            WriteBytesExt::write_u8(&mut out_velocity.start, bcd(((year % 100) & 0x00FF).try_into().unwrap())).unwrap();
            WriteBytesExt::write_u8(&mut out_velocity.start, bcd(month)).unwrap();
            WriteBytesExt::write_u8(&mut out_velocity.start, bcd(day)).unwrap();
            WriteBytesExt::write_u8(&mut out_velocity.start, bcd(hour)).unwrap();
            WriteBytesExt::write_u8(&mut out_velocity.start, bcd(minute)).unwrap();
            WriteBytesExt::write_u8(&mut out_velocity.start, bcd(second)).unwrap();
            println!("{}{}{}{}{}{} - {:?}", year, month, day, hour, minute, second, out_velocity.start);
        }

        let speed: u8 = row.speed.unwrap_or(0.0) as u8;
        WriteBytesExt::write_u8(&mut out_velocity.speeds, speed).unwrap();
        prev_dt = row.timestamp;
        cnt += 1;

        if cnt == 120 {
            velocities.push(out_velocity);

            cnt = 0;
            out_velocity = OutVelocity {
                start: Vec::new(),
                speeds: Vec::new(),
            };
        }
    }

    if cnt != 0 {
        while cnt < 120 {
            WriteBytesExt::write_u8(&mut out_velocity.speeds, 0).unwrap();
            cnt += 1;
        }
        velocities.push(out_velocity);
    }

    (prev_dt, velocities)
}

#[tokio::test]
async fn test_fb161_dl_velocity() {
    let mut file = File::create("/tmp/fb161_xxxxx_xxxxx.vdr").await.unwrap();

    // block number
    let block_num: u16 = 0x01;
    let mut wtr = Vec::new();
    WriteBytesExt::write_u16::<BigEndian>(&mut wtr, block_num).unwrap();
    //file.write_all(&wtr).await.unwrap();

    // block format - [code][name][len][data]
    // code: 1byte
    // name: 18bytes
    // len: 4bytes
    // data: len bytes
    let block_code: u8 = 0x01;
    WriteBytesExt::write_u8(&mut wtr, block_code).unwrap();
    //file.write_u8(code).await.unwrap();
    let block_name = b"detail-velocity-information";
    //file.write_all(name).await.unwrap(); /* chinese? */
    wtr.append(&mut block_name.to_vec());
    //let start_datetime = NaiveDate::from_ymd(2022, 3, 28).and_hms(11, 33, 38);
    let start_datetime = NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0);

    if let (Some(_dt), v) = select_velocity(Some(start_datetime), 65535).await {
        let block_len = v.len() * (2 + 7 + 120);
        WriteBytesExt::write_u32::<BigEndian>(&mut wtr, block_len.try_into().unwrap()).unwrap();
        WriteBytesExt::write_u16::<BigEndian>(&mut wtr, v.len().try_into().unwrap()).unwrap();
        for mut e in v {
            wtr.append(&mut e.start);
            wtr.append(&mut e.speeds);
        }
    }
    else {
    }

    let block_checksum: u8 = 0xFA;
    WriteBytesExt::write_u8(&mut wtr, block_checksum).unwrap();

    file.write_all(&wtr).await.unwrap();
}

#[tokio::test]
async fn test_bcd() {
    assert_eq!(bcd(19), 0x19);
    assert_eq!(bcd(80), 0x80);
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct DownloadFile {
    num: u16,
    // 1th block
    // 2th block
    // ...
    //checksum: u8,
}

// block format - [code][name][len][data]
// code: 1byte
// name: 18bytes
// len: 4bytes
// data: len bytes
#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlock {
    code: u8,
    name: [u8; 18],
    len: u32,
    //data: DownloadBlockEntry,
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
enum DownloadBlockEntry {
    EventErrorInformation(EventErrorInformation),
    DetailVelocityInformation(DetailVelocityInformation),
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct DetailVelocityInformation {
    num: u16,
    date: [u8; 7],
    #[serde(with = "BigArray")]
    speed: [u8; 120],
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct EventErrorInformation {
    cnt: u8,
    date: Vec<u8>,
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockVelocity {
    code: u8,
    name: [u8; 18],
    len: u32,
    num: u16,
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct DownloadEntryVelocity {
    date_time: [u8; 7],
    #[serde(with = "BigArray")]
    speed: [u8; 120],
}

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockEvent {
    code: u8,
    name: [u8; 18],
    len: u32,
    num: u16,
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct DownloadEntryError {
    etype: u8,
    start_ts: [u8; 7],
    end_ts: [u8; 7],
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
struct DownloadEntryEventDeprecated {
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

#[derive(Clone, Copy, PartialEq, Debug, Serialize, Deserialize)]
#[repr(packed(1))]
struct DownloadBlockLocation {
    code: u8,
    name: [u8; 18],
    len: u32,
    num: u16,
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
struct DownloadBlockTechnology {
    code: u8,
    name: [u8; 18],
    len: u32,

    /* vu */
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

async fn fb161_download_velocity(writer: &mut BufWriter<File>) -> Result<u64> {
    let mut block: DownloadBlockVelocity = DownloadBlockVelocity {
        code: 0x01,
        name: *b"velocity-informati",
        len: 0x00000000,
        num: 0x0000,
    };
    //let mut force_cnt = 10;

    if let Ok(prev_pos) = writer.seek(SeekFrom::Current(0)).await {
        let data_ofs = prev_pos + mem::size_of::<DownloadBlockVelocity>() as u64;
        writer.seek(SeekFrom::Start(data_ofs)).await?;

        if let Ok(mut conn) = fb161_sql_conn().await {
            let mut s = sqlx::query_as!(SqlVelocity, r#"select speed,timestamp as "timestamp: _" from velocity"#)
                .fetch(&mut conn);

            let mut prev_timestamp = NaiveDate::from_ymd(1970, 1, 1).and_hms(0, 0, 0);
            let mut index: usize = 0;
            let mut entry = DownloadEntryVelocity {
                date_time: *b"\x19\x70\x01\x01\x00\x00\x00",
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
                                    if let Ok(entry) = bincode::serialize(&entry) {
                                        let _ = writer.write_all(&entry).await?;
                                    }

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
                            entry.date_time[0] = bcd((timestamp.year() / 100) as u8);
                            entry.date_time[1] = bcd((timestamp.year() % 100) as u8);
                            entry.date_time[2] = bcd(timestamp.month() as u8);
                            entry.date_time[3] = bcd(timestamp.day() as u8);
                            entry.date_time[4] = bcd(timestamp.hour() as u8);
                            entry.date_time[5] = bcd(timestamp.minute() as u8);
                            entry.date_time[6] = bcd(timestamp.second() as u8);
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
                    if let Ok(entry) = bincode::serialize(&entry) {
                        let _ = writer.write_all(&entry).await?;
                    }

                    index = 0;
                    entry = DownloadEntryVelocity {
                        date_time: *b"\x19\x70\x01\x01\x00\x00\x00",
                        speed: [0; 120],
                    };
                }
            }
            // flush non-full pending entry
            if index != 0 {
                if let Ok(entry) = bincode::serialize(&entry) {
                    let _ = writer.write_all(&entry).await?;
                }
            }
        }

        if let Ok(curr_pos) = writer.seek(SeekFrom::Current(0)).await {
            writer.seek(SeekFrom::Start(prev_pos)).await?;
            block.len = 2/*block.num*/ + (block.num as u32) * (mem::size_of::<DownloadEntryVelocity>()) as u32;
            let block= bincode::serialize(&block)?;
            let wn = writer.write_all(&block).await;
            assert!(wn.is_ok());

            let _ = writer.seek(SeekFrom::Start(curr_pos)).await;

            //writer.flush().await.unwrap();
            Ok(curr_pos - prev_pos)
        }
        else {
            Err(anyhow!("current seek error"))
        }
    }
    else {
        Err(anyhow!("previous seek error"))
    }
}

async fn fb161_download_event(writer: &mut BufWriter<File>) -> Result<u64> {
    let mut event_num = 0;
    let mut block: DownloadBlockEvent = DownloadBlockEvent {
        code: 0x00,
        name: String::from("事件故障資料").as_bytes().try_into().unwrap(),
        len: 0x00,
        num: 0x00,
    };

    let prev_pos = match writer.seek(SeekFrom::Current(0)).await {
        Ok(pos) => {
            let data_ofs = pos + mem::size_of::<DownloadBlockEvent>() as u64;
            writer.seek(SeekFrom::Start(data_ofs)).await?;
            pos
        },
        Err(e) => {
            return Err(anyhow!("previous seek error: {}", e));
        }
    };

    if let Ok(mut conn) = fb161_sql_conn().await {
        let mut s = sqlx::query!(r#"select fault_type, fault_start, fault_end from error"#)
            .fetch(&mut conn);

        let mut entry = DownloadEntryEventDeprecated {
            etype: 0x00,
            start_ts: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
            end_ts: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
        };
        while let Ok(Some(row)) = s.try_next().await {
            block.num += 1; /* error only */
            entry.etype = (row.fault_type & 0x0000000F) as u8;
            if let Some(timestamp) = row.fault_start {
                entry.start_ts[0] = bcd((timestamp.year() / 100) as u8);
                entry.start_ts[1] = bcd((timestamp.year() % 100) as u8);
                entry.start_ts[2] = bcd(timestamp.month() as u8);
                entry.start_ts[3] = bcd(timestamp.day() as u8);
                entry.start_ts[4] = bcd(timestamp.hour() as u8);
                entry.start_ts[5] = bcd(timestamp.minute() as u8);
                entry.start_ts[6] = bcd(timestamp.second() as u8);
            }
            if let Some(timestamp) = row.fault_end {
                entry.end_ts[0] = bcd((timestamp.year() / 100) as u8);
                entry.end_ts[1] = bcd((timestamp.year() % 100) as u8);
                entry.end_ts[2] = bcd(timestamp.month() as u8);
                entry.end_ts[3] = bcd(timestamp.day() as u8);
                entry.end_ts[4] = bcd(timestamp.hour() as u8);
                entry.end_ts[5] = bcd(timestamp.minute() as u8);
                entry.end_ts[6] = bcd(timestamp.second() as u8);
            }
            if let Ok(entry) = bincode::serialize(&entry) {
                let _ = writer.write_all(&entry).await?;
            }
        }
    }

    let event_pos = writer.seek(SeekFrom::Current(0)).await.unwrap_or(0);
    assert!(event_pos != 0);

    if let Ok(mut sconn) = fb161_sql_conn().await {
        let mut s = sqlx::query!(r#"select event_type, event_start, event_end from event"#)
            .fetch(&mut sconn);

        let mut entry = DownloadEntryEventDeprecated {
            etype: 0x00,
            start_ts: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
            end_ts: *b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
        };
        writer.seek(SeekFrom::Current(1)).await?; /* for event record number */
        while let Ok(Some(row)) = s.try_next().await {
            event_num += 1;
            entry.etype = (row.event_type & 0x0000000F) as u8;
            if let Some(timestamp) = row.event_start {
                entry.start_ts[0] = bcd((timestamp.year() / 100) as u8);
                entry.start_ts[1] = bcd((timestamp.year() % 100) as u8);
                entry.start_ts[2] = bcd(timestamp.month() as u8);
                entry.start_ts[3] = bcd(timestamp.day() as u8);
                entry.start_ts[4] = bcd(timestamp.hour() as u8);
                entry.start_ts[5] = bcd(timestamp.minute() as u8);
                entry.start_ts[6] = bcd(timestamp.second() as u8);
            }
            if let Some(timestamp) = row.event_end {
                entry.end_ts[0] = bcd((timestamp.year() / 100) as u8);
                entry.end_ts[1] = bcd((timestamp.year() % 100) as u8);
                entry.end_ts[2] = bcd(timestamp.month() as u8);
                entry.end_ts[3] = bcd(timestamp.day() as u8);
                entry.end_ts[4] = bcd(timestamp.hour() as u8);
                entry.end_ts[5] = bcd(timestamp.minute() as u8);
                entry.end_ts[6] = bcd(timestamp.second() as u8);
            }
            if let Ok(entry) = bincode::serialize(&entry) {
                let _ = writer.write_all(&entry).await?;

                /* TODO, similar cnt */
                let _ = writer.write_all(b"\x00").await?;
            }
        }
    }

    if let Ok(curr_pos) = writer.seek(SeekFrom::Current(0)).await {
        writer.seek(SeekFrom::Start(prev_pos)).await?;
        block.len = (block.num as u32) * (mem::size_of::<DownloadEntryEvent>()) as u32;
        block.len +=  1/*event-num*/ + (event_num as u32) * (mem::size_of::<DownloadEntryEvent>() + 1/*similar cnt*/) as u32;
        let block= bincode::serialize(&block)?;
        let wn = writer.write_all(&block).await;
        assert!(wn.is_ok());

        {
            let _ = writer.seek(SeekFrom::Start(event_pos)).await;
            //let _ = writer.write(event_num::<u8>.as_bytes()).await;

            let mut buf = Vec::new();
            WriteBytesExt::write_u8(&mut buf, event_num).unwrap();
            let _ = writer.write_all(&Bytes::from(buf)).await;
        }

        let _ = writer.seek(SeekFrom::Start(curr_pos)).await;

        //writer.flush().await.unwrap();
        Ok(curr_pos - prev_pos)
    }
    else {
        Err(anyhow!("current seek error"))
    }
}

async fn fb161_download_location(writer: &mut BufWriter<File>) -> Result<u64> {
    let mut block: DownloadBlockLocation = DownloadBlockLocation {
        code: 0x04,
        name: String::from("定位資料點點").as_bytes().try_into().unwrap(),
        len: 0x00,
        num: 0x00,
    };

    let prev_pos = match writer.seek(SeekFrom::Current(0)).await {
        Ok(pos) => {
            let data_ofs = pos + mem::size_of::<DownloadBlockLocation>() as u64;
            writer.seek(SeekFrom::Start(data_ofs)).await?;
            pos
        },
        Err(e) => {
            return Err(anyhow!("previous seek error: {}", e));
        }
    };

    if let Ok(mut conn) = fb161_sql_conn().await {
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
                    if minutes == 0 {
                        let ts = NaiveDateTime::parse_from_str(&ts, "%Y-%m-%d %H:%M").unwrap();
                        bcd_timestamp(&mut entry.start, ts)?;
                    }

                    /* TODO convert to meter from degree */
                    entry.hour[minutes].logitude = logi as f32 * 111.12;
                    entry.hour[minutes].latitude = lati as f32 * 111.12;
                    entry.hour[minutes].altitude = (alti as f32 * 0.01) as u16;
                    entry.hour[minutes].speed = spd as u8;

                    minutes += 1;

                    if minutes == 60 {
                        if let Ok(entry) = bincode::serialize(&entry) {
                            let _ = writer.write_all(&entry).await?;
                        }
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
            if let Ok(entry) = bincode::serialize(&entry) {
                let _ = writer.write_all(&entry).await?;
            }
            block.num += 1;
        }
    }

    if let Ok(curr_pos) = writer.seek(SeekFrom::Current(0)).await {
        writer.seek(SeekFrom::Start(prev_pos)).await?;
        block.len = (block.num as u32) * (mem::size_of::<DownloadEntryLocationHour>()) as u32;
        let block= bincode::serialize(&block)?;
        let wn = writer.write_all(&block).await;
        assert!(wn.is_ok());

        let _ = writer.seek(SeekFrom::Start(curr_pos)).await;

        Ok(curr_pos - prev_pos)
    }
    else {
        Err(anyhow!("current seek error"))
    }
}

async fn fb161_download_driver(writer: &mut BufWriter<File>) -> Result<u64> {
    let mut block: DownloadBlockLocation = DownloadBlockLocation {
        code: 0x03,
        name: String::from("駕駛活動類型").as_bytes().try_into().unwrap(),
        len: 0x00,
        num: 0x00,
    };

    let prev_pos = match writer.seek(SeekFrom::Current(0)).await {
        Ok(pos) => {
            let data_ofs = pos + mem::size_of::<DownloadBlockGeneric>() as u64;
            writer.seek(SeekFrom::Start(data_ofs)).await?;
            pos
        },
        Err(e) => {
            return Err(anyhow!("previous seek error: {}", e));
        }
    };

    if let Ok(mut conn) = fb161_sql_conn().await {
        let mut entry: DownloadEntryDriver = DownloadEntryDriver {
            account: [0; 18],
            action: 0,
            dtime: [0; 7],
        };

        let mut s = sqlx::query!(r#"SELECT account, action, timestamp FROM driver"#)
            .fetch(&mut conn);
        while let Ok(Some(row)) = s.try_next().await {
            debug!("[debug][driver] {:?}", row);

            if let Some(ts) = row.timestamp {
                bcd_timestamp(&mut entry.dtime, ts)?;

                copy_slice(&mut entry.account, row.account.as_bytes());

                entry.action = row.action as u8;
                if let Ok(entry) = bincode::serialize(&entry) {
                    let _ = writer.write_all(&entry).await?;
                }
                block.num += 1;
            }
        }
    }

    if let Ok(curr_pos) = writer.seek(SeekFrom::Current(0)).await {
        writer.seek(SeekFrom::Start(prev_pos)).await?;
        block.len = (block.num as u32) * (mem::size_of::<DownloadEntryDriver>()) as u32;
        let block= bincode::serialize(&block)?;
        let wn = writer.write_all(&block).await;
        assert!(wn.is_ok());

        let _ = writer.seek(SeekFrom::Start(curr_pos)).await;

        Ok(curr_pos - prev_pos)
    }
    else {
        Err(anyhow!("current seek error"))
    }
}

async fn fb161_download_vu(writer: &mut BufWriter<File>) -> Result<u64> {
    let prev_pos = match writer.seek(SeekFrom::Current(0)).await {
        Ok(pos) => {
            let data_ofs = pos + mem::size_of::<DownloadBlockTechnology>() as u64;
            writer.seek(SeekFrom::Start(data_ofs)).await?;
            pos
        },
        Err(e) => {
            return Err(anyhow!("previous seek error: {}", e));
        }
    };

    let mut calibration_num: u16 = 0;
    if let Ok(mut conn) = fb161_sql_conn().await {
        if let Ok(row) = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM calibration"#).fetch_one(&mut conn).await {
            calibration_num = row.cnt as u16;
        }
    }
    let _ = writer.write(&calibration_num.to_le_bytes()).await?;

    if let Ok(mut conn) = fb161_sql_conn().await {
        let mut s = sqlx::query!(r#"SELECT user, odo_before, odo_after, timestamp_before, timestamp_after FROM calibration"#)
            .fetch(&mut conn);
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

            /*let (n, m) = (entry.user.len(), row.user.as_bytes().len());
            if n > m {
                entry.user[..m].copy_from_slice(row.user.as_bytes());
            }
            else {
                entry.user[..n].copy_from_slice(&row.user.as_bytes()[..n]);
            }*/
            copy_slice(&mut entry.user, row.user.as_bytes());

            if let Ok(entry) = bincode::serialize(&entry) {
                let _ = writer.write_all(&entry).await?;
            }
        }
    }
    else {
        panic!("sql connection error");
    }

    let mut td_num: u16 = 0;
    if let Ok(mut conn) = fb161_sql_conn().await {
        if let Ok(row) = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM time_date"#).fetch_one(&mut conn).await {
            td_num = row.cnt as u16;
        }
    }
    let _ = writer.write(&td_num.to_le_bytes()).await?;
    debug!("[debug][time-date] num-{:?}", td_num);

    if let Ok(mut conn) = fb161_sql_conn().await {
        let mut s = sqlx::query!(r#"SELECT timestamp_before AS "timestamp_before: NaiveDateTime", timestamp_after AS "timestamp_after: NaiveDateTime" FROM time_date"#)
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
            if let Ok(entry) = bincode::serialize(&entry) {
                let _ = writer.write_all(&entry).await?;
            }
        }
    }
    else {
        panic!("sql connection error");
    }

    let mut travel_num: u16 = 0;
    if let Ok(mut conn) = fb161_sql_conn().await {
        if let Ok(row) = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM travel_threshold"#).fetch_one(&mut conn).await {
            travel_num = row.cnt as u16;
        }
    }
    let _ = writer.write(&travel_num.to_le_bytes()).await?;

    if let Ok(mut conn) = fb161_sql_conn().await {
        let mut s = sqlx::query!(r#"SELECT timestamp_before, timestamp_after, threshold_before, threshold_after FROM travel_threshold"#)
            .fetch(&mut conn);
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

            if let Ok(entry) = bincode::serialize(&entry) {
                let _ = writer.write_all(&entry).await?;
            }
        }
    }

    let mut rest_num: u16 = 0;
    if let Ok(mut conn) = fb161_sql_conn().await {
        if let Ok(row) = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM rest_threshold"#).fetch_one(&mut conn).await {
            rest_num = row.cnt as u16;
        }
    }
    let _ = writer.write(&rest_num.to_le_bytes()).await?;

    if let Ok(mut conn) = fb161_sql_conn().await {
        let mut s = sqlx::query!(r#"SELECT timestamp_before, timestamp_after, threshold_before, threshold_after FROM rest_threshold"#)
            .fetch(&mut conn);
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

            if let Ok(entry) = bincode::serialize(&entry) {
                let _ = writer.write_all(&entry).await?;
            }
        }
    }

    let mut block: DownloadBlockTechnology = DownloadBlockTechnology {
        code: 0x02,
        name: String::from("技術資料格式").as_bytes().try_into().unwrap(),
        len: 0x00,

        manufacture_name: [0; 36],
        manufacture_address: [0; 36],
        serial_number: [0; 8],
        sw_version: [0; 4],
        install_timestamp: [0; 7],
        manufacture_date: [0; 4],
        certification_number: [0; 8],
        car_number: [0; 17],
    };

    if let Ok(mut conn) = fb161_sql_conn().await {
        if let Ok(row) = sqlx::query!(r#"SELECT manufacture_name, manufacture_address, serial_number, sw_version, install_timestamp, manufacture_date as "manufacture_date: NaiveDate", certification_number, car_number FROM vehicle_unit"#).fetch_one(&mut conn).await {
            copy_slice(&mut block.manufacture_name, row.manufacture_name.as_bytes());
            copy_slice(&mut block.manufacture_address, row.manufacture_address.as_bytes());
            copy_slice(&mut block.serial_number, row.serial_number.as_bytes());
            copy_slice(&mut block.sw_version, row.sw_version.as_bytes());
            copy_slice(&mut block.certification_number, row.certification_number.as_bytes());

            if let Some(car_number) = row.car_number {
                copy_slice(&mut block.car_number, car_number.as_bytes());
            }

            if let Some(ts) = row.install_timestamp {
                bcd_timestamp(&mut block.install_timestamp, ts)?;
            }
            if let Some(ts) = row.manufacture_date {
                block.manufacture_date[0] = bcd((ts.year() / 100) as u8);
                block.manufacture_date[1] = bcd((ts.year() % 100) as u8);
                block.manufacture_date[2] = bcd(ts.month() as u8);
                block.manufacture_date[3] = bcd(ts.day() as u8);
            }
        }
    }

    if let Ok(curr_pos) = writer.seek(SeekFrom::Current(0)).await {
        writer.seek(SeekFrom::Start(prev_pos)).await?;
        block.len = (mem::size_of::<DownloadBlockTechnology>()
                     - mem::size_of::<DownloadBlockGeneric>()
                     + mem::size_of::<u16/*num*/>()) as u32;
        block.len += (calibration_num as u32) * (mem::size_of::<DownloadBlockCalibration>()) as u32;
        block.len += (td_num as u32) * (mem::size_of::<DownloadBlockTimeDate>()) as u32;
        block.len += (travel_num as u32) * (mem::size_of::<DownloadBlockTravelThreshold>()) as u32;
        block.len += (rest_num as u32) * (mem::size_of::<DownloadBlockRestThreshold>()) as u32;
        let block= bincode::serialize(&block)?;
        let wn = writer.write_all(&block).await;
        assert!(wn.is_ok());

        let _ = writer.seek(SeekFrom::Start(curr_pos)).await;

        Ok(curr_pos - prev_pos)
    }
    else {
        Err(anyhow!("current seek error"))
    }
}


async fn fb161_sql_conn() -> sqlx::Result<PoolConnection<Sqlite>> {
    dotenv().ok();

    let url: String = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://:memory:".to_string());
    let pool: SqlitePool = SqlitePool::connect(&url).await.unwrap();
    pool.acquire().await
}

async fn fb161_download(file: &str) -> Result<()> {
    if let Ok(file)  = File::create(file).await {
        let mut writer = BufWriter::new(file);

        // block number
        writer.write(&5_u16.to_le_bytes()).await?;
        let _ = fb161_download_event(&mut writer).await;
        let _ = fb161_download_velocity(&mut writer).await;
        let _ = fb161_download_vu(&mut writer).await;
        let _ = fb161_download_driver(&mut writer).await;
        let _ = fb161_download_location(&mut writer).await;
        
        //TODO robust
        let block_checksum = b"\xFA";
        let _ = writer.write_all(block_checksum).await;

        writer.flush().await.unwrap();
        Ok(())
    }
    else {
        Err(anyhow!("file create error"))
    }
}

async fn fb161_download_name() -> Result<String> {
    let mut conn = fb161_sql_conn().await?;
    let s = sqlx::query!(r#"select timestamp as "timestamp: NaiveDateTime" from velocity"#).fetch_one(&mut conn).await?;
    let ts = s.timestamp;

    let s = sqlx::query!(r#"SELECT car_number FROM vehicle_unit"#).fetch_one(&mut conn).await?;
    let car_number = s.car_number;

    match (ts, car_number) {
        (Some(ts), Some(car)) => {
            Ok(format!("D{}_{}.vdr", ts.format("%Y%m%d_%H%M%S"), car))
        },
        (Some(ts), None) => {
            Ok(format!("D{}_XXXXXXXX.vdr", ts.format("%Y%m%d_%H%M%S")))
        },
        (None, Some(car)) => {
            Ok(format!("DXXXXXXXX_XXXXXX_{}.vdr", car))
        },
        (None, None) => {
            Ok("DXXXXXXXX_XXXXXX_XXXXXXX.vdr".to_string())
        }
    }
}

#[tokio::test]
async fn test_fb161_download() {
    let f = fb161_download_name().await;

    assert!(f.is_ok());
    let f = f.unwrap();

    match fb161_download(&f).await {
        Ok(_) => {},
        Err(e) => {
            panic!("{}", e);
        }
    }
}

#[tokio::test]
async fn test_utf8() {
    let name = String::from("事件及故障資料");
    let array: [u8; 21] = name.as_bytes().try_into().expect("Event&ErrorInfo");
    println!("[debug] {:?} {:?} {:?}", name, name.as_bytes(), array);
    let name = String::from("事件及故障資");
    let array: [u8; 18] = name.as_bytes().try_into().expect("Event&ErrorInfo");
    println!("[debug] {:?} {:?} {:?}", name, name.as_bytes(), array);
    assert_eq!(&array[..], name.as_bytes());
}

#[tokio::test]
async fn test_fb161_download_location() {
    if let Ok(file)  = File::create("/tmp/fb161_location.vdr").await {
        let mut writer = BufWriter::new(file);

        {
            //TODO
            let mut block_num = Vec::new();
            WriteBytesExt::write_u16::<LittleEndian>(&mut block_num, 1).unwrap();
            let _ = writer.write_all(&Bytes::from(block_num)).await;
        }
        let _ = fb161_download_location(&mut writer).await;
        {
            //TODO
            let block_checksum = b"\xFA";
            let _ = writer.write_all(block_checksum).await;
        }
        writer.flush().await.unwrap();
    }
    else {
        panic!("fb161_download_location");
    }
}

#[tokio::test]
async fn test_fb161_download_velocity() {
    if let Ok(file)  = File::create("/tmp/fb161_velocity.vdr").await {
        let mut writer = BufWriter::new(file);

        {
            //TODO
            let mut block_num = Vec::new();
            WriteBytesExt::write_u16::<LittleEndian>(&mut block_num, 1).unwrap();
            let _ = writer.write_all(&Bytes::from(block_num)).await;
        }
        let _ = fb161_download_velocity(&mut writer).await;
        {
            //TODO
            let block_checksum = b"\xFA";
            let _ = writer.write_all(block_checksum).await;
        }
        writer.flush().await.unwrap();
    }
    else {
        panic!("fb161_download_velocity");
    }
}

#[tokio::test]
async fn test_fb161_download_driver() {
    if let Ok(file)  = File::create("/tmp/fb161_driver.vdr").await {
        let mut writer = BufWriter::new(file);

        {
            //TODO
            let mut block_num = Vec::new();
            WriteBytesExt::write_u16::<LittleEndian>(&mut block_num, 1).unwrap();
            let _ = writer.write_all(&Bytes::from(block_num)).await;
        }
        let _ = fb161_download_driver(&mut writer).await;
        {
            //TODO
            let block_checksum = b"\xFA";
            let _ = writer.write_all(block_checksum).await;
        }
        writer.flush().await.unwrap();
    }
    else {
        panic!("fb161_download_driver");
    }
}

#[tokio::test]
async fn test_fb161_download_vu() {
    if let Ok(file)  = File::create("/tmp/fb161_vu.vdr").await {
        let mut writer = BufWriter::new(file);

        {
            //TODO
            let mut block_num = Vec::new();
            WriteBytesExt::write_u16::<LittleEndian>(&mut block_num, 1).unwrap();
            let _ = writer.write_all(&Bytes::from(block_num)).await;
        }
        let _ = fb161_download_vu(&mut writer).await;
        {
            //TODO
            let block_checksum = b"\xFA";
            let _ = writer.write_all(block_checksum).await;
        }
        writer.flush().await.unwrap();
    }
    else {
        panic!("test_fb161_download_vu");
    }
}

fn copy_slice<'a>(a: &'a mut [u8], s: &[u8]) -> &'a[u8] {
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

fn bcd_timestamp(buff: &mut [u8; 7], ts: NaiveDateTime) -> Result<()> {
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

async fn fb161_multiple_sql() -> Result<()> {
    {
        let mut conn = fb161_sql_conn().await?;
        let s = sqlx::query!(r#"select timestamp as "timestamp: NaiveDateTime" from velocity"#).fetch_one(&mut *conn).await?;
        assert!(s.timestamp != None);
    }

    {
        let mut conn = fb161_sql_conn().await?;
        let mut s = sqlx::query!(r#"select speed,timestamp as "timestamp: NaiveDateTime" from velocity"#).fetch(&mut *conn);
        while let Ok(Some(row)) = s.try_next().await {
            assert!(row.speed != None);
            assert!(row.timestamp != None);
        }
    }

    {
        let mut conn = fb161_sql_conn().await?;
        let s = sqlx::query!(r#"SELECT car_number FROM vehicle_unit"#).fetch_one(&mut *conn).await?;
        assert!(s.car_number != None);
    }

    {
        let mut conn = fb161_sql_conn().await?;
        /*let mut s = sqlx::query_as!(SqlVelocity, r#"select speed,timestamp as "timestamp: _" from velocity"#).fetch(&mut conn);
          while let Ok(Some(row)) = s.try_next().await {
          assert!(row.speed == None);
          assert!(row.timestamp == None);
          }*/
        let mut s = sqlx::query!(r#"select fault_type, fault_start, fault_end from error"#).fetch(&mut *conn);
        while let Ok(Some(row)) = s.try_next().await {
            assert!(row.fault_start != None);
            assert!(row.fault_end != None);
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_fb161_multisql() {
    match fb161_multiple_sql().await {
        Ok(_) => {},
        Err(e) => {
            panic!("{}", e);
        }
    }
}

pub struct Fb161Downloader {
    writer: BufWriter<File>,
}

impl Fb161Downloader {
    pub async fn new(fname: &str) -> Self {
        if let Ok(file) = File::create(fname).await {
            Self {
                writer: BufWriter::new(file),
            }
        }
        else {
            panic!("Fb161Downloader::new");
        }
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
        let event_num;
        let error_num;

        let seek_it = SeekFrom::Current(mem::size_of::<DownloadBlockMeta>().try_into().unwrap());
        self.writer.seek(seek_it).await?;

        {
            let mut conn = fb161_sql_conn().await?;

            let row = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM error"#).fetch_one(&mut conn).await?;
            error_num = row.cnt as u8;
            self.writer.write(&error_num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT fault_type, fault_start, fault_end FROM error LIMIT ?"#, error_num)
                .fetch(&mut conn);

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
            let mut conn = fb161_sql_conn().await?; 

            let row = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM event"#).fetch_one(&mut conn).await?;
            event_num = row.cnt as u8;
            self.writer.write(&event_num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"select event_type, event_start, event_end from event LIMIT ?"#, event_num)
                .fetch(&mut conn);

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

        let mut block: DownloadBlockGeneric = DownloadBlockGeneric {
            code: 0x01,
            name: *b"velocity-informati",
            len: 0x00000000,
            num: 0x0000,
        };

        {
            let seek_it = SeekFrom::Current(mem::size_of::<DownloadBlockMeta>().try_into().unwrap());
            self.writer.seek(seek_it).await?;

            let mut conn = fb161_sql_conn().await?;
            let mut s = sqlx::query_as!(SqlVelocity, r#"select speed,timestamp as "timestamp: _" from velocity"#)
                .fetch(&mut conn);

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
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut conn = fb161_sql_conn().await?;
            let num = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM calibration"#).fetch_one(&mut conn).await?.cnt;
            self.writer.write(&num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT user, odo_before, odo_after, timestamp_before, timestamp_after FROM calibration"#)
                .fetch(&mut conn);
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
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut conn = fb161_sql_conn().await?;
            let num = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM time_date"#).fetch_one(&mut conn).await?.cnt;
            self.writer.write(&num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT timestamp_before AS "timestamp_before: NaiveDateTime", timestamp_after AS "timestamp_after: NaiveDateTime" FROM time_date"#)
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
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut conn = fb161_sql_conn().await?;
            let num = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM travel_threshold"#).fetch_one(&mut conn).await?.cnt;
            self.writer.write(&num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT timestamp_before, timestamp_after, threshold_before, threshold_after FROM travel_threshold"#)
                .fetch(&mut conn);
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
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        {
            let mut conn = fb161_sql_conn().await?;
            let num = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM rest_threshold"#).fetch_one(&mut conn).await?.cnt;
            self.writer.write(&num.to_le_bytes()).await?;

            let mut s = sqlx::query!(r#"SELECT timestamp_before, timestamp_after, threshold_before, threshold_after FROM rest_threshold"#)
                .fetch(&mut conn);
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

            let mut conn = fb161_sql_conn().await?;
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
        let orig_pos = self.writer.seek(SeekFrom::Current(0)).await?;
        let driver_num;

        let seek_it = SeekFrom::Current(mem::size_of::<DownloadBlockMeta>().try_into().unwrap());
        self.writer.seek(seek_it).await?;

        {
            let mut conn = fb161_sql_conn().await?;

            let row = sqlx::query!(r#"SELECT COUNT(*) as cnt FROM driver"#).fetch_one(&mut conn).await?;
            driver_num = row.cnt as u8;
            self.writer.write(&driver_num.to_le_bytes()).await?;

            let mut entry: DownloadEntryDriver = DownloadEntryDriver {
                account: [0; 18],
                action: 0,
                dtime: [0; 7],
            };

            let mut s = sqlx::query!(r#"SELECT account, action, timestamp FROM driver"#)
                .fetch(&mut conn);
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
            let mut conn = fb161_sql_conn().await?;

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
                        if minutes == 0 {
                            let ts = NaiveDateTime::parse_from_str(&ts, "%Y-%m-%d %H:%M")?;
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

    /*fn parity_bits(&mut self, data: &[u8]) -> u8 {
        let mut parity = 0;
        for b in data {
            parity ^= *b;
        }
        parity
    }*/

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

    pub async fn check(&mut self, _block_item: u8) -> Result<()> {
        Err(anyhow!("not implemented"))
    }
}

#[tokio::test]
async fn test_fb161_oo() {
    let mut fb161 = Fb161Downloader::new("./test_fb161.vdr").await;
    fb161.download().await.unwrap();
    assert!(fb161.check(5).await.is_ok());
}
