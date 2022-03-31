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
struct DownloadEntryEvent {
    etype: u8,
    start_ts: [u8; 7],
    end_ts: [u8; 7],
    //related: u8,
}


async fn fb161_download_velocity(writer: &mut BufWriter<File>) -> Result<u64> {
    let mut block: DownloadBlockVelocity = DownloadBlockVelocity {
        code: 0x01,
        name: *b"velocity-informati",
        len: 0x11,
        num: 0x01,
    };

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
            block.len = (block.num as u32) * (mem::size_of::<DownloadEntryVelocity>()) as u32;
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

#[warn(dead_code)]
async fn test_fb161_download_event(writer: &mut BufWriter<File>) -> Result<u64> {
    let mut block_event: DownloadBlock = DownloadBlock {
        code: 0x00,
        name: *b"event-error-inform",
        len: 0x22,
    };
    let data_event = EventErrorInformation {
        cnt: 8,
        date: vec![0x88, 0x88, 0x88, 0x88, 0x88, 0x88, 0x88],
    };
    if let Ok(prev_pos) = writer.seek(SeekFrom::Current(0)).await {
        let data_ofs = prev_pos + mem::size_of::<DownloadBlock>() as u64;
        let wn = writer.seek(SeekFrom::Start(data_ofs)).await;
        assert!(wn.is_ok());

        println!("[debug] pre_pos-{} data_ofs-{} DownloadBlock-{}", prev_pos, data_ofs, mem::size_of::<DownloadBlock>());

        let data_event = bincode::serialize(&data_event).unwrap();
        let wn = writer.write_all(&data_event).await;
        assert!(wn.is_ok());

        //writer.flush().await.unwrap();
        if let Ok(curr_pos) = writer.seek(SeekFrom::Current(0)).await {
            writer.seek(SeekFrom::Start(prev_pos)).await?;
            block_event.len = data_event.len() as u32;
            let block_event = bincode::serialize(&block_event)?;
            let wn = writer.write_all(&block_event).await;
            assert!(wn.is_ok());

            let _ = writer.seek(SeekFrom::Start(curr_pos)).await;

            Ok(curr_pos - prev_pos)
        }
        else {
            //Err(Error::new(ErrorKind::Other, "current seek error"))
            Err(anyhow!("current seek error"))
        }
    }
    else {
        //Err(Error::new(ErrorKind::Other, "previous seek error"))
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

        let mut entry = DownloadEntryEvent {
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

        let mut entry = DownloadEntryEvent {
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

async fn fb161_sql_conn() -> sqlx::Result<PoolConnection<Sqlite>> {
    dotenv().ok();

    let url: String = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://:memory:".to_string());
    let pool: SqlitePool = SqlitePool::connect(&url).await.unwrap();
    pool.acquire().await
}

#[tokio::test]
async fn test_fb161_download() {
    if let Ok(file)  = File::create("/tmp/fb161_xxxxx_xxxxx.vdr").await {
        let mut writer = BufWriter::new(file);

        {
            //TODO
            let mut block_num = Vec::new();
            WriteBytesExt::write_u16::<LittleEndian>(&mut block_num, 2).unwrap();
            let _ = writer.write_all(&Bytes::from(block_num)).await;
        }
        let _ = fb161_download_event(&mut writer).await;
        let _ = fb161_download_velocity(&mut writer).await;
        {
            //TODO
            let block_checksum = b"\xFA";
            let _ = writer.write_all(block_checksum).await;
        }
        writer.flush().await.unwrap();
    }
    else {
        assert!(false);
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
