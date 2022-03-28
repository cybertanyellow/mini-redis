use tokio::io::{self, AsyncWriteExt, AsyncReadExt};
use tokio::fs::File;
use std::env;
use bytes::Bytes;
use sqlx::sqlite::{*, SqlitePool};
use sqlx::pool::PoolConnection;
//use sqlx::types::chrono::{DateTime, Utc};
use sqlx::types::chrono::{NaiveDateTime, NaiveDate};
use serde::{Deserialize, Serialize};
//use chrono::serde::{/*ts_milliseconds, */ts_seconds};
use chrono::{Timelike, Datelike};
use tokio_stream::StreamExt;
use dotenv::dotenv;
use byteorder::{BigEndian, LittleEndian, WriteBytesExt, ReadBytesExt};
//use bincode::{config, Decode, Encode};
use std::convert::TryInto;

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

async fn select_velocity() -> (Option<NaiveDateTime>, Vec<u8>) {
    dotenv().ok();

    let mut speeds: Vec<u8> = Vec::new();
    //let mut ts = NaiveDateTime::parse_from_str("1970-01-01 00:00:00.000", "%Y-%m-%d %H:%M:%S%.3f");
    let mut ndt = None;

    let url: String = env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://:memory:".to_string());
    let pool: SqlitePool = SqlitePool::connect(&url).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();

    let mut s = sqlx::query!(r#"select speed,timestamp from velocity limit 65535"#).fetch(&mut conn);
    while let Some(row) = s.try_next().await.unwrap() {
        let speed: u8 = row.speed.unwrap_or(0.0) as u8;
        if let Some(tss) = row.timestamp {
            /*if let Ok(ts) = NaiveDateTime::parse_from_str(&tss, "%Y-%m-%d %H:%M:%S%.3f") {
                //println!("{} {}{}{}{}{}{}", speed, ts.year(), ts.month(), ts.day(), ts.hour(), ts.minute(), ts.second());
            }*/
            ndt = NaiveDateTime::parse_from_str(&tss, "%Y-%m-%d %H:%M:%S%.3f").ok();
        }
        speeds.push(speed);
    }

    (ndt, speeds)
}

#[tokio::test]
async fn test_fb161_dl_velocity() {
    let mut file = File::create("/tmp/fb161_velocity.txt").await.unwrap();

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
    let code: u8 = 0x01;
    WriteBytesExt::write_u8(&mut wtr, code).unwrap();
    //file.write_u8(code).await.unwrap();
    let name = b"detail-velocity-information";
    //file.write_all(name).await.unwrap(); /* chinese? */
    wtr.append(&mut name.to_vec());

    if let (Some(_dt), mut speeds) = select_velocity().await {
        // len
        //let mut wtr = Vec::new();
        WriteBytesExt::write_u16::<BigEndian>(&mut wtr, speeds.len().try_into().unwrap()).unwrap();
        //file.write_all(&wtr).await.unwrap();
        //file.write_all(&speeds).await.unwrap();
        wtr.append(&mut speeds);
    }
    else {
    }

    // checksum
    //let mut wtr = Vec::new();
    WriteBytesExt::write_u16::<BigEndian>(&mut wtr, 1).unwrap();

    file.write_all(&wtr).await.unwrap();
}
