//! Writes user_daily/ and campaign_daily/ as uncompressed Parquet.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use parquet::schema::types::Type;

use crate::{key_bytes, CampStat, Ctx, Longs, PersonOut, UserRow};

const USER_SCHEMA: &str = "message user_daily {
    REQUIRED BYTE_ARRAY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CAMPAIGN_SCHEMA: &str = "message campaign_daily {
    REQUIRED BYTE_ARRAY campaign_id (UTF8);
    REQUIRED BYTE_ARRAY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

enum Col {
    Bytes(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn write_file(path: &Path, schema: Arc<Type>, cols: Vec<Col>) {
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::None)
            .build(),
    );
    let file = fs::File::create(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let mut writer = SerializedFileWriter::new(file, schema, props).expect("parquet writer");
    let mut rg = writer.next_row_group().expect("row group");
    let mut cols = cols.into_iter();
    while let Some(mut cw) = rg.next_column().expect("column") {
        match cols.next().expect("column data") {
            Col::Bytes(v, def) => {
                cw.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None).expect("write");
            }
            Col::I32(v) => {
                cw.typed::<Int32Type>().write_batch(&v, None, None).expect("write");
            }
            Col::I64(v) => {
                cw.typed::<Int64Type>().write_batch(&v, None, None).expect("write");
            }
        }
        cw.close().expect("close column");
    }
    rg.close().expect("close row group");
    writer.close().expect("close file");
}

fn user_cols(rows: &[UserRow], ctx: &Ctx, longs: &Longs) -> Vec<Col> {
    let mut buf = [0u8; 16];
    let ids = rows.iter().map(|r| ByteArray::from(key_bytes(r.person, &mut buf, longs).to_vec())).collect();
    let cats: Vec<ByteArray> = ctx.categories.iter().map(|c| ByteArray::from(c.to_vec())).collect();
    let tops = rows.iter().filter(|r| r.top >= 0).map(|r| cats[r.top as usize].clone()).collect();
    let def = rows.iter().map(|r| (r.top >= 0) as i16).collect();
    vec![
        Col::Bytes(ids, None),
        Col::I32(rows.iter().map(|r| r.day).collect()),
        Col::I64(rows.iter().map(|r| r.sessions).collect()),
        Col::I64(rows.iter().map(|r| r.events).collect()),
        Col::I64(rows.iter().map(|r| r.revenue).collect()),
        Col::Bytes(tops, Some(def)),
    ]
}

pub fn write_all(out: &Path, ctx: &Ctx, longs: &Longs, parts: &mut [PersonOut]) {
    let user_dir = out.join("user_daily");
    let camp_dir = out.join("campaign_daily");
    fs::create_dir_all(&user_dir).expect("mkdir user_daily");
    fs::create_dir_all(&camp_dir).expect("mkdir campaign_daily");
    let user_schema = Arc::new(parse_message_type(USER_SCHEMA).unwrap());
    let camp_schema = Arc::new(parse_message_type(CAMPAIGN_SCHEMA).unwrap());

    // Campaign stats are summed over threads; one small file.
    let nd = ctx.ndays;
    let mut total = vec![CampStat::default(); (ctx.organic() + 1) * nd];
    for p in parts.iter() {
        for (t, s) in total.iter_mut().zip(&p.camps) {
            t.sessions += s.sessions;
            t.purchasing += s.purchasing;
            t.revenue += s.revenue;
            t.assisted += s.assisted;
        }
    }

    let users: Vec<&[UserRow]> = parts.iter().map(|p| &p.users[..]).collect();
    std::thread::scope(|sc| {
        for (i, rows) in users.iter().enumerate() {
            if rows.is_empty() && i != 0 {
                continue;
            }
            let path = user_dir.join(format!("part-{i:05}.parquet"));
            let schema = user_schema.clone();
            sc.spawn(move || write_file(&path, schema, user_cols(rows, ctx, longs)));
        }

        let (mut ids, mut channels, mut days) = (Vec::new(), Vec::new(), Vec::new());
        let (mut sessions, mut purchasing, mut revenue, mut assisted) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for c in 0..=ctx.organic() {
            let (id, channel): (&[u8], &[u8]) = if c == ctx.organic() {
                (b"organic", b"organic")
            } else {
                (ctx.campaign_ids[c], ctx.channels[c])
            };
            for d in 0..nd {
                let s = total[c * nd + d];
                if s.sessions == 0 && s.assisted == 0 {
                    continue;
                }
                ids.push(ByteArray::from(id.to_vec()));
                channels.push(ByteArray::from(channel.to_vec()));
                days.push((ctx.day0 + d as i64) as i32);
                sessions.push(s.sessions);
                purchasing.push(s.purchasing);
                revenue.push(s.revenue);
                assisted.push(s.assisted);
            }
        }
        write_file(
            &camp_dir.join("part-00000.parquet"),
            camp_schema,
            vec![
                Col::Bytes(ids, None),
                Col::Bytes(channels, None),
                Col::I32(days),
                Col::I64(sessions),
                Col::I64(purchasing),
                Col::I64(revenue),
                Col::I64(assisted),
            ],
        );
    });
}
