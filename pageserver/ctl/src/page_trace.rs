use std::collections::HashMap;
use std::io::{Cursor, ErrorKind, Read};
use std::time::SystemTime;

use camino::Utf8PathBuf;
use clap::Parser;
use pageserver_api::key::Key;
use pageserver_api::models::PageTraceEvent;
use pageserver_api::reltag::RelTag;
use utils::lsn::Lsn;

#[derive(Parser)]
pub(crate) struct PageTraceCmd {
    /// Trace input data path
    path: Utf8PathBuf,
}

pub(crate) async fn main(cmd: &PageTraceCmd) -> anyhow::Result<()> {
    let data = tokio::fs::read(&cmd.path).await?;

    let mut reader = Cursor::new(&data);

    let mut events = Vec::new();

    let event_size = bincode::serialized_size(&PageTraceEvent {
        key: 0.into(),
        effective_lsn: Lsn(0),
        time: SystemTime::now(),
    })?;

    loop {
        let mut event_bytes = vec![0; event_size as usize];
        if let Err(e) = reader.read_exact(&mut event_bytes) {
            if e.kind() == ErrorKind::UnexpectedEof {
                eprintln!("End of input, read {} keys", events.len());
                break;
            } else {
                return Err(e.into());
            }
        }
        let event = bincode::deserialize::<PageTraceEvent>(&event_bytes)?;

        events.push(event);
    }

    let mut reads_by_relation = HashMap::new();
    let mut reads_by_key = HashMap::new();

    for event in events {
        let key = Key::from_compact(event.key);
        let reltag = RelTag {
            spcnode: key.field2,
            dbnode: key.field3,
            relnode: key.field4,
            forknum: key.field5,
        };

        *(reads_by_relation.entry(reltag).or_insert(0)) += 1;
        *(reads_by_key.entry(event.key).or_insert(0)) += 1;
    }

    let mut multi_read_keys = Vec::new();
    for (key, count) in reads_by_key {
        if count > 1 {
            multi_read_keys.push((key, count));
        }
    }

    multi_read_keys.sort_by_key(|i| (i.1, i.0));

    eprintln!("Multi-read keys: {}", multi_read_keys.len());
    for (key, count) in multi_read_keys {
        eprintln!("  {}: {}", key, count);
    }

    eprintln!("Reads by relation:");
    let mut reads_by_relation = reads_by_relation.into_iter().collect::<Vec<_>>();
    reads_by_relation.sort_by_key(|i| (i.1, i.0));
    for (reltag, count) in reads_by_relation {
        eprintln!("  {}: {}", reltag, count);
    }

    Ok(())
}
