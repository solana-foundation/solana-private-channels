//! Proves a `pg_dump -Fc` archive can restore every row a truncation is about to delete.
//!
//! The archive is read once, from one open handle, through `pg_restore`. Anything the
//! parser does not recognise fails the proof, because only a proof may authorize deletion.

use {
    anyhow::{anyhow, bail, Context, Result},
    sha2::{Digest, Sha256},
    std::{
        collections::HashSet,
        fs::File,
        io::{self, BufRead, BufReader, Read, Write},
        path::Path,
        process::{ChildStdin, Command, Stdio},
        thread,
    },
};

/// Longest line or kept field the parser buffers; block rows are skipped, never buffered.
const MAX_LINE: usize = 64 * 1024;
/// Most of `pg_restore`'s stderr kept for the refusal message.
const MAX_STDERR: usize = 64 * 1024;

const BLOCKS: &str = "public.blocks";
const TRANSACTIONS: &str = "public.transactions";
const METADATA: &str = "public.metadata";
const ACCOUNT_HISTORY: &str = "public.account_history";
const DEPLOYMENT_ID_KEY: &[u8] = b"deployment_id";

/// What the live database holds for the range about to be deleted.
#[derive(Debug, Clone)]
pub struct LiveLedger {
    pub deployment_id: Vec<u8>,
    pub truncate_before_slot: u64,
    /// Oldest live block; dump blocks below it were already truncated and are not counted.
    pub live_min_slot: u64,
    pub live_block_count: u64,
    /// `None` when the live database has no `account_history` table.
    pub account_history_count: Option<u64>,
    /// Oldest live `account_history` slot, for the same reason as `live_min_slot`.
    pub account_history_min_slot: u64,
}

/// What the restore stream says about the archive.
#[derive(Debug, Default, PartialEq, Eq)]
struct DumpFacts {
    tables: HashSet<String>,
    deployment_id: Option<Vec<u8>>,
    blocks_in_range: u64,
    account_history_below: u64,
}

/// Run the proof and return the SHA-256 of the exact bytes proven. Blocking; the
/// caller runs it off the async runtime.
pub fn prove_dump(pg_restore_bin: &Path, dump_path: &Path, live: &LiveLedger) -> Result<String> {
    let file = File::open(dump_path)
        .with_context(|| format!("Cannot open pg_dump file '{}'", dump_path.display()))?;
    if !file.metadata()?.is_file() {
        bail!("pg_dump path '{}' is not a file", dump_path.display());
    }

    // No shell, fixed arguments, and the dump arrives on stdin, so no path reaches argv.
    let mut child = Command::new(pg_restore_bin)
        .arg("--file=-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| match e.kind() {
            io::ErrorKind::NotFound => {
                anyhow!("pg_restore not found at '{}'", pg_restore_bin.display())
            }
            _ => anyhow!("Failed to start '{}': {e}", pg_restore_bin.display()),
        })?;
    let (Some(stdin), Some(stdout), Some(stderr)) =
        (child.stdin.take(), child.stdout.take(), child.stderr.take())
    else {
        let _ = child.kill();
        let _ = child.wait();
        bail!("pg_restore pipes were not opened");
    };

    let feeder = thread::spawn(move || feed(file, stdin));
    let drainer = thread::spawn(move || drain(stderr));

    let facts = parse_restore_output(BufReader::with_capacity(MAX_LINE, stdout), live);
    let facts = match facts {
        Ok(facts) => facts,
        Err(e) => {
            // Its threads are left to finish on their own: a grandchild could still hold the pipes.
            let _ = child.kill();
            let _ = child.wait();
            return Err(e);
        }
    };
    let status = child.wait().context("Failed to wait for pg_restore")?;
    let fed = feeder
        .join()
        .map_err(|_| anyhow!("pg_restore feeder thread panicked"))?;
    let stderr = drainer
        .join()
        .map_err(|_| anyhow!("pg_restore stderr thread panicked"))?;

    // Partial output before a failure proves nothing, so the verdict waits for the exit.
    if !status.success() || !stderr.is_empty() {
        bail!(
            "pg_restore could not read the archive ({status}): {}",
            String::from_utf8_lossy(&stderr).trim()
        );
    }
    let sha256 = fed.context("Failed to feed the dump to pg_restore")?;
    decide(&facts, live)?;
    Ok(sha256)
}

/// Copy the dump into pg_restore and hash exactly the bytes it was given.
fn feed(mut file: File, mut stdin: ChildStdin) -> io::Result<String> {
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; MAX_LINE];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        stdin.write_all(&buf[..n])?;
    }
    Ok(hex::encode(hasher.finalize()))
}

/// Keep the first `MAX_STDERR` bytes and discard the rest, so the child never blocks on it.
fn drain(mut stderr: impl Read) -> Vec<u8> {
    let mut kept = Vec::new();
    let _ = stderr
        .by_ref()
        .take(MAX_STDERR as u64)
        .read_to_end(&mut kept);
    let _ = io::copy(&mut stderr, &mut io::sink());
    kept
}

/// A proof passes only if every check holds.
fn decide(facts: &DumpFacts, live: &LiveLedger) -> Result<()> {
    let mut required = vec![BLOCKS, TRANSACTIONS, METADATA];
    if live.account_history_count.is_some() {
        required.push(ACCOUNT_HISTORY);
    }
    for table in required {
        if !facts.tables.contains(table) {
            bail!("the dump has no data section for {table}");
        }
    }

    let dump_id = facts
        .deployment_id
        .as_ref()
        .ok_or_else(|| anyhow!("the dump has no deployment_id"))?;
    if *dump_id != live.deployment_id {
        bail!("the dump is of another ledger (deployment_id does not match)");
    }

    // Truncation only deletes below the live minimum, so equal counts mean equal sets.
    if facts.blocks_in_range != live.live_block_count {
        bail!(
            "the dump holds {} of the {} blocks in slots {}..{} this run would delete",
            facts.blocks_in_range,
            live.live_block_count,
            live.live_min_slot,
            live.truncate_before_slot
        );
    }
    if let Some(expected) = live.account_history_count {
        if facts.account_history_below != expected {
            bail!(
                "the dump holds {} of the {expected} account_history rows in slots {}..{} \
                 this run would delete",
                facts.account_history_below,
                live.account_history_min_slot,
                live.truncate_before_slot
            );
        }
    }
    Ok(())
}

/// How one COPY section's rows are read.
enum Section {
    /// Only the `slot` column is read; counted when `min <= slot < below`.
    Slots {
        table: &'static str,
        index: usize,
        min: u64,
        below: u64,
    },
    Metadata {
        key: usize,
        value: usize,
    },
    Skip,
}

/// Read pg_restore's SQL stream and collect the facts `decide` needs.
fn parse_restore_output<R: BufRead>(mut reader: R, live: &LiveLedger) -> Result<DumpFacts> {
    let mut facts = DumpFacts::default();
    let mut line = Vec::new();
    while read_line_bounded(&mut reader, &mut line)? {
        let Some((table, columns)) = parse_copy_header(&line)? else {
            continue;
        };
        let column = |name: &str| {
            columns
                .iter()
                .position(|c| c == name)
                .ok_or_else(|| anyhow!("{table} in the dump has no {name} column"))
        };
        let section = match table.as_str() {
            BLOCKS => Section::Slots {
                table: BLOCKS,
                index: column("slot")?,
                min: live.live_min_slot,
                below: live.truncate_before_slot,
            },
            ACCOUNT_HISTORY => Section::Slots {
                table: ACCOUNT_HISTORY,
                index: column("slot")?,
                min: live.account_history_min_slot,
                below: live.truncate_before_slot,
            },
            METADATA => Section::Metadata {
                key: column("key")?,
                value: column("value")?,
            },
            _ => Section::Skip,
        };
        read_copy_rows(&mut reader, &section, &mut facts)?;
        facts.tables.insert(table);
    }
    Ok(facts)
}

/// Read one COPY section's rows up to its `\.` terminator.
fn read_copy_rows<R: BufRead>(
    reader: &mut R,
    section: &Section,
    facts: &mut DumpFacts,
) -> Result<()> {
    loop {
        let (first, mut ended) = next_field(reader, Some(MAX_LINE))?;
        if ended && first == b"\\." {
            return Ok(());
        }
        match section {
            Section::Skip => {}
            Section::Slots {
                table,
                index,
                min,
                below,
            } => {
                let raw = if *index == 0 {
                    first
                } else {
                    for _ in 1..*index {
                        expect_more(ended, table)?;
                        ended = next_field(reader, None)?.1;
                    }
                    expect_more(ended, table)?;
                    let (raw, row_ended) = next_field(reader, Some(32))?;
                    ended = row_ended;
                    raw
                };
                let slot = parse_slot(&raw, table)?;
                if (*min..*below).contains(&slot) {
                    if *table == BLOCKS {
                        facts.blocks_in_range += 1;
                    } else {
                        facts.account_history_below += 1;
                    }
                }
            }
            Section::Metadata { key, value } => {
                let mut fields = vec![first];
                while !ended {
                    let (field, row_ended) = next_field(reader, Some(MAX_LINE))?;
                    fields.push(field);
                    ended = row_ended;
                }
                let (Some(raw_key), Some(raw_value)) = (fields.get(*key), fields.get(*value))
                else {
                    bail!("metadata row in the dump is missing columns");
                };
                if unescape_copy(raw_key)?.as_deref() == Some(DEPLOYMENT_ID_KEY) {
                    if facts.deployment_id.is_some() {
                        bail!("the dump holds more than one deployment_id");
                    }
                    facts.deployment_id = Some(decode_bytea(raw_value)?);
                }
            }
        }
        if !ended {
            reader
                .skip_until(b'\n')
                .context("Failed to read the restore output")?;
        }
    }
}

fn expect_more(ended: bool, table: &str) -> Result<()> {
    if ended {
        bail!("{table} row in the dump has fewer columns than its header");
    }
    Ok(())
}

/// `COPY public.t (a, b) FROM stdin;` as the table and its columns; `None` for any other line.
fn parse_copy_header(line: &[u8]) -> Result<Option<(String, Vec<String>)>> {
    let Some(rest) = line.strip_prefix(b"COPY ") else {
        return Ok(None);
    };
    let text = std::str::from_utf8(rest).context("COPY header in the dump is not UTF-8")?;
    let Some(text) = text.strip_suffix(" FROM stdin;") else {
        bail!("unrecognised COPY header in the dump: {text}");
    };
    let (table, columns) = text
        .split_once(" (")
        .and_then(|(table, cols)| Some((table, cols.strip_suffix(')')?)))
        .ok_or_else(|| anyhow!("COPY header without a column list: {text}"))?;
    let columns = columns
        .split(", ")
        .map(|c| c.trim_matches('"').to_string())
        .collect();
    Ok(Some((table.to_string(), columns)))
}

/// Read one line of at most `MAX_LINE` bytes, without its newline. `false` at the end.
fn read_line_bounded<R: BufRead>(reader: &mut R, buf: &mut Vec<u8>) -> Result<bool> {
    buf.clear();
    let n = reader
        .by_ref()
        .take(MAX_LINE as u64 + 1)
        .read_until(b'\n', buf)
        .context("Failed to read the restore output")?;
    if n == 0 {
        return Ok(false);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
    } else if buf.len() > MAX_LINE {
        bail!("a line in the restore output is over {MAX_LINE} bytes");
    }
    Ok(true)
}

/// Consume one tab-separated COPY field, keeping at most `keep` bytes of it (`None` keeps
/// nothing). Returns the kept bytes and whether the field ended the row.
fn next_field<R: BufRead>(reader: &mut R, keep: Option<usize>) -> Result<(Vec<u8>, bool)> {
    let mut kept = Vec::new();
    loop {
        let available = reader
            .fill_buf()
            .context("Failed to read the restore output")?;
        if available.is_empty() {
            bail!("the restore output ended inside a COPY section");
        }
        let end = available.iter().position(|&b| b == b'\t' || b == b'\n');
        let take = end.unwrap_or(available.len());
        if let Some(limit) = keep {
            if kept.len() + take > limit {
                bail!("a field in the restore output is over {limit} bytes");
            }
            kept.extend_from_slice(&available[..take]);
        }
        match end {
            Some(i) => {
                let row_ended = available[i] == b'\n';
                reader.consume(i + 1);
                return Ok((kept, row_ended));
            }
            None => reader.consume(take),
        }
    }
}

fn parse_slot(raw: &[u8], table: &str) -> Result<u64> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| {
            anyhow!(
                "{table} row in the dump has a bad slot: {}",
                String::from_utf8_lossy(raw)
            )
        })
}

/// Undo COPY text escaping. `None` is SQL NULL. Escapes pg_dump never writes are refused.
fn unescape_copy(raw: &[u8]) -> Result<Option<Vec<u8>>> {
    if raw == b"\\N" {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut bytes = raw.iter();
    while let Some(&b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        out.push(match bytes.next() {
            Some(b'\\') => b'\\',
            Some(b't') => b'\t',
            Some(b'n') => b'\n',
            Some(b'r') => b'\r',
            other => bail!("unsupported COPY escape in the dump: {other:?}"),
        });
    }
    Ok(Some(out))
}

/// A hex-format bytea field. Escape format is refused rather than guessed at.
fn decode_bytea(raw: &[u8]) -> Result<Vec<u8>> {
    let text = unescape_copy(raw)?.ok_or_else(|| anyhow!("deployment_id in the dump is NULL"))?;
    let hex_digits = text
        .strip_prefix(b"\\x")
        .ok_or_else(|| anyhow!("deployment_id in the dump is not hex-format bytea"))?;
    hex::decode(hex_digits).context("deployment_id in the dump is not valid hex")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const ID_HEX: &str = "0102ff";

    /// pg_restore's SQL for a small ledger: blocks 1..=5, one account_history row per slot.
    fn restore_sql(extra_metadata: &str) -> String {
        format!(
            "--\n-- PostgreSQL database dump\n--\n\
             CREATE TABLE public.blocks (\n    slot bigint NOT NULL,\n    data bytea NOT NULL\n);\n\
             COPY public.accounts (pubkey, data) FROM stdin;\n\\\\x01\t\\\\x02\n\\.\n\n\
             COPY public.blocks (slot, data) FROM stdin;\n\
             1\t\\\\x0a\n2\t\\\\x0b\n3\t\\\\x0c\n4\t\\\\x0d\n5\t\\\\x0e\n\\.\n\n\
             COPY public.transactions (signature, data) FROM stdin;\n\\\\x01\t\\\\x01\n\\.\n\n\
             COPY public.account_history (id, slot, data) FROM stdin;\n\
             1\t1\t\\\\x01\n2\t2\t\\\\x02\n3\t3\t\\\\x03\n\\.\n\n\
             COPY public.metadata (key, value) FROM stdin;\n\
             deployment_id\t\\\\x{ID_HEX}\nk\\\\tx\t\\\\x00\n{extra_metadata}\\.\n\n\
             ALTER TABLE ONLY public.blocks\n    ADD CONSTRAINT blocks_pkey PRIMARY KEY (slot);\n"
        )
    }

    fn parse(sql: &str, below: u64, min: u64) -> Result<DumpFacts> {
        let mut ledger = live(0, None);
        ledger.truncate_before_slot = below;
        ledger.live_min_slot = min;
        ledger.account_history_min_slot = min;
        parse_restore_output(Cursor::new(sql.as_bytes().to_vec()), &ledger)
    }

    fn live(block_count: u64, history: Option<u64>) -> LiveLedger {
        LiveLedger {
            deployment_id: hex::decode(ID_HEX).unwrap(),
            truncate_before_slot: 3,
            live_min_slot: 1,
            live_block_count: block_count,
            account_history_count: history,
            account_history_min_slot: 1,
        }
    }

    #[test]
    fn parses_copy_sections() {
        let facts = parse(&restore_sql(""), 3, 1).unwrap();

        let tables: HashSet<String> = [
            "public.accounts",
            BLOCKS,
            TRANSACTIONS,
            ACCOUNT_HISTORY,
            METADATA,
        ]
        .map(String::from)
        .into();
        assert_eq!(facts.tables, tables);
        assert_eq!(facts.deployment_id, Some(hex::decode(ID_HEX).unwrap()));
        assert_eq!(facts.blocks_in_range, 2, "slots 1 and 2 are below 3");
        // account_history's slot is its second column, so the index must come from the header.
        assert_eq!(facts.account_history_below, 2);
        assert_eq!(parse(&restore_sql(""), 5, 3).unwrap().blocks_in_range, 2);
    }

    #[test]
    fn rejects_unknowns() {
        let cases = [
            (
                "escape-format bytea",
                restore_sql("").replace(
                    &format!("deployment_id\t\\\\x{ID_HEX}"),
                    "deployment_id\t\\\\001",
                ),
                "not hex-format",
            ),
            (
                "missing deployment_id",
                restore_sql("").replace("deployment_id\t", "other_key\t"),
                "no deployment_id",
            ),
            (
                "duplicate deployment_id",
                restore_sql("deployment_id\t\\\\x01\n"),
                "more than one",
            ),
            (
                "bad hex",
                restore_sql("").replace(&format!("\\\\x{ID_HEX}"), "\\\\x0g"),
                "not valid hex",
            ),
            (
                "block table without a slot column",
                restore_sql("").replace("public.blocks (slot, data)", "public.blocks (id, data)"),
                "no slot column",
            ),
            (
                "metadata line over 64 KiB",
                restore_sql(&format!("big\t\\\\x{}\n", "00".repeat(MAX_LINE))),
                "over",
            ),
            (
                "output ends inside a section",
                "COPY public.blocks (slot, data) FROM stdin;\n1\t\\\\x0a\n".to_string(),
                "ended inside",
            ),
        ];
        for (label, sql, needle) in cases {
            let verdict = parse(&sql, 3, 1).and_then(|facts| decide(&facts, &live(2, Some(2))));
            let err = verdict.expect_err(label).to_string();
            assert!(err.contains(needle), "{label}: {err}");
        }
    }

    #[test]
    fn decide_boundaries() {
        let facts = parse(&restore_sql(""), 3, 1).unwrap();
        assert!(decide(&facts, &live(2, Some(2))).is_ok());
        assert!(decide(&facts, &live(1, Some(2))).is_err(), "live has fewer");
        assert!(decide(&facts, &live(3, Some(2))).is_err(), "live has more");
        assert!(decide(&facts, &live(2, Some(3))).is_err(), "history short");

        // A dump reused after an earlier run still holds slot 1, which is no longer live.
        let reused = parse(&restore_sql(""), 3, 2).unwrap();
        let mut after_first_run = live(1, Some(1));
        after_first_run.live_min_slot = 2;
        after_first_run.account_history_min_slot = 2;
        assert!(decide(&reused, &after_first_run).is_ok());

        let mut no_history = parse(&restore_sql(""), 3, 1).unwrap();
        no_history.tables.remove(ACCOUNT_HISTORY);
        assert!(decide(&no_history, &live(2, Some(2))).is_err());
        assert!(decide(&no_history, &live(2, None)).is_ok());

        let mut other = live(2, Some(2));
        other.deployment_id = vec![9];
        assert!(decide(&facts, &other).is_err(), "another ledger");
    }

    /// A reader that yields one `blocks` row of `len` bytes without ever holding it.
    struct HugeRow {
        prefix: Vec<u8>,
        remaining: usize,
        suffix: Vec<u8>,
    }

    impl Read for HugeRow {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if !self.prefix.is_empty() {
                let n = self.prefix.len().min(buf.len());
                buf[..n].copy_from_slice(&self.prefix[..n]);
                self.prefix.drain(..n);
                return Ok(n);
            }
            if self.remaining > 0 {
                let n = self.remaining.min(buf.len());
                buf[..n].fill(b'a');
                self.remaining -= n;
                return Ok(n);
            }
            let n = self.suffix.len().min(buf.len());
            buf[..n].copy_from_slice(&self.suffix[..n]);
            self.suffix.drain(..n);
            Ok(n)
        }
    }

    /// A block row is far larger than any line the parser buffers, so it must be skipped
    /// in place. Buffering it would trip the 64 KiB line bound.
    #[test]
    fn huge_block_line_is_not_buffered() {
        let reader = HugeRow {
            prefix: b"COPY public.blocks (slot, data) FROM stdin;\n1\t\\\\x".to_vec(),
            remaining: 50 * 1024 * 1024,
            suffix: b"\n\\.\n".to_vec(),
        };
        let facts =
            parse_restore_output(BufReader::with_capacity(MAX_LINE, reader), &live(0, None))
                .unwrap();
        assert_eq!(facts.blocks_in_range, 1);
    }

    #[test]
    fn missing_pg_restore_fails_closed() {
        let dump = tempfile::NamedTempFile::new().unwrap();
        let bin = Path::new("/nonexistent/pg_restore");
        let err = prove_dump(bin, dump.path(), &live(0, None)).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/pg_restore"), "{err}");
    }

    /// A parse failure must not wait for pg_restore to finish on its own.
    #[test]
    fn child_is_killed_on_parse_failure() {
        let bin = crate::test_helpers::executable_script(
            "#!/bin/sh\nprintf 'COPY public.metadata (key, value) FROM stdin;\\n\
             deployment_id\\t\\\\\\\\x0g\\n'\nsleep 60\n",
        );
        let dump = tempfile::NamedTempFile::new().unwrap();

        let started = std::time::Instant::now();
        let err = prove_dump(&bin, dump.path(), &live(0, None)).unwrap_err();
        assert!(err.to_string().contains("not valid hex"), "{err}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the child must be killed, not waited out"
        );
    }
}
