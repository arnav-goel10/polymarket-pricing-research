//! Append-only recording of raw feed frames, and a reader that replays them.
//!
//! File layout (all integers little-endian):
//!
//! ```text
//! header   8 bytes  magic "POLYREC\0"
//!          2 bytes  format version (currently 1)
//!          2 bytes  reserved, zero
//! record   4 bytes  payload length N (u32, at most MAX_RECORD_BYTES)
//!          4 bytes  CRC-32 (IEEE) of the payload
//!          N bytes  payload:
//!                     8 bytes  receive time, microseconds since the Unix epoch (i64)
//!                     1 byte   source: 1 RTDS, 2 Binance, 3 CLOB book, 4 control
//!                     N-9      the frame exactly as received (UTF-8 text)
//! ```
//!
//! A crash can leave a partial record at the end of the file. The reader
//! reports that as [`ReadError::TruncatedTail`] after yielding every complete
//! record, so a recording cut short by a power loss is still usable. A record
//! whose checksum does not match is never returned.

use std::io::{self, BufWriter, Read, Write};
use std::path::Path;

pub const MAGIC: [u8; 8] = *b"POLYREC\0";
pub const VERSION: u16 = 1;
pub const HEADER_LEN: usize = 12;
/// Payload size cap. Real frames are a few KB; a larger length means the
/// length field itself is corrupt, so the reader stops instead of allocating.
pub const MAX_RECORD_BYTES: u32 = 4 * 1024 * 1024;
const PAYLOAD_PREFIX: usize = 9;

/// Which feed a frame came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Source {
    Rtds = 1,
    Binance = 2,
    ClobBook = 3,
    /// Recorder bookkeeping, e.g. `{"subscribe":"<token id>"}` written each
    /// time the order-book feed (re)subscribes, so replay knows the token.
    Control = 4,
}

impl Source {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(Source::Rtds),
            2 => Some(Source::Binance),
            3 => Some(Source::ClobBook),
            4 => Some(Source::Control),
            _ => None,
        }
    }
}

/// One recorded frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub recv_micros: i64,
    pub source: Source,
    pub frame: String,
}

#[derive(Debug)]
pub enum ReadError {
    Io(io::Error),
    /// The file does not start with the recording magic.
    BadMagic,
    UnsupportedVersion(u16),
    /// The file ends inside a record (for example after a crash). Every
    /// complete record before `offset` was already returned.
    TruncatedTail {
        offset: u64,
    },
    /// The stored checksum does not match the payload.
    ChecksumMismatch {
        offset: u64,
    },
    /// The length field exceeds [`MAX_RECORD_BYTES`] or is too small.
    BadLength {
        offset: u64,
        len: u32,
    },
    /// Unknown source byte or a frame that is not UTF-8.
    BadPayload {
        offset: u64,
    },
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Io(e) => write!(f, "io error: {e}"),
            ReadError::BadMagic => write!(f, "not a recording (bad magic)"),
            ReadError::UnsupportedVersion(v) => write!(f, "unsupported recording version {v}"),
            ReadError::TruncatedTail { offset } => write!(f, "truncated record at byte {offset}"),
            ReadError::ChecksumMismatch { offset } => {
                write!(f, "checksum mismatch in record at byte {offset}")
            }
            ReadError::BadLength { offset, len } => {
                write!(f, "invalid record length {len} at byte {offset}")
            }
            ReadError::BadPayload { offset } => write!(f, "invalid payload at byte {offset}"),
        }
    }
}

impl std::error::Error for ReadError {}

impl From<io::Error> for ReadError {
    fn from(e: io::Error) -> Self {
        ReadError::Io(e)
    }
}

/// Appends records to any writer. Buffered; call [`finish`](Self::finish)
/// to flush (and fsync, for files).
pub struct RecordWriter<W: Write> {
    out: BufWriter<W>,
    records: u64,
    bytes: u64,
}

impl RecordWriter<std::fs::File> {
    /// Create (truncate) a recording file and write the header.
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::new(std::fs::File::create(path)?)
    }

    /// Flush buffers and fsync the file.
    pub fn finish(self) -> io::Result<(u64, u64)> {
        let (records, bytes) = (self.records, self.bytes);
        let file = self.out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        Ok((records, bytes))
    }
}

impl<W: Write> RecordWriter<W> {
    pub fn new(inner: W) -> io::Result<Self> {
        let mut out = BufWriter::with_capacity(64 * 1024, inner);
        out.write_all(&MAGIC)?;
        out.write_all(&VERSION.to_le_bytes())?;
        out.write_all(&0u16.to_le_bytes())?;
        Ok(Self {
            out,
            records: 0,
            bytes: HEADER_LEN as u64,
        })
    }

    pub fn append(&mut self, recv_micros: i64, source: Source, frame: &str) -> io::Result<()> {
        let len = PAYLOAD_PREFIX + frame.len();
        if len > MAX_RECORD_BYTES as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds MAX_RECORD_BYTES",
            ));
        }
        let ts = recv_micros.to_le_bytes();
        let src = [source as u8];
        let mut h = crc32fast::Hasher::new();
        h.update(&ts);
        h.update(&src);
        h.update(frame.as_bytes());
        self.out.write_all(&(len as u32).to_le_bytes())?;
        self.out.write_all(&h.finalize().to_le_bytes())?;
        self.out.write_all(&ts)?;
        self.out.write_all(&src)?;
        self.out.write_all(frame.as_bytes())?;
        self.records += 1;
        self.bytes += 8 + len as u64;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    pub fn into_inner(self) -> io::Result<W> {
        self.out.into_inner().map_err(|e| e.into_error())
    }
}

/// Streams records out of any reader. Iteration stops after the first error.
pub struct RecordReader<R: Read> {
    input: R,
    offset: u64,
    done: bool,
    buf: Vec<u8>,
}

impl RecordReader<io::BufReader<std::fs::File>> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ReadError> {
        Self::new(io::BufReader::with_capacity(
            256 * 1024,
            std::fs::File::open(path)?,
        ))
    }
}

/// Read until `buf` is full or EOF; returns bytes read.
fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(n)
}

impl<R: Read> RecordReader<R> {
    /// Validate the header and position at the first record.
    pub fn new(mut input: R) -> Result<Self, ReadError> {
        let mut h = [0u8; HEADER_LEN];
        if read_full(&mut input, &mut h)? < HEADER_LEN || h[..8] != MAGIC {
            return Err(ReadError::BadMagic);
        }
        let version = u16::from_le_bytes([h[8], h[9]]);
        if version != VERSION {
            return Err(ReadError::UnsupportedVersion(version));
        }
        Ok(Self {
            input,
            offset: HEADER_LEN as u64,
            done: false,
            buf: Vec::with_capacity(8 * 1024),
        })
    }

    fn next_record(&mut self) -> Result<Option<Record>, ReadError> {
        let start = self.offset;
        let mut head = [0u8; 8];
        match read_full(&mut self.input, &mut head)? {
            0 => return Ok(None),
            8 => {}
            _ => return Err(ReadError::TruncatedTail { offset: start }),
        }
        let len = u32::from_le_bytes([head[0], head[1], head[2], head[3]]);
        let crc = u32::from_le_bytes([head[4], head[5], head[6], head[7]]);
        // Some filesystems leave zero-filled blocks after a crash. A zero
        // length never occurs in a real record, so treat it as the tail.
        if len == 0 && crc == 0 {
            return Err(ReadError::TruncatedTail { offset: start });
        }
        if len > MAX_RECORD_BYTES || (len as usize) < PAYLOAD_PREFIX {
            return Err(ReadError::BadLength { offset: start, len });
        }
        self.buf.resize(len as usize, 0);
        if read_full(&mut self.input, &mut self.buf)? < len as usize {
            return Err(ReadError::TruncatedTail { offset: start });
        }
        if crc32fast::hash(&self.buf) != crc {
            return Err(ReadError::ChecksumMismatch { offset: start });
        }
        self.offset += 8 + len as u64;
        let recv_micros = i64::from_le_bytes(self.buf[..8].try_into().expect("8 bytes"));
        let source = Source::from_u8(self.buf[8]).ok_or(ReadError::BadPayload { offset: start })?;
        let frame = std::str::from_utf8(&self.buf[PAYLOAD_PREFIX..])
            .map_err(|_| ReadError::BadPayload { offset: start })?
            .to_owned();
        Ok(Some(Record {
            recv_micros,
            source,
            frame,
        }))
    }

    /// Byte offset of the next record.
    pub fn offset(&self) -> u64 {
        self.offset
    }
}

impl<R: Read> Iterator for RecordReader<R> {
    type Item = Result<Record, ReadError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.next_record() {
            Ok(Some(r)) => Some(Ok(r)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Read every complete record. A truncated tail (crash mid-write) is
/// tolerated and reported in the second value; any other error fails.
pub fn read_all(path: impl AsRef<Path>) -> Result<(Vec<Record>, Option<u64>), ReadError> {
    let mut out = Vec::new();
    for r in RecordReader::open(path)? {
        match r {
            Ok(rec) => out.push(rec),
            Err(ReadError::TruncatedTail { offset }) => return Ok((out, Some(offset))),
            Err(e) => return Err(e),
        }
    }
    Ok((out, None))
}

/// Parse a recording held in memory (used by the fuzz target and tests).
pub fn read_bytes(bytes: &[u8]) -> (Vec<Record>, Option<ReadError>) {
    let mut out = Vec::new();
    let reader = match RecordReader::new(bytes) {
        Ok(r) => r,
        Err(e) => return (out, Some(e)),
    };
    for r in reader {
        match r {
            Ok(rec) => out.push(rec),
            Err(e) => return (out, Some(e)),
        }
    }
    (out, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<u8> {
        let mut w = RecordWriter::new(Vec::new()).unwrap();
        w.append(1_000_000, Source::Rtds, r#"{"topic":"crypto_prices"}"#)
            .unwrap();
        w.append(1_000_100, Source::Binance, "{}").unwrap();
        w.append(1_000_200, Source::ClobBook, "[]").unwrap();
        w.into_inner().unwrap()
    }

    #[test]
    fn round_trips_records() {
        let (recs, err) = read_bytes(&sample());
        assert!(err.is_none());
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[1].source, Source::Binance);
        assert_eq!(recs[2].recv_micros, 1_000_200);
        assert_eq!(recs[0].frame, r#"{"topic":"crypto_prices"}"#);
    }

    #[test]
    fn every_truncation_point_yields_a_clean_prefix() {
        let full = sample();
        let (all, _) = read_bytes(&full);
        // Byte offsets where a record ends (cutting there loses nothing).
        let mut boundaries = vec![HEADER_LEN];
        let mut at = HEADER_LEN;
        while at < full.len() {
            at += 8 + u32::from_le_bytes(full[at..at + 4].try_into().unwrap()) as usize;
            boundaries.push(at);
        }
        for cut in 0..full.len() {
            let (recs, err) = read_bytes(&full[..cut]);
            if cut < HEADER_LEN {
                assert!(matches!(err, Some(ReadError::BadMagic)), "cut {cut}");
                continue;
            }
            assert_eq!(
                recs[..],
                all[..recs.len()],
                "prefix must match at cut {cut}"
            );
            if boundaries.contains(&cut) {
                assert!(err.is_none(), "cut {cut} is a record boundary: {err:?}");
            } else {
                assert!(
                    matches!(err, Some(ReadError::TruncatedTail { .. })),
                    "cut {cut} is mid-record and must be reported: {err:?}"
                );
            }
        }
    }

    #[test]
    fn zero_filled_tail_after_a_crash_is_a_truncated_tail() {
        let mut v = sample();
        let (all, _) = read_bytes(&v);
        v.extend_from_slice(&[0u8; 4096]);
        let (recs, err) = read_bytes(&v);
        assert_eq!(recs, all);
        assert!(
            matches!(err, Some(ReadError::TruncatedTail { .. })),
            "{err:?}"
        );
    }

    #[test]
    fn any_single_bit_flip_in_a_payload_is_caught() {
        let full = sample();
        // Flip each bit of the first record's payload (after its 8-byte head).
        let first_len = u32::from_le_bytes(full[12..16].try_into().unwrap()) as usize;
        for byte in 20..20 + first_len {
            for bit in 0..8 {
                let mut bad = full.clone();
                bad[byte] ^= 1 << bit;
                let (recs, err) = read_bytes(&bad);
                assert!(recs.is_empty(), "corrupt record returned");
                assert!(
                    matches!(err, Some(ReadError::ChecksumMismatch { offset: 12 })),
                    "{err:?}"
                );
            }
        }
    }

    #[test]
    fn rejects_bad_header_version_and_huge_length() {
        assert!(matches!(
            read_bytes(b"NOTAREC\0\x01\x00\x00\x00").1,
            Some(ReadError::BadMagic)
        ));
        let mut v = sample();
        v[8] = 9;
        assert!(matches!(
            read_bytes(&v).1,
            Some(ReadError::UnsupportedVersion(9))
        ));
        let mut v = sample();
        v[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            read_bytes(&v).1,
            Some(ReadError::BadLength { offset: 12, .. })
        ));
    }

    #[test]
    fn file_round_trip_with_fsync() {
        let dir = std::env::temp_dir().join(format!("polyrec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("t.prec");
        let mut w = RecordWriter::create(&path).unwrap();
        for i in 0..1000 {
            w.append(i, Source::ClobBook, &format!("frame {i}"))
                .unwrap();
        }
        let (n, bytes) = w.finish().unwrap();
        assert_eq!(n, 1000);
        assert_eq!(bytes, std::fs::metadata(&path).unwrap().len());
        let (recs, tail) = read_all(&path).unwrap();
        assert_eq!(recs.len(), 1000);
        assert!(tail.is_none());
        assert_eq!(recs[999].frame, "frame 999");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
