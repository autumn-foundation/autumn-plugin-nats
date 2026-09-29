//! Tests for the `JetStream` helpers.

use tokio::io::AsyncReadExt as _;

use super::SliceReader;

#[tokio::test]
async fn slice_reader_reads_all_bytes() {
    let data = b"hello nats";
    let mut reader = SliceReader(data);
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, data);
}

#[tokio::test]
async fn slice_reader_handles_empty_input() {
    let mut reader = SliceReader(b"");
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    assert!(out.is_empty());
}
