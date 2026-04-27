use super::BackupEvent;
use crate::command::Filter;
use std::io::{self, Read};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

struct ByteCountReader<R: Read> {
    inner: R,
    sender: Sender<BackupEvent>,
    bytes: u64,
    last_send: Instant,
    total: Option<u64>,
}

impl<R: Read> Read for ByteCountReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.bytes += n as u64;
        if self.last_send.elapsed().as_millis() >= 100 {
            self.sender
                .send(BackupEvent::BytesTransferred {
                    bytes: self.bytes,
                    estimated_total: self.total,
                })
                .ok();
            self.last_send = Instant::now();
        }
        Ok(n)
    }
}

impl<R: Read> ByteCountReader<R> {
    fn new(inner: R, sender: Sender<BackupEvent>, total: Option<u64>) -> Self {
        Self {
            inner,
            sender,
            bytes: 0,
            last_send: Instant::now() - Duration::from_secs(1),
            total,
        }
    }
}

pub(crate) struct ByteCountFilter {
    sender: Sender<BackupEvent>,
    total: Option<u64>,
}

impl ByteCountFilter {
    pub(crate) fn new(sender: Sender<BackupEvent>, total: Option<u64>) -> Self {
        Self { sender, total }
    }
}

impl Filter for ByteCountFilter {
    fn filter(&self, inner: Box<dyn Read>) -> Box<dyn Read> {
        Box::new(ByteCountReader::new(inner, self.sender.clone(), self.total))
    }
}
