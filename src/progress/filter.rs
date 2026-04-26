use super::BackupEvent;
use crate::command::Filter;
use std::io::{self, Read};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

struct CountingReader<R: Read> {
    inner: R,
    sender: Sender<BackupEvent>,
    bytes: u64,
    last_send: Instant,
    total: Option<u64>,
}

impl<R: Read> Read for CountingReader<R> {
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

impl<R: Read> CountingReader<R> {
    fn new(inner: R, sender: Sender<BackupEvent>, total: Option<u64>) -> Self {
        Self {
            inner,
            sender,
            total,
            bytes: 0,
            last_send: Instant::now() - Duration::from_secs(1),
        }
    }
}

pub struct CountingReaderBuilder {
    sender: Sender<BackupEvent>,
    total: Option<u64>,
}

impl Filter for CountingReaderBuilder {
    fn filter(&self, reader: Box<dyn Read>) -> Box<dyn Read> {
        Box::new(CountingReader::new(reader, self.sender.clone(), self.total))
    }
}

impl CountingReaderBuilder {
    pub fn build(sender: Sender<BackupEvent>, total: Option<u64>) -> Box<dyn Filter> {
        Box::new(Self { sender, total })
    }
}
