use std::time::Instant;

#[derive(Clone, Copy)]
struct Sample {
    at: Instant,
    bytes: u64,
}

pub(crate) struct RateTracker {
    samples: Vec<Sample>,
    head: usize,
    count: usize,
    smoothed: f64,
    alpha: f64,
}

impl RateTracker {
    pub(crate) fn new(capacity: usize) -> Self {
        RateTracker {
            samples: vec![
                Sample {
                    at: Instant::now(),
                    bytes: 0,
                };
                capacity
            ],
            head: 0,
            count: 0,
            smoothed: 0.0,
            alpha: 0.05,
        }
    }

    pub(crate) fn push(&mut self, bytes: u64) {
        let sample = Sample {
            at: Instant::now(),
            bytes,
        };
        self.samples[self.head] = sample;
        self.head = (self.head + 1) % self.samples.capacity();
        self.count = (self.count + 1).min(self.samples.capacity())
    }

    pub(crate) fn rate(&mut self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }

        let cap = self.samples.capacity();

        let oldest_idx = (cap + self.head - self.count) % cap;
        let newest_idx = (cap + self.head - 1) % cap;

        let oldest = self.samples[oldest_idx];
        let newest = self.samples[newest_idx];

        let elapsed = newest.at.duration_since(oldest.at).as_secs_f64();
        if elapsed == 0.0 {
            return None;
        }

        // If for any reason oldest < newest, the subtraction will overflow
        if newest.bytes < oldest.bytes {
            return None;
        }

        let rate = (newest.bytes - oldest.bytes) as f64 / elapsed;

        // exponentially weighted moving average to smooth those spikes
        self.smoothed = self.alpha * rate + (1.0 - self.alpha) * self.smoothed;

        Some(self.smoothed)
    }
}
