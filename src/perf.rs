use std::{collections::HashSet, time::Instant};
use worterbuch_client::TransactionId;

#[derive(Debug)]
pub struct PerformanceData {
    message_counter: u64,
    minute_cursor: usize,
    hour_cursor: usize,
    msg_per_minute: [u64; 60],
    msg_per_hour: [u64; 60],
    last_measurement_second: Instant,
    last_measurement_minute: Instant,
    in_flight_messages: HashSet<TransactionId>,
}

impl Default for PerformanceData {
    fn default() -> Self {
        Self {
            message_counter: 0,
            minute_cursor: 0,
            hour_cursor: 0,
            msg_per_minute: [0; 60],
            msg_per_hour: [0; 60],
            last_measurement_second: Instant::now(),
            last_measurement_minute: Instant::now(),
            in_flight_messages: HashSet::new(),
        }
    }
}

impl PerformanceData {
    pub fn update(&mut self, msg_count: u64) -> Option<(u64, u64, u64, usize)> {
        self.message_counter += msg_count;
        let elapsed = self.last_measurement_second.elapsed().as_secs_f32() as f64;
        if elapsed > 1.0 {
            let msg_per_second = (self.message_counter as f64 / elapsed) as u64;

            self.msg_per_minute[self.minute_cursor] = msg_per_second;
            self.last_measurement_second = Instant::now();
            self.minute_cursor = self.inc_cursor(self.minute_cursor);

            let msg_per_minute = self.msg_per_minute.iter().sum();
            self.msg_per_hour[self.hour_cursor] =
                (self.msg_per_hour[self.hour_cursor] + msg_per_minute) / 2;

            let elapsed_minute = self.last_measurement_minute.elapsed().as_secs_f32() as f64 / 60.0;
            if elapsed_minute > 1.0 {
                self.last_measurement_minute = Instant::now();
                self.hour_cursor = self.inc_cursor(self.hour_cursor);
            }

            let msg_per_hour = self.msg_per_hour.iter().sum();

            self.message_counter = 0;

            let in_fligh_messages = self.in_flight_messages.len();

            Some((
                msg_per_second,
                msg_per_minute,
                msg_per_hour,
                in_fligh_messages,
            ))
        } else {
            None
        }
    }

    pub fn message_queued(&mut self, transaction_id: TransactionId) {
        self.in_flight_messages.insert(transaction_id);
    }

    pub fn message_acked(&mut self, transaction_id: TransactionId) {
        self.in_flight_messages.remove(&transaction_id);
    }

    fn inc_cursor(&self, cursor: usize) -> usize {
        if cursor < 59 {
            cursor + 1
        } else {
            0
        }
    }
}
