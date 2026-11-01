//! Bounded node delivery. Critical queue failure invalidates the whole session;
//! clients reconnect and rebuild presence, signaling and holder registrations.
use axum::extract::ws::Message;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Instant;
use tokio::sync::{mpsc, Notify};

// Per sending node/session, bounded by both message count and payload bytes.
// This is runtime-only; replacing an active socket preserves its budget.
#[derive(Clone)]
pub(crate) struct SignalBudget {
    messages: f64,
    bytes: f64,
    updated: Instant,
}
impl Default for SignalBudget {
    fn default() -> Self {
        Self {
            messages: 128.0,
            bytes: 524_288.0,
            updated: Instant::now(),
        }
    }
}
impl SignalBudget {
    pub(crate) fn allow(&mut self, bytes: usize, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        self.updated = now;
        self.messages = (self.messages + elapsed * 32.0).min(128.0);
        self.bytes = (self.bytes + elapsed * 131_072.0).min(524_288.0);
        if self.messages < 1.0 || self.bytes < bytes as f64 {
            return false;
        }
        self.messages -= 1.0;
        self.bytes -= bytes as f64;
        true
    }
}

#[derive(Clone)]
pub(crate) struct Outbound {
    sender: mpsc::Sender<Message>,
    admission: Arc<Mutex<()>>,
    failed: Arc<AtomicBool>,
    notification: Arc<Notify>,
}
impl From<mpsc::Sender<Message>> for Outbound {
    fn from(sender: mpsc::Sender<Message>) -> Self {
        Self {
            sender,
            admission: Arc::new(Mutex::new(())),
            failed: Arc::new(AtomicBool::new(false)),
            notification: Arc::new(Notify::new()),
        }
    }
}
impl Outbound {
    pub(crate) fn try_send(
        &self,
        message: Message,
    ) -> Result<(), mpsc::error::TrySendError<Message>> {
        let _admission = self
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let result = self.sender.try_send(message);
        if result.is_err() {
            self.invalidate();
        }
        result
    }
    pub(crate) fn hint(&self, message: Message) {
        self.optional(message);
    }
    pub(crate) fn optional(&self, message: Message) {
        let _admission = self
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Reserve capacity for control and receipts. Check AFTER reserving a
        // permit so simultaneous optional producers cannot consume the reserve.
        // Hints have polling fallback; signaling can retry on peer rediscovery.
        let reserve = (self.sender.max_capacity() / 4).clamp(1, 16);
        if let Ok(permit) = self.sender.try_reserve() {
            if self.sender.capacity() >= reserve {
                permit.send(message);
            }
        }
    }
    pub(crate) fn invalidate(&self) {
        self.failed.store(true, Ordering::Release);
        self.notification.notify_one();
    }
    pub(crate) async fn failed(&self) {
        while !self.failed.load(Ordering::Acquire) {
            self.notification.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn full_critical_queue_wakes_session_even_when_overflow_precedes_wait() {
        let (sender, _receiver) = mpsc::channel(1);
        let outbound = Outbound::from(sender);
        outbound.try_send(Message::Text("receipt".into())).unwrap();
        assert!(outbound.try_send(Message::Text("control".into())).is_err());
        tokio::time::timeout(std::time::Duration::from_millis(100), outbound.failed())
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn coalescible_work_hint_does_not_invalidate_a_full_session() {
        let (sender, mut receiver) = mpsc::channel(1);
        let outbound = Outbound::from(sender);
        outbound.try_send(Message::Text("receipt".into())).unwrap();
        outbound.hint(Message::Text("work_available".into()));
        assert!(!outbound.failed.load(Ordering::Acquire));
        assert_eq!(receiver.recv().await, Some(Message::Text("receipt".into())));
    }

    #[tokio::test]
    async fn optional_flood_leaves_capacity_for_critical_delivery() {
        let (sender, mut receiver) = mpsc::channel(128);
        let outbound = Outbound::from(sender);
        for _ in 0..1000 {
            outbound.hint(Message::Text("hint".into()));
            outbound.optional(Message::Text("signal".into()));
        }
        assert_eq!(outbound.sender.capacity(), 16);
        outbound.try_send(Message::Text("control".into())).unwrap();
        assert!(!outbound.failed.load(Ordering::Acquire));
        let mut control = false;
        while let Ok(message) = receiver.try_recv() {
            control |= message == Message::Text("control".into());
        }
        assert!(control);
    }

    #[test]
    fn concurrent_optional_producers_cannot_temporarily_take_the_critical_reserve() {
        let (sender, mut receiver) = mpsc::channel(128);
        let outbound = Outbound::from(sender);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let outbound = outbound.clone();
                scope.spawn(move || {
                    for _ in 0..1000 {
                        outbound.optional(Message::Text("signal".into()));
                    }
                });
            }
            for _ in 0..100 {
                outbound.try_send(Message::Text("control".into())).unwrap();
                while let Ok(message) = receiver.try_recv() {
                    if message == Message::Text("control".into()) {
                        break;
                    }
                }
            }
        });
        assert!(!outbound.failed.load(Ordering::Acquire));
    }

    #[test]
    fn signal_budget_limits_count_and_bytes_and_refills_without_deny_extension() {
        let mut budget = SignalBudget::default();
        let at = budget.updated;
        for _ in 0..128 {
            assert!(budget.allow(1, at));
        }
        assert!(!budget.allow(1, at));
        assert!(budget.allow(1, at + std::time::Duration::from_millis(32)));
        let mut budget = SignalBudget::default();
        let at = budget.updated;
        for _ in 0..16 {
            assert!(budget.allow(32_768, at));
        }
        assert!(!budget.allow(1, at));
        assert!(budget.allow(32_768, at + std::time::Duration::from_millis(250)));
    }
}
