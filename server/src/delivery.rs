//! Bounded node delivery. Critical queue failure invalidates the whole session;
//! clients reconnect and rebuild presence, signaling and holder registrations.
use axum::extract::ws::Message;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{mpsc, Notify};

#[derive(Clone)]
pub(crate) struct Outbound {
    sender: mpsc::Sender<Message>,
    failed: Arc<AtomicBool>,
    notification: Arc<Notify>,
}
impl From<mpsc::Sender<Message>> for Outbound {
    fn from(sender: mpsc::Sender<Message>) -> Self {
        Self {
            sender,
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
        let result = self.sender.try_send(message);
        if result.is_err() {
            self.invalidate();
        }
        result
    }
    pub(crate) fn hint(&self, message: Message) {
        // Hints carry no authoritative state. Polling supplies missed hints,
        // and a hint must never displace a receipt or control response.
        let _ = self.sender.try_send(message);
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
}
