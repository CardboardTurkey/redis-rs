//! Request forwarding for the cluster connection driver.
//!
//! # Why this is not a separate crate
//!
//! [`forward_requests`] works with any `Stream` and `Sink`, but only the cluster driver
//! uses it, and its batch sizes are tuned to that driver. A second user with other needs
//! would justify a crate, with the limits as parameters.

use std::{
    future::poll_fn,
    pin::pin,
    task::{Poll, ready},
};

use futures_util::{sink::Sink, stream::Stream};

/// Request limit for one batch of [`forward_requests`].
///
/// The value comes from load tests. With one batch of 128 per poll, the connection fell
/// behind a sustained load and stalled. Batches of 32 kept up.
///
/// The likely reason: every flush but the last in a poll starts while budget remains, so
/// most requests make progress in the poll that takes them. Requests from the batch where
/// the budget runs out wait for the next poll's first flush, and a small batch keeps that
/// remainder small.
const REQUESTS_PER_FLUSH: usize = 32;

/// Request limit for one poll of [`forward_requests`].
///
/// This equals tokio's current [cooperative budget](https://github.com/tokio-rs/tokio/blob/tokio-1.52.3/tokio/src/task/coop/mod.rs#L105-L117),
/// which usually ends the poll first. The limit matters on runtimes without a budget.
const REQUESTS_PER_POLL: usize = 128;

/// Forwards `requests` into `sink`.
///
/// Each poll starts with a flush, then alternates between taking a batch of requests and
/// flushing it. Unlike
/// [`StreamExt::forward`](https://github.com/rust-lang/futures-rs/blob/0.3.31/futures-util/src/stream/stream/forward.rs#L47-L74),
/// which flushes only when the stream returns `Pending`, this flushes each batch before
/// taking the next.
///
/// The order matters under tokio's multi-thread runtime, where callers on other threads
/// refill the request channel while this task drains it. The channel then returns
/// `Pending` only once the task's cooperative budget is spent, so a flush after it runs
/// with no budget left. Each request then stops at its first tokio sync primitive (for a
/// new request, the connection lock) and makes no progress in that poll. While callers
/// keep the channel full, every poll ends that way and the connection stalls.
pub(super) async fn forward_requests<St, Si>(requests: St, sink: Si) -> Result<(), Si::Error>
where
    St: Stream,
    Si: Sink<St::Item>,
{
    let mut requests = pin!(requests);
    let mut sink = pin!(sink);
    let mut closing = false;
    poll_fn(move |cx| {
        if closing {
            return sink.as_mut().poll_close(cx);
        }
        let mut taken = 0;
        loop {
            // A pending flush has registered its own wake-up, so moving on loses none.
            if let Poll::Ready(Err(err)) = sink.as_mut().poll_flush(cx) {
                return Poll::Ready(Err(err));
            }
            if taken >= REQUESTS_PER_POLL {
                // More requests may be waiting, and the stream registered no wake-up for them.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let mut batch = 0;
            while batch < REQUESTS_PER_FLUSH {
                ready!(sink.as_mut().poll_ready(cx))?;
                match requests.as_mut().poll_next(cx) {
                    Poll::Ready(Some(request)) => {
                        sink.as_mut().start_send(request)?;
                        batch += 1;
                    }
                    Poll::Ready(None) => {
                        closing = true;
                        return sink.as_mut().poll_close(cx);
                    }
                    Poll::Pending => break,
                }
            }
            if batch == 0 {
                return Poll::Pending;
            }
            taken += batch;
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task,
        time::Duration,
    };

    use futures_util::stream;

    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum SinkEvent {
        Send,
        Flush,
        Close,
    }

    /// Result of [`Recorder`]'s flush.
    #[derive(Clone, Copy, Default)]
    enum FlushOutcome {
        #[default]
        Ready,
        Fail,
        /// Pending with no wake-up, so a caller that waits on it hangs.
        Pending,
    }

    /// Sink that records each call [`forward_requests`] makes.
    #[derive(Clone, Default)]
    struct Recorder {
        events: Arc<Mutex<Vec<SinkEvent>>>,
        flush: FlushOutcome,
        pending_readies: Arc<AtomicUsize>,
        pending_closes: Arc<AtomicUsize>,
    }

    impl Recorder {
        fn events(&self) -> Vec<SinkEvent> {
            self.events.lock().unwrap().clone()
        }

        fn count(&self, event: SinkEvent) -> usize {
            self.events().iter().filter(|e| **e == event).count()
        }

        fn record(&self, event: SinkEvent) {
            self.events.lock().unwrap().push(event);
        }
    }

    /// Takes one from `counter` and wakes the task, or returns false once it is zero.
    fn take_pending(counter: &AtomicUsize, cx: &mut task::Context) -> bool {
        let taken = counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if taken {
            cx.waker().wake_by_ref();
        }
        taken
    }

    impl Sink<u32> for Recorder {
        type Error = ();

        fn poll_ready(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
            if take_pending(&self.pending_readies, cx) {
                return Poll::Pending;
            }
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, _item: u32) -> Result<(), ()> {
            self.record(SinkEvent::Send);
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut task::Context) -> Poll<Result<(), ()>> {
            self.record(SinkEvent::Flush);
            match self.flush {
                FlushOutcome::Ready => Poll::Ready(Ok(())),
                FlushOutcome::Fail => Poll::Ready(Err(())),
                FlushOutcome::Pending => Poll::Pending,
            }
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
            self.record(SinkEvent::Close);
            if take_pending(&self.pending_closes, cx) {
                return Poll::Pending;
            }
            Poll::Ready(Ok(()))
        }
    }

    struct WakeCount(AtomicUsize);

    impl task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Sink whose flush needs tokio's cooperative budget for each item, like a new
    /// request taking the connection lock.
    struct BudgetedSink {
        queued: usize,
        sent: Arc<AtomicUsize>,
    }

    impl Sink<u32> for BudgetedSink {
        type Error = ();

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut task::Context) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(mut self: Pin<&mut Self>, _item: u32) -> Result<(), ()> {
            self.queued += 1;
            Ok(())
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
            while self.queued > 0 {
                ready!(tokio::task::coop::poll_proceed(cx)).made_progress();
                self.queued -= 1;
                self.sent.fetch_add(1, Ordering::SeqCst);
            }
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut task::Context) -> Poll<Result<(), ()>> {
            self.poll_flush(cx)
        }
    }

    #[test]
    fn flushes_between_batches() {
        let sink = Recorder::default();
        futures::executor::block_on(forward_requests(stream::iter(0..300), sink.clone())).unwrap();

        let events = sink.events();
        assert_eq!(events.first(), Some(&SinkEvent::Flush));
        assert_eq!(events.last(), Some(&SinkEvent::Close));
        assert_eq!(sink.count(SinkEvent::Send), 300);
        let longest_run = events
            .split(|e| *e != SinkEvent::Send)
            .map(<[_]>::len)
            .max()
            .unwrap();
        assert_eq!(longest_run, REQUESTS_PER_FLUSH);
    }

    #[test]
    fn yields_when_requests_never_stop() {
        let sink = Recorder::default();
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = task::Waker::from(wakes.clone());
        let mut cx = task::Context::from_waker(&waker);
        let mut forward = pin!(forward_requests(stream::repeat(0), sink.clone()));

        assert!(forward.as_mut().poll(&mut cx).is_pending());
        assert_eq!(sink.count(SinkEvent::Send), REQUESTS_PER_POLL);
        assert_eq!(sink.events().last(), Some(&SinkEvent::Flush));
        assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stops_when_the_flush_fails() {
        let sink = Recorder {
            flush: FlushOutcome::Fail,
            ..Recorder::default()
        };
        let result =
            futures::executor::block_on(forward_requests(stream::iter(0..10), sink.clone()));

        assert_eq!(result, Err(()));
        assert_eq!(sink.events(), vec![SinkEvent::Flush]);
    }

    #[test]
    fn takes_requests_while_the_flush_is_pending() {
        let sink = Recorder {
            flush: FlushOutcome::Pending,
            ..Recorder::default()
        };
        let mut cx = task::Context::from_waker(task::Waker::noop());
        let mut forward = pin!(forward_requests(stream::iter(0..10), sink.clone()));

        assert_eq!(forward.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        assert_eq!(sink.count(SinkEvent::Send), 10);
        assert_eq!(sink.events().last(), Some(&SinkEvent::Close));
    }

    #[test]
    fn resumes_after_the_sink_is_not_ready() {
        let sink = Recorder {
            pending_readies: Arc::new(AtomicUsize::new(3)),
            ..Recorder::default()
        };
        futures::executor::block_on(forward_requests(stream::iter(0..10), sink.clone())).unwrap();

        assert_eq!(sink.count(SinkEvent::Send), 10);
    }

    #[test]
    fn only_polls_close_after_the_stream_ends() {
        let sink = Recorder {
            pending_closes: Arc::new(AtomicUsize::new(2)),
            ..Recorder::default()
        };
        futures::executor::block_on(forward_requests(stream::iter(0..3), sink.clone())).unwrap();

        let events = sink.events();
        let first_close = events.iter().position(|e| *e == SinkEvent::Close).unwrap();
        assert!(events[first_close..].iter().all(|e| *e == SinkEvent::Close));
        assert_eq!(sink.count(SinkEvent::Close), 3);
    }

    #[tokio::test]
    async fn sends_while_the_channel_stays_full() {
        // Each item spends budget, like a tokio channel, and is always ready, like a
        // channel that callers keep full.
        let requests = stream::poll_fn(|cx| {
            ready!(tokio::task::coop::poll_proceed(cx)).made_progress();
            Poll::Ready(Some(0))
        });
        let sent = Arc::new(AtomicUsize::new(0));
        let sink = BudgetedSink {
            queued: 0,
            sent: sent.clone(),
        };
        tokio::spawn(forward_requests(requests, sink));

        tokio::time::timeout(Duration::from_secs(5), async {
            while sent.load(Ordering::SeqCst) < 1_000 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
