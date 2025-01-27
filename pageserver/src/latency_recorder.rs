use std::sync::Mutex;
use std::{sync::Arc, time::Instant};

enum GetPageSpan {
    GetPage(Span),
    Throttle(Span),
    WaitLsn(Span),
    Batch(Span),
    VectoredRead(Span),
    PlanIO(Span),
    DownloadLayer(Span),
    ExecuteAndWalredoAll(Span),
    ExecuteAndWalredo(Span),
    Flush(Span),
}

#[derive(Copy, Clone, Debug)]
enum SpanName {
    GetPage,
    Throttle,
    WaitLsn,
    Batch,
    VectoredRead,
    PlanIO,
    DownloadLayer,
    ExecuteAndWalredoAll,
    ExecuteAndWalredo,
    Flush,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct SpanId(u32);

impl SpanId {
    fn root() -> Self {
        SpanId(0)
    }

    fn next(&self) -> Self {
        SpanId(self.0.checked_add(1).expect("too many spans per request"))
    }
}

struct OpenSpan {
    id: SpanId,
    parent_id: Option<SpanId>,
    start: Instant,
    name: SpanName,
    closed: bool,
}

struct Span {
    id: SpanId,
    parent_id: Option<SpanId>,
    start: Instant,
    end: Instant,
    name: SpanName,
}

impl OpenSpan {
    fn new(id: SpanId, parent_id: Option<SpanId>, name: SpanName) -> Self {
        let now = Instant::now();
        Self {
            id,
            parent_id,
            start: now,
            name,
            closed: false,
        }
    }

    fn close(mut self) -> Span {
        self.closed = true;
        Span {
            id: self.id,
            parent_id: self.parent_id,
            start: self.start,
            end: Instant::now(),
            name: self.name,
        }
    }
}

impl Drop for OpenSpan {
    fn drop(&mut self) {
        if !self.closed {
            tracing::warn!("Span {:?} dropped without being closed", self.name)
        }
    }
}

#[derive(Default)]
struct GetPageLatencyRecorderInner {
    spans: Vec<Span>,
    open_span_ids: Vec<SpanId>,

    downstream: Option<Arc<Mutex<GetPageLatencyRecorderInner>>>,
}

#[derive(Clone)]
pub(crate) struct GetPageLatencyRecorder {
    inner: Arc<Mutex<GetPageLatencyRecorderInner>>,
}

impl GetPageLatencyRecorder {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub(crate) fn enter_span(&self, name: SpanName) -> OpenSpan {
        let mut inner = self.inner.lock().unwrap();

        let (parent_id, id) = match inner.open_span_ids.last() {
            Some(latest_id) => (Some(*latest_id), latest_id.next()),
            None => (None, SpanId::root()),
        };

        inner.open_span_ids.push(id);
        OpenSpan::new(id, parent_id, name)
    }

    pub(crate) fn close_span(&self, span: OpenSpan) {
        let mut inner = self.inner.lock().unwrap();
        let popped = inner.open_span_ids.pop();

        // Spans must be closed in reverse open order
        assert_eq!(popped, Some(span.id));

        inner.spans.push(span.close())
    }
}
