use prometheus::{core::Desc, proto};

use crate::register_internal;

#[derive(Default)]
pub struct TokioCollector {
    handles: Vec<(tokio::runtime::Handle, String)>,
}

impl TokioCollector {
    pub fn add_runtime(mut self, handle: tokio::runtime::Handle, name: String) -> Self {
        self.handles.push((handle, name));
        self
    }

    pub fn register(self) -> Result<(), prometheus::Error> {
        register_internal(Box::new(self))
    }

    #[cfg(tokio_unstable)]
    fn gauge(
        &self,
        desc: &Desc,
        f: impl Fn(&tokio::runtime::RuntimeMetrics) -> f64,
    ) -> proto::MetricFamily {
        self.metric(desc, proto::MetricType::GAUGE, |rt| {
            let mut g = proto::Gauge::default();
            g.set_value(f(rt));
            let mut m = proto::Metric::default();
            m.set_gauge(g);
            m
        })
    }

    #[cfg(tokio_unstable)]
    fn counter_per_worker(
        &self,
        desc: &Desc,
        f: impl Fn(&tokio::runtime::RuntimeMetrics, usize) -> f64,
    ) -> proto::MetricFamily {
        self.metric_per_worker(desc, proto::MetricType::COUNTER, |m, i| {
            let mut g = proto::Counter::default();
            g.set_value(f(m, i));
            let mut m = proto::Metric::default();
            m.set_counter(g);
            m
        })
    }

    #[cfg(tokio_unstable)]
    fn metric(
        &self,
        desc: &Desc,
        typ: proto::MetricType,
        f: impl Fn(&tokio::runtime::RuntimeMetrics) -> proto::Metric,
    ) -> proto::MetricFamily {
        self.metrics(desc, typ, |rt, labels, metrics| {
            let mut m = f(rt);
            m.set_label(labels.clone());
            metrics.push(m);
        })
    }

    #[cfg(tokio_unstable)]
    fn metric_per_worker(
        &self,
        desc: &Desc,
        typ: proto::MetricType,
        f: impl Fn(&tokio::runtime::RuntimeMetrics, usize) -> proto::Metric,
    ) -> proto::MetricFamily {
        self.metrics(desc, typ, |rt, labels, metrics| {
            let workers = rt.num_workers();
            for i in 0..workers {
                let mut m = f(rt, i);

                let mut label_worker = proto::LabelPair::default();
                label_worker.set_name("worker".to_owned());
                label_worker.set_value(i.to_string());
                let mut labels = labels.clone();
                labels.push(label_worker);

                m.set_label(labels);
                metrics.push(m);
            }
        })
    }

    #[cfg(tokio_unstable)]
    fn metrics(
        &self,
        desc: &Desc,
        typ: proto::MetricType,
        f: impl Fn(&tokio::runtime::RuntimeMetrics, Vec<proto::LabelPair>, &mut Vec<proto::Metric>),
    ) -> proto::MetricFamily {
        let mut m = proto::MetricFamily::default();
        m.set_name(desc.fq_name.clone());
        m.set_help(desc.help.clone());
        m.set_field_type(typ);
        let mut metrics = vec![];
        for (rt, name) in &self.handles {
            let rt_metrics = rt.metrics();

            let mut label_name = proto::LabelPair::default();
            label_name.set_name("runtime".to_owned());
            label_name.set_value(name.to_owned());

            let labels = vec![label_name];

            f(&rt_metrics, labels, &mut metrics);
        }

        m.set_metric(metrics);
        m
    }
}

#[cfg(tokio_unstable)]
static ACTIVE_TASK_COUNT: once_cell::sync::Lazy<Desc> = once_cell::sync::Lazy::new(|| {
    Desc::new(
        "tokio_active_task_count_total".to_owned(),
        "the number of active tasks in the runtime".to_owned(),
        vec!["runtime".to_owned()],
        Default::default(),
    )
    .expect("should be a valid description")
});
#[cfg(tokio_unstable)]
static WORKER_STEAL_COUNT: once_cell::sync::Lazy<Desc> = once_cell::sync::Lazy::new(|| {
    Desc::new(
        "tokio_worker_steal_count_total".to_owned(),
        "the number of tasks the given worker thread stole from another worker thread".to_owned(),
        vec!["runtime".to_owned(), "worker".to_owned()],
        Default::default(),
    )
    .expect("should be a valid description")
});

#[allow(unused_mut, clippy::let_and_return)]
impl prometheus::core::Collector for TokioCollector {
    fn desc(&self) -> Vec<&Desc> {
        let mut metrics = Vec::new();
        #[cfg(tokio_unstable)]
        metrics.extend([&*ACTIVE_TASK_COUNT, &*WORKER_STEAL_COUNT]);
        metrics
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let mut metrics = Vec::new();
        #[cfg(tokio_unstable)]
        metrics.extend([
            self.gauge(&ACTIVE_TASK_COUNT, |m| m.active_tasks_count() as f64),
            self.counter_per_worker(&WORKER_STEAL_COUNT, |m, i| m.worker_steal_count(i) as f64),
        ]);
        metrics
    }
}

#[cfg(test)]
#[cfg(tokio_unstable)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use prometheus::{Registry, TextEncoder};
    use tokio::{runtime, sync::Mutex, task::JoinSet};

    use crate::tokio_metrics::TokioCollector;

    #[test]
    fn gather_multiple_runtimes() {
        let registry = Registry::new();

        let runtime1 = runtime::Builder::new_current_thread().build().unwrap();
        let runtime2 = runtime::Builder::new_current_thread().build().unwrap();

        let collector = TokioCollector::default()
            .add_runtime(runtime1.handle().clone(), "runtime1".to_owned())
            .add_runtime(runtime2.handle().clone(), "runtime2".to_owned());

        registry.register(Box::new(collector)).unwrap();

        std::thread::scope(|s| {
            let lock1 = Arc::new(Mutex::new(0));
            let lock2 = Arc::new(Mutex::new(0));

            let guard1 = lock1.clone().try_lock_owned().unwrap();
            let guard2 = lock2.clone().try_lock_owned().unwrap();

            s.spawn(move || {
                runtime1.block_on(async {
                    let mut joinset = JoinSet::new();
                    for _ in 0..5 {
                        let lock = lock1.clone();
                        joinset.spawn(async move {
                            let mut _guard = lock.lock().await;
                        });
                    }
                    while let Some(_x) = joinset.join_next().await {}
                })
            });

            s.spawn(move || {
                runtime2.block_on(async {
                    let mut joinset = JoinSet::new();
                    for _ in 0..5 {
                        let lock = lock2.clone();
                        joinset.spawn(async move {
                            let mut _guard = lock.lock().await;
                        });
                    }
                    while let Some(_x) = joinset.join_next().await {}
                })
            });

            std::thread::sleep(Duration::from_millis(10));

            let text = TextEncoder.encode_to_string(&registry.gather()).unwrap();
            assert_eq!(
                text,
                r#"# HELP tokio_active_task_count_total the number of active tasks in the runtime
# TYPE tokio_active_task_count_total gauge
tokio_active_task_count_total{runtime="runtime1"} 5
tokio_active_task_count_total{runtime="runtime2"} 5
# HELP tokio_worker_steal_count_total the number of tasks the given worker thread stole from another worker thread
# TYPE tokio_worker_steal_count_total counter
tokio_worker_steal_count_total{runtime="runtime1",worker="0"} 0
tokio_worker_steal_count_total{runtime="runtime2",worker="0"} 0
"#
            );

            drop(guard1);
            std::thread::sleep(Duration::from_millis(10));

            let text = TextEncoder.encode_to_string(&registry.gather()).unwrap();
            assert_eq!(
                text,
                r#"# HELP tokio_active_task_count_total the number of active tasks in the runtime
# TYPE tokio_active_task_count_total gauge
tokio_active_task_count_total{runtime="runtime1"} 0
tokio_active_task_count_total{runtime="runtime2"} 5
# HELP tokio_worker_steal_count_total the number of tasks the given worker thread stole from another worker thread
# TYPE tokio_worker_steal_count_total counter
tokio_worker_steal_count_total{runtime="runtime1",worker="0"} 0
tokio_worker_steal_count_total{runtime="runtime2",worker="0"} 0
"#
            );

            drop(guard2);
            std::thread::sleep(Duration::from_millis(10));

            let text = TextEncoder.encode_to_string(&registry.gather()).unwrap();
            assert_eq!(
                text,
                r#"# HELP tokio_active_task_count_total the number of active tasks in the runtime
# TYPE tokio_active_task_count_total gauge
tokio_active_task_count_total{runtime="runtime1"} 0
tokio_active_task_count_total{runtime="runtime2"} 0
# HELP tokio_worker_steal_count_total the number of tasks the given worker thread stole from another worker thread
# TYPE tokio_worker_steal_count_total counter
tokio_worker_steal_count_total{runtime="runtime1",worker="0"} 0
tokio_worker_steal_count_total{runtime="runtime2",worker="0"} 0
"#
            );
        });
    }
}
