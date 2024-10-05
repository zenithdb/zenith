use std::marker::PhantomData;

use measured::{
    label::NoLabels,
    metric::{
        gauge::GaugeState, group::Encoding, name::MetricNameEncoder, MetricEncoding,
        MetricFamilyEncoding, MetricType,
    },
    text::TextEncoder,
    LabelGroup, MetricGroup,
};
use tikv_jemalloc_ctl::{config, epoch, epoch_mib, stats, version, Access, AsName, Name};
use tracing::info;

pub struct MetricRecorder {
    epoch: epoch_mib,
    inner: Metrics,
}

#[derive(MetricGroup)]
struct Metrics {
    active_bytes: JemallocGaugeFamily<stats::active_mib>,
    allocated_bytes: JemallocGaugeFamily<stats::allocated_mib>,
    mapped_bytes: JemallocGaugeFamily<stats::mapped_mib>,
    metadata_bytes: JemallocGaugeFamily<stats::metadata_mib>,
    resident_bytes: JemallocGaugeFamily<stats::resident_mib>,
    retained_bytes: JemallocGaugeFamily<stats::retained_mib>,
}

impl<Enc: Encoding> MetricGroup<Enc> for MetricRecorder
where
    Metrics: MetricGroup<Enc>,
{
    fn collect_group_into(&self, enc: &mut Enc) -> Result<(), Enc::Err> {
        if self.epoch.advance().is_ok() {
            self.inner.collect_group_into(enc)?;
        }
        Ok(())
    }
}

impl MetricRecorder {
    pub fn new() -> Result<Self, anyhow::Error> {
        tracing::info!(
            config = config::malloc_conf::read()?,
            version = version::read()?,
            "starting jemalloc recorder"
        );

        Ok(Self {
            epoch: epoch::mib()?,
            inner: Metrics {
                active_bytes: JemallocGaugeFamily(stats::active::mib()?),
                allocated_bytes: JemallocGaugeFamily(stats::allocated::mib()?),
                mapped_bytes: JemallocGaugeFamily(stats::mapped::mib()?),
                metadata_bytes: JemallocGaugeFamily(stats::metadata::mib()?),
                resident_bytes: JemallocGaugeFamily(stats::resident::mib()?),
                retained_bytes: JemallocGaugeFamily(stats::retained::mib()?),
            },
        })
    }
}

struct JemallocGauge<T>(PhantomData<T>);

impl<T> Default for JemallocGauge<T> {
    fn default() -> Self {
        JemallocGauge(PhantomData)
    }
}
impl<T> MetricType for JemallocGauge<T> {
    type Metadata = T;
}

struct JemallocGaugeFamily<T>(T);
impl<M, T: Encoding> MetricFamilyEncoding<T> for JemallocGaugeFamily<M>
where
    JemallocGauge<M>: MetricEncoding<T, Metadata = M>,
{
    fn collect_family_into(&self, name: impl MetricNameEncoder, enc: &mut T) -> Result<(), T::Err> {
        JemallocGauge::write_type(&name, enc)?;
        JemallocGauge(PhantomData).collect_into(&self.0, NoLabels, name, enc)
    }
}

macro_rules! jemalloc_gauge {
    ($stat:ident, $mib:ident) => {
        impl<W: std::io::Write> MetricEncoding<TextEncoder<W>> for JemallocGauge<stats::$mib> {
            fn write_type(
                name: impl MetricNameEncoder,
                enc: &mut TextEncoder<W>,
            ) -> Result<(), std::io::Error> {
                GaugeState::write_type(name, enc)
            }

            fn collect_into(
                &self,
                mib: &stats::$mib,
                labels: impl LabelGroup,
                name: impl MetricNameEncoder,
                enc: &mut TextEncoder<W>,
            ) -> Result<(), std::io::Error> {
                if let Ok(v) = mib.read() {
                    GaugeState::new(v as i64).collect_into(&(), labels, name, enc)?;
                }
                Ok(())
            }
        }
    };
}

jemalloc_gauge!(active, active_mib);
jemalloc_gauge!(allocated, allocated_mib);
jemalloc_gauge!(mapped, mapped_mib);
jemalloc_gauge!(metadata, metadata_mib);
jemalloc_gauge!(resident, resident_mib);
jemalloc_gauge!(retained, retained_mib);

pub fn inspect_thp() -> Result<(), tikv_jemalloc_ctl::Error> {
    let opt_thp: &Name = c"opt.thp".to_bytes_with_nul().name();
    let s: &str = opt_thp.read()?;
    info!("jemalloc opt.thp {s}");
    Ok(())
}
