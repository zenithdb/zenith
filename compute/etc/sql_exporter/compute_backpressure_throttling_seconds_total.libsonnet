{
  metric_name: 'compute_backpressure_throttling_seconds_total',
  type: 'counter',
  help: 'Time compute has spent throttled',
  key_labels: null,
  values: [
    'throttled',
  ],
  query: importstr 'sql_exporter/compute_backpressure_throttling_seconds_total.sql',
}
