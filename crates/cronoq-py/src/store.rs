//! Durable state for Python schedulers: the SQLite store, with payloads and results encoded to
//! bytes by a Python codec (JSON unless the caller passes another one).

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use cronoq::core::{Mutation, Recovered, RecoveredTerminal, TaskId, TaskStatus};
use cronoq::{Store, StoreError, TimeBase};
use cronoq_store_sqlite::{SqliteStore as Inner, SqliteStoreConfig};
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::types::{PyObj, secs};

/// Keep scheduler state in a SQLite file, so queued work survives a restart. Several processes
/// may open the same file: one leads and the others wait in `start()` until it stops.
#[pyclass(module = "cronoq._cronoq", frozen)]
pub struct SqliteStore {
    pub(crate) path: PathBuf,
    pub(crate) cfg: SqliteStoreConfig,
}

#[pymethods]
impl SqliteStore {
    #[new]
    #[pyo3(signature = (path, *, lease_ttl=10.0, lease_renew=2.0, lock_poll=0.5, retention=3600.0, full_sync=true))]
    fn new(
        path: PathBuf,
        lease_ttl: f64,
        lease_renew: f64,
        lock_poll: f64,
        retention: f64,
        full_sync: bool,
    ) -> PyResult<Self> {
        let cfg = SqliteStoreConfig {
            lease_ttl: secs(lease_ttl, "lease_ttl")?,
            lease_renew: secs(lease_renew, "lease_renew")?,
            lock_poll: secs(lock_poll, "lock_poll")?,
            retention: secs(retention, "retention")?,
            full_sync,
            ..Default::default()
        };
        if cfg.lease_renew.is_zero() || cfg.lease_renew >= cfg.lease_ttl {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "lease_renew must be > 0 and shorter than lease_ttl",
            ));
        }
        Ok(SqliteStore { path, cfg })
    }

    fn __repr__(&self) -> String {
        format!("SqliteStore({:?})", self.path)
    }
}

/// A Python codec: `encode(obj) -> bytes` and `decode(bytes) -> obj`.
pub(crate) struct Codec {
    pub(crate) encode: PyObj,
    pub(crate) decode: PyObj,
}

impl Codec {
    pub(crate) fn encode(&self, py: Python<'_>, obj: &PyObj) -> PyResult<Bytes> {
        let out = self.encode.call1(py, (obj.clone_ref(py),))?;
        let b = out.cast_bound::<PyBytes>(py).map_err(|_| {
            pyo3::exceptions::PyTypeError::new_err("codec.encode must return bytes")
        })?;
        Ok(Bytes::copy_from_slice(b.as_bytes()))
    }

    fn decode(&self, py: Python<'_>, b: &[u8]) -> PyResult<PyObj> {
        self.decode.call1(py, (PyBytes::new(py, b),))
    }
}

/// A batch and its outputs, encoded.
type Encoded = (Vec<Mutation<Bytes>>, Vec<(TaskId, Bytes)>);

/// The SQLite store seen through a codec, as a store of Python objects.
pub(crate) struct CodecStore {
    pub(crate) inner: Inner,
    pub(crate) codec: Arc<Codec>,
}

fn warn(py: Python<'_>, msg: &str) {
    let r = py
        .import("logging")
        .and_then(|l| l.call_method1("getLogger", ("cronoq",)))
        .and_then(|l| l.call_method1("warning", (msg,)));
    if let Err(e) = r {
        e.print(py);
    }
}

impl CodecStore {
    pub(crate) async fn open(
        path: PathBuf,
        cfg: SqliteStoreConfig,
        codec: Arc<Codec>,
    ) -> Result<CodecStore, StoreError> {
        Ok(CodecStore {
            inner: Inner::open_with(path, cfg).await?,
            codec,
        })
    }

    fn encode_batch(
        &self,
        batch: &[Mutation<PyObj>],
        outputs: &[(TaskId, &PyObj)],
    ) -> Result<Encoded, StoreError> {
        Python::attach(|py| {
            let enc = |o: &PyObj| {
                self.codec
                    .encode(py, o)
                    .map_err(|e| StoreError::Other(format!("cannot encode payload: {e}")))
            };
            let mut out = Vec::with_capacity(batch.len());
            for m in batch {
                out.push(match m {
                    Mutation::Submitted {
                        id,
                        seq,
                        arrival,
                        spec,
                    } => {
                        let bytes = enc(&spec.payload)?;
                        Mutation::Submitted {
                            id: *id,
                            seq: *seq,
                            arrival: *arrival,
                            spec: spec.clone().map_payload(Arc::new(bytes)),
                        }
                    }
                    Mutation::RecurringUpsert { spec } => {
                        let bytes = enc(&spec.template.payload)?;
                        let s = spec.clone();
                        Mutation::RecurringUpsert {
                            spec: cronoq::core::RecurringSpec {
                                name: s.name,
                                cron: s.cron,
                                tz: s.tz,
                                template: s.template.map_payload(Arc::new(bytes)),
                                overlap: s.overlap,
                                tolerance: s.tolerance,
                                on_miss: s.on_miss,
                            },
                        }
                    }
                    Mutation::Status {
                        id,
                        status,
                        attempt,
                        at,
                        message,
                    } => Mutation::Status {
                        id: *id,
                        status: *status,
                        attempt: *attempt,
                        at: *at,
                        message: message.clone(),
                    },
                    Mutation::RecurringRemove { name } => {
                        Mutation::RecurringRemove { name: name.clone() }
                    }
                    Mutation::RecurringNext { name, next } => Mutation::RecurringNext {
                        name: name.clone(),
                        next: *next,
                    },
                });
            }
            // A result the codec cannot encode is not stored; the task itself still succeeded.
            let mut outs = Vec::with_capacity(outputs.len());
            for (id, o) in outputs {
                match self.codec.encode(py, o) {
                    Ok(b) => outs.push((*id, b)),
                    Err(e) => warn(
                        py,
                        &format!("result of task {id} not stored: cannot encode it: {e}"),
                    ),
                }
            }
            Ok((out, outs))
        })
    }
}

#[async_trait]
impl Store<PyObj, PyObj> for CodecStore {
    async fn acquire_leadership(&self) -> Result<u16, StoreError> {
        self.inner.acquire_leadership().await
    }

    async fn release_leadership(&self) {
        Store::<Bytes, Bytes>::release_leadership(&self.inner).await
    }

    async fn commit(
        &self,
        epoch: u16,
        base: TimeBase,
        batch: &[Mutation<PyObj>],
        outputs: &[(TaskId, &PyObj)],
    ) -> Result<(), StoreError> {
        let (batch, outs) = self.encode_batch(batch, outputs)?;
        let refs: Vec<(TaskId, &Bytes)> = outs.iter().map(|(id, b)| (*id, b)).collect();
        self.inner.commit(epoch, base, &batch, &refs).await
    }

    async fn recover(&self, base: TimeBase) -> Result<Recovered<PyObj>, StoreError> {
        let rec = self.inner.recover(base).await?;
        Ok(Python::attach(|py| {
            let mut out = Recovered {
                tasks: Vec::with_capacity(rec.tasks.len()),
                terminal: rec.terminal,
                recurring: Vec::with_capacity(rec.recurring.len()),
                next_id: rec.next_id,
            };
            for t in rec.tasks {
                match self.codec.decode(py, &t.spec.payload) {
                    Ok(p) => out.tasks.push(cronoq::core::RecoveredTask {
                        id: t.id,
                        seq: t.seq,
                        arrival: t.arrival,
                        attempt: t.attempt,
                        spec: t.spec.map_payload(Arc::new(p)),
                    }),
                    // Treated as failed so recovery can go on; the stored row is left for repair.
                    Err(e) => {
                        warn(
                            py,
                            &format!(
                                "task {} not recovered: cannot decode its payload: {e}",
                                t.id
                            ),
                        );
                        out.terminal.push(RecoveredTerminal {
                            id: t.id,
                            key: t.spec.key.clone(),
                            status: TaskStatus::Failed,
                            at: t.arrival,
                        });
                    }
                }
            }
            for (spec, next) in rec.recurring {
                match self.codec.decode(py, &spec.template.payload) {
                    Ok(p) => out.recurring.push((
                        cronoq::core::RecurringSpec {
                            name: spec.name,
                            cron: spec.cron,
                            tz: spec.tz,
                            template: spec.template.map_payload(Arc::new(p)),
                            overlap: spec.overlap,
                            tolerance: spec.tolerance,
                            on_miss: spec.on_miss,
                        },
                        next,
                    )),
                    Err(e) => warn(
                        py,
                        &format!(
                            "schedule '{}' not recovered: cannot decode its payload: {e}",
                            spec.name
                        ),
                    ),
                }
            }
            out
        }))
    }

    async fn load_output(&self, id: TaskId) -> Result<Option<PyObj>, StoreError> {
        let Some(b) = self.inner.load_output(id).await? else {
            return Ok(None);
        };
        Python::attach(|py| match self.codec.decode(py, &b) {
            Ok(o) => Ok(Some(o)),
            Err(e) => {
                warn(
                    py,
                    &format!("result of task {id} not loaded: cannot decode it: {e}"),
                );
                Ok(None)
            }
        })
    }
}
