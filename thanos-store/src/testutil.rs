//! A fake Store API and Info server for tests, behind the `testutil`
//! feature. It plays back scripted frames, so tests can stage split
//! series, batches, warnings, mid-stream failures and histogram chunks
//! without a real Thanos.

use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::chunkenc::encode_xor;
use crate::labels::{compile_matchers, from_proto_matchers, LabelSet};
use crate::storepb::thanos::chunk::Encoding;
use crate::storepb::thanos::info::info_server::{Info, InfoServer};
use crate::storepb::thanos::info::{InfoRequest, InfoResponse, StoreInfo};
use crate::storepb::thanos::store_server::{Store, StoreServer};
use crate::storepb::thanos::{
    series_response, AggrChunk, Chunk, Label, LabelNamesRequest, LabelNamesResponse,
    LabelValuesRequest, LabelValuesResponse, Series, SeriesBatch, SeriesRequest, SeriesResponse,
    ZLabelSet,
};

/// One scripted `Series` frame, or the status that ends the stream.
pub type Frame = Result<SeriesResponse, Status>;

/// A store that answers from fixed data and records what it was asked.
#[derive(Debug)]
pub struct FakeStore {
    /// What `Info` returns.
    pub info: InfoResponse,
    /// What `Series` streams, in order; an `Err` ends the stream with that
    /// status after the frames before it.
    pub frames: Vec<Frame>,
    /// Fail `Series` before streaming anything.
    pub series_status: Option<Status>,
    /// Return only the scripted series whose labels satisfy the request's
    /// matchers, as a real store would. Off, everything is streamed, for
    /// tests of what the client does with a misbehaving store.
    pub honors_matchers: bool,
    pub label_names: Result<LabelNamesResponse, Status>,
    pub label_values: Result<LabelValuesResponse, Status>,
    /// Every `Series` request received, for asserting on what the client
    /// sent.
    pub series_requests: Mutex<Vec<SeriesRequest>>,
    pub label_names_requests: Mutex<Vec<LabelNamesRequest>>,
    pub label_values_requests: Mutex<Vec<LabelValuesRequest>>,
}

impl FakeStore {
    /// A store announcing `info`, with nothing to stream yet.
    pub fn new(info: InfoResponse) -> Self {
        Self {
            info,
            frames: Vec::new(),
            series_status: None,
            honors_matchers: true,
            label_names: Ok(LabelNamesResponse::default()),
            label_values: Ok(LabelValuesResponse::default()),
            series_requests: Mutex::new(Vec::new()),
            label_names_requests: Mutex::new(Vec::new()),
            label_values_requests: Mutex::new(Vec::new()),
        }
    }

    pub fn with_frames(mut self, frames: Vec<Frame>) -> Self {
        self.frames = frames;
        self
    }

    pub fn with_series_status(mut self, status: Status) -> Self {
        self.series_status = Some(status);
        self
    }

    /// Stream every scripted frame whatever the matchers say.
    pub fn ignoring_matchers(mut self) -> Self {
        self.honors_matchers = false;
        self
    }

    pub fn with_label_names(mut self, response: Result<LabelNamesResponse, Status>) -> Self {
        self.label_names = response;
        self
    }

    pub fn with_label_values(mut self, response: Result<LabelValuesResponse, Status>) -> Self {
        self.label_values = response;
        self
    }

    /// The `Series` requests received so far.
    pub fn series_requests(&self) -> Vec<SeriesRequest> {
        self.series_requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn label_names_requests(&self) -> Vec<LabelNamesRequest> {
        self.label_names_requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn label_values_requests(&self) -> Vec<LabelValuesRequest> {
        self.label_values_requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[tonic::async_trait]
impl Store for FakeStore {
    type SeriesStream = Pin<Box<dyn Stream<Item = Frame> + Send>>;

    async fn series(
        &self,
        request: Request<SeriesRequest>,
    ) -> Result<Response<Self::SeriesStream>, Status> {
        let request = request.into_inner();
        self.series_requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.clone());
        if let Some(status) = &self.series_status {
            return Err(status.clone());
        }
        let mut frames = self.frames.clone();
        if self.honors_matchers {
            let matchers = from_proto_matchers(&request.matchers)
                .and_then(|m| compile_matchers(&m))
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            let keep = |series: &Series| {
                let labels = LabelSet::from_proto(&series.labels);
                matchers
                    .iter()
                    .all(|m| m.matches(labels.get(&m.name).unwrap_or("")))
            };
            frames.retain_mut(|frame| match frame {
                Ok(SeriesResponse {
                    result: Some(series_response::Result::Series(series)),
                }) => keep(series),
                Ok(SeriesResponse {
                    result: Some(series_response::Result::Batch(batch)),
                }) => {
                    batch.series.retain(&keep);
                    !batch.series.is_empty()
                }
                _ => true,
            });
        }
        Ok(Response::new(Box::pin(tokio_stream::iter(frames))))
    }

    async fn label_names(
        &self,
        request: Request<LabelNamesRequest>,
    ) -> Result<Response<LabelNamesResponse>, Status> {
        self.label_names_requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.into_inner());
        self.label_names.clone().map(Response::new)
    }

    async fn label_values(
        &self,
        request: Request<LabelValuesRequest>,
    ) -> Result<Response<LabelValuesResponse>, Status> {
        self.label_values_requests
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.into_inner());
        self.label_values.clone().map(Response::new)
    }
}

#[tonic::async_trait]
impl Info for FakeStore {
    async fn info(&self, _request: Request<InfoRequest>) -> Result<Response<InfoResponse>, Status> {
        Ok(Response::new(self.info.clone()))
    }
}

/// Serve `store` on a free loopback port. Returns the `host:port` to dial
/// and the server task, which runs until aborted or the runtime ends.
pub async fn serve(store: Arc<FakeStore>) -> (String, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a loopback port");
    let addr = listener.local_addr().expect("local address").to_string();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(StoreServer::from_arc(Arc::clone(&store)))
            .add_service(InfoServer::from_arc(store))
            .serve_with_incoming(incoming)
            .await
            .expect("serve the fake store");
    });
    (addr, handle)
}

/// An `Info` reply for a store of `component` type announcing
/// `label_sets` and holding `[min_time, max_time]`.
pub fn info(
    component: &str,
    label_sets: &[&[(&str, &str)]],
    min_time: i64,
    max_time: i64,
) -> InfoResponse {
    InfoResponse {
        label_sets: label_sets
            .iter()
            .map(|set| ZLabelSet {
                labels: labels(set),
            })
            .collect(),
        component_type: component.to_string(),
        store: Some(StoreInfo {
            min_time,
            max_time,
            ..Default::default()
        }),
        ..Default::default()
    }
}

pub fn labels(pairs: &[(&str, &str)]) -> Vec<Label> {
    pairs
        .iter()
        .map(|(name, value)| Label {
            name: name.to_string(),
            value: value.to_string(),
        })
        .collect()
}

/// A raw XOR chunk of `samples`, its bounds those of the samples.
pub fn raw_chunk(samples: &[(i64, f64)]) -> AggrChunk {
    AggrChunk {
        min_time: samples.first().map_or(0, |s| s.0),
        max_time: samples.last().map_or(0, |s| s.0),
        raw: Some(Chunk {
            r#type: Encoding::Xor as i32,
            data: encode_xor(samples),
            hash: 0,
        }),
        ..Default::default()
    }
}

/// A chunk of an encoding this client skips, with bytes that would not
/// decode as XOR.
pub fn histogram_chunk(min_time: i64, max_time: i64) -> AggrChunk {
    AggrChunk {
        min_time,
        max_time,
        raw: Some(Chunk {
            r#type: Encoding::Histogram as i32,
            data: vec![0xff; 8],
            hash: 0,
        }),
        ..Default::default()
    }
}

pub fn series(pairs: &[(&str, &str)], chunks: Vec<AggrChunk>) -> Series {
    Series {
        labels: labels(pairs),
        chunks,
    }
}

pub fn series_frame(series: Series) -> Frame {
    Ok(SeriesResponse {
        result: Some(series_response::Result::Series(series)),
    })
}

pub fn batch_frame(series: Vec<Series>) -> Frame {
    Ok(SeriesResponse {
        result: Some(series_response::Result::Batch(SeriesBatch { series })),
    })
}

pub fn warning_frame(warning: &str) -> Frame {
    Ok(SeriesResponse {
        result: Some(series_response::Result::Warning(warning.to_string())),
    })
}
