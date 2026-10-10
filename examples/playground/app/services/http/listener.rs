//! A policy generation owns admission and drain, not the bound socket.
use axum::extract::connect_info::Connected;
use axum::serve::{IncomingStream, Listener};
use axum::{Extension, Router};
use continuo::{Gate, GateDrainOutcome, Permit, Result, RunContext};
use std::future::{Future, IntoFuture};
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub struct PeerAddress(pub SocketAddr);

impl Connected<IncomingStream<'_, PolicyListener>> for PeerAddress {
    fn connect_info(stream: IncomingStream<'_, PolicyListener>) -> Self {
        Self(*stream.remote_addr())
    }
}

pub struct Generation {
    gate: Gate,
    drain: CancellationToken,
    grace: Option<Duration>,
}

impl Generation {
    pub fn graceful_timeout(&self) -> Option<Duration> {
        self.grace
    }

    pub fn stop(&self) {
        self.gate.graceful_shutdown(self.grace);
        self.drain.cancel();
    }
}

pub fn gate(
    max_connections: Option<NonZeroUsize>,
    acquire_timeout: Duration,
) -> Gate {
    Gate::new(max_connections.map(|limit| limit.get()), acquire_timeout)
}

pub fn start(
    generations: &mut JoinSet<Result<()>>,
    socket: Arc<TcpListener>,
    gate: Gate,
    grace: Option<Duration>,
    app: Router,
    context: &RunContext,
) -> Generation {
    let drain = context.cancellation_token().child_token();
    let generation =
        Generation { gate: gate.clone(), drain: drain.clone(), grace };
    generations.spawn(async move {
        let forced = CancellationToken::new();
        let failure = Arc::new(Mutex::new(None));
        let listener = PolicyListener {
            socket,
            gate: gate.clone(),
            failure: failure.clone(),
            forced: forced.clone(),
            grace,
        };
        let shutdown = drain.clone();
        let shutdown_gate = gate.clone();
        let request_shutdown = forced.clone();
        let app = app.layer(axum::middleware::from_fn(move |request: axum::extract::Request, next: axum::middleware::Next| {
            let forced = request_shutdown.clone();
            async move {
                tokio::select! {
                    biased;
                    _ = forced.cancelled() => {
                        axum::response::IntoResponse::into_response(axum::http::StatusCode::SERVICE_UNAVAILABLE)
                    }
                    response = next.run(request) => response,
                }
            }
        }));
        let server = axum::serve(
            listener,
            app.layer(Extension(drain))
                .into_make_service_with_connect_info::<PeerAddress>(),
        )
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = shutdown.cancelled() => {},
                _ = shutdown_gate.wait_graceful_shutdown() => {},
            }
            shutdown_gate.graceful_shutdown(grace);
            shutdown.cancel();
        })
        .into_future();
        tokio::pin!(server);
        let drain_gate = gate.clone();
        let draining = async move {
            drain_gate.wait_graceful_shutdown().await;
            drain_gate.wait_all_done().await
        };
        tokio::pin!(draining);
        let result = tokio::select! {
            result = &mut server => result,
            outcome = &mut draining => {
                if let GateDrainOutcome::Forced { remaining } = outcome {
                    tracing::warn!(remaining, "HTTP grace expired; closing old generation connections");
                    forced.cancel();
                }
                server.await
            }
        };
        result?;
        if let Some(error) =
            failure.lock().expect("HTTP accept failure poisoned").take()
        {
            return Err(error.into());
        }
        Ok(())
    });
    generation
}

pub struct PolicyListener {
    socket: Arc<TcpListener>,
    gate: Gate,
    failure: Arc<Mutex<Option<io::Error>>>,
    forced: CancellationToken,
    grace: Option<Duration>,
}

impl PolicyListener {
    fn log_admission(
        &self,
        address: SocketAddr,
        status: &str,
    ) {
        match self.gate.max_count() {
            Some(capacity) => {
                tracing::debug!(%address, "HTTP connection {status} — {}/{capacity}", self.gate.count())
            }
            None => {
                tracing::debug!(%address, "HTTP connection {status} — {}/unlimited", self.gate.count())
            }
        }
    }
}

impl Listener for PolicyListener {
    type Io = Connection;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Connection, SocketAddr) {
        loop {
            let accepted = tokio::select! {
                biased;
                _ = self.gate.wait_graceful_shutdown() => {
                    return std::future::pending().await;
                }
                accepted = self.socket.accept() => accepted,
            };
            match accepted {
                Ok((stream, address)) => {
                    self.log_admission(address, "accepted; awaiting permit");
                    match self.gate.enter().await {
                        Ok(permit) => {
                            self.log_admission(address, "permitted");
                            return (
                                Connection {
                                    stream,
                                    _permit: permit,
                                    forced: Box::pin(
                                        self.forced.clone().cancelled_owned(),
                                    ),
                                },
                                address,
                            );
                        }
                        Err(continuo::gate::Error::AcquireTimeout(timeout)) => {
                            match self.gate.max_count() {
                                Some(capacity) => {
                                    tracing::debug!(%address, ?timeout, "HTTP connection rejected: admission timed out — {}/{capacity}", self.gate.count())
                                }
                                None => {
                                    tracing::debug!(%address, ?timeout, "HTTP connection rejected: admission timed out — {}/unlimited", self.gate.count())
                                }
                            }
                        }
                        Err(error) => {
                            self.log_admission(
                                address,
                                "rejected; gate shutting down",
                            );
                            tracing::trace!(%address, %error, "HTTP admission interrupted");
                            return std::future::pending().await;
                        }
                    }
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::Interrupted
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::ConnectionReset
                    ) =>
                {
                    continue;
                }
                Err(error) => {
                    // No invented retry cadence: stop this generation and
                    // propagate the accept failure to the service runtime.
                    *self
                        .failure
                        .lock()
                        .expect("HTTP accept failure poisoned") = Some(error);
                    self.gate.graceful_shutdown(self.grace);
                    return std::future::pending().await;
                }
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }
}

pub struct Connection {
    stream: TcpStream,
    _permit: Permit,
    forced: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}

impl Connection {
    fn check_shutdown(
        &mut self,
        cx: &mut Context<'_>,
    ) -> io::Result<()> {
        if self.forced.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "HTTP graceful timeout elapsed",
            ))
        } else {
            Ok(())
        }
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.check_shutdown(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(error) = self.check_shutdown(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.check_shutdown(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
