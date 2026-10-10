//! Application events; transport-specific contracts do not belong in Continuo.
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone, Debug, serde::Serialize)]
pub struct ServerAddressChanged {
    pub from: SocketAddr,
    pub to: SocketAddr,
    /// Old generation's drain allowance; None means no forced-close deadline.
    pub graceful_timeout: Option<Duration>,
}

impl continuo::Event for ServerAddressChanged {}
