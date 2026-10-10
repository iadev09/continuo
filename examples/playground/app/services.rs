// Application-owned services, composed through Register by Application.
mod counter;
mod http;
mod live;
mod other;
mod reload;
mod signals;
pub use counter::CounterService;
pub use http::{HttpService, PeerAddress};
pub use other::OtherService;
pub use reload::ReloadService;
pub use signals::SignalService;
