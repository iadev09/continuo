// Application-owned providers, registered together by Application.
mod counter;
mod http;
mod live;
mod signals;
pub use counter::CounterService;
pub use http::HttpService;
pub use signals::SignalService;
