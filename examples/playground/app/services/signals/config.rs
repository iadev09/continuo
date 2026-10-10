use std::fmt;

use serde::Deserialize;
use tokio::signal::unix::SignalKind;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum UnixSignal {
    Hup,
    Usr1,
    Usr2,
}

impl UnixSignal {
    pub fn kind(self) -> SignalKind {
        match self {
            Self::Hup => SignalKind::hangup(),
            Self::Usr1 => SignalKind::user_defined1(),
            Self::Usr2 => SignalKind::user_defined2(),
        }
    }
}

impl fmt::Display for UnixSignal {
    fn fmt(
        &self,
        formatter: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        formatter.write_str(match self {
            Self::Hup => "HUP",
            Self::Usr1 => "USR1",
            Self::Usr2 => "USR2",
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SignalConfig {
    pub reload: UnixSignal,
    pub hit: UnixSignal,
}

impl SignalConfig {
    pub fn validate(&self) -> congif::Result<()> {
        if self.reload == self.hit {
            return Err(congif::Error::validation(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "reload and hit must use different signals",
            )));
        }
        Ok(())
    }

    pub fn commands(
        &self,
        pid: u32,
    ) -> (String, String) {
        (
            format!("kill -{} {pid}", self.reload),
            format!("kill -{} {pid}", self.hit),
        )
    }
}
