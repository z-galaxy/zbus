use std::{future::Future, io::ErrorKind, time::Duration};

use futures_lite::FutureExt;

use super::{Runtime, traits};
use crate::{Error, Result};

impl Runtime {
    /// Sleeps for `duration` on this runtime's timer.
    pub(crate) async fn sleep(&self, duration: Duration) {
        match self {
            #[cfg(feature = "builtin-runtime")]
            Self::Builtin(runtime) => traits::Runtime::sleep(runtime, duration).await,
            #[cfg(feature = "tokio")]
            Self::Tokio(runtime) => traits::Runtime::sleep(runtime, duration).await,
            Self::External(runtime) => traits::Runtime::sleep(runtime, duration).await,
        }
    }

    /// Awaits `fut`, failing with a timed-out error once `duration` has passed.
    pub(crate) async fn timeout<F, T>(&self, fut: F, duration: Duration) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        fut.or(async {
            self.sleep(duration).await;
            Err(Error::from(std::io::Error::new(
                ErrorKind::TimedOut,
                "timed out",
            )))
        })
        .await
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use std::pin::pin;

    use futures_util::poll;
    use ntest::timeout;

    use super::*;
    use crate::runtime::Tokio;

    /// A timeout is measured on the runtime's clock rather than the standard one.
    ///
    /// Tokio's clock is paused here and pushed a minute ahead of where it started, which is far
    /// enough that a deadline worked out from `Instant::now` on the standard clock would already
    /// have passed before the call was even made.
    #[tokio::test(start_paused = true)]
    #[timeout(15000)]
    async fn a_tokio_timeout_expires_on_the_tokio_clock() {
        tokio::time::advance(Duration::from_secs(60)).await;
        let runtime = Runtime::Tokio(Tokio::current().expect("a Tokio runtime is current"));
        let mut timing_out =
            pin!(runtime.timeout(std::future::pending::<Result<()>>(), Duration::from_secs(1)));

        // The first poll is what arms the timer, so the clock only moves once it is running.
        assert!(poll!(timing_out.as_mut()).is_pending());
        tokio::time::advance(Duration::from_millis(500)).await;
        assert!(poll!(timing_out.as_mut()).is_pending());

        tokio::time::advance(Duration::from_millis(600)).await;
        let error = timing_out.await.unwrap_err();
        assert!(
            matches!(&error, Error::InputOutput(e) if e.kind() == ErrorKind::TimedOut),
            "got {error:?}",
        );
    }
}
