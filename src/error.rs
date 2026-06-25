//! Typed failure outcomes with rich, fail-fast diagnostic context.

use std::time::Duration;

/// Circuit-breaker state at the moment of failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Disabled,
    Closed,
    Open,
    HalfOpen,
}

/// Diagnostic context attached to every [`ExecutionError`].
#[derive(Debug, Clone)]
pub struct ErrorContext {
    pub attempts: u32,
    pub elapsed: Duration,
    pub last_delay: Option<Duration>,
    pub breaker_state: BreakerState,
}

/// Why an execution failed. Boxed context keeps the hot `Result` small.
#[non_exhaustive]
#[derive(Debug)]
pub enum ExecutionError<E> {
    Operation {
        source: E,
        context: Box<ErrorContext>,
    },
    AttemptTimeout {
        context: Box<ErrorContext>,
    },
    TotalTimeout {
        context: Box<ErrorContext>,
    },
    CircuitOpen {
        context: Box<ErrorContext>,
    },
    ConcurrencyRejected {
        context: Box<ErrorContext>,
    },
    RetryBudgetExhausted {
        context: Box<ErrorContext>,
    },
}

impl<E> ExecutionError<E> {
    /// Diagnostic context (attempts, elapsed, last delay, breaker state).
    pub fn context(&self) -> &ErrorContext {
        match self {
            Self::Operation { context, .. }
            | Self::AttemptTimeout { context }
            | Self::TotalTimeout { context }
            | Self::CircuitOpen { context }
            | Self::ConcurrencyRejected { context }
            | Self::RetryBudgetExhausted { context } => context,
        }
    }

    /// Recover the underlying operation error, if this was an operation failure.
    pub fn into_inner(self) -> Option<E> {
        match self {
            Self::Operation { source, .. } => Some(source),
            _ => None,
        }
    }

    pub fn is_timeout(&self) -> bool {
        matches!(
            self,
            Self::AttemptTimeout { .. } | Self::TotalTimeout { .. }
        )
    }
    pub fn is_circuit_open(&self) -> bool {
        matches!(self, Self::CircuitOpen { .. })
    }
    pub fn is_rejected(&self) -> bool {
        matches!(self, Self::ConcurrencyRejected { .. })
    }
    pub fn is_exhausted(&self) -> bool {
        matches!(
            self,
            Self::Operation { .. } | Self::RetryBudgetExhausted { .. }
        )
    }
}

impl<E: std::fmt::Display> std::fmt::Display for ExecutionError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ctx = self.context();
        match self {
            Self::Operation { source, .. } => write!(
                f,
                "operation failed after {} attempt(s) in {:?}: {source}",
                ctx.attempts, ctx.elapsed
            ),
            Self::AttemptTimeout { .. } => {
                write!(f, "attempt timed out (attempt {})", ctx.attempts)
            }
            Self::TotalTimeout { .. } => {
                write!(
                    f,
                    "total timeout after {:?} ({} attempts)",
                    ctx.elapsed, ctx.attempts
                )
            }
            Self::CircuitOpen { .. } => write!(f, "circuit open"),
            Self::ConcurrencyRejected { .. } => {
                write!(f, "concurrency limit rejected the call")
            }
            Self::RetryBudgetExhausted { .. } => write!(f, "retry budget exhausted"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ExecutionError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> Box<ErrorContext> {
        Box::new(ErrorContext {
            attempts: 3,
            elapsed: Duration::from_millis(120),
            last_delay: Some(Duration::from_millis(50)),
            breaker_state: BreakerState::Disabled,
        })
    }

    #[test]
    fn predicates_and_context() {
        let e: ExecutionError<std::io::Error> = ExecutionError::TotalTimeout { context: ctx() };
        assert!(e.is_timeout());
        assert!(!e.is_circuit_open());
        assert_eq!(e.context().attempts, 3);
    }

    #[test]
    fn into_inner_recovers_operation_error() {
        let src = std::io::Error::other("boom");
        let e = ExecutionError::Operation {
            source: src,
            context: ctx(),
        };
        assert_eq!(e.into_inner().unwrap().to_string(), "boom");
    }

    #[test]
    fn error_source_chains() {
        use std::error::Error;
        let src = std::io::Error::other("io fail");
        let e = ExecutionError::Operation {
            source: src,
            context: ctx(),
        };
        assert!(e.source().is_some());
    }
}
