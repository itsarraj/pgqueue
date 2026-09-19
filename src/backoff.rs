use std::time::Duration;

/// Exponential backoff, deterministic and pure — no jitter. Delay doubles
/// per attempt starting at `base`, capped at `cap`. Attempt is 1-indexed
/// (the first retry after a failure is attempt 1).
///
/// Jitter (randomizing the delay to avoid a thundering herd when many jobs
/// fail at once) is a reasonable real-world addition but deliberately left
/// out here to keep this function pure and exactly reproducible in tests —
/// callers that want jitter can add it on top of this return value.
pub fn backoff_delay(attempt: u32, base: Duration, cap: Duration) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let shift = attempt.saturating_sub(1).min(31);
    let multiplier = 1u64.checked_shl(shift).unwrap_or(u64::MAX);
    let millis = (base.as_millis() as u64).saturating_mul(multiplier);
    let capped = millis.min(cap.as_millis() as u64);
    Duration::from_millis(capped)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_attempt_is_zero_delay() {
        assert_eq!(
            backoff_delay(0, Duration::from_secs(1), Duration::from_secs(300)),
            Duration::ZERO
        );
    }

    #[test]
    fn doubles_each_attempt_until_cap() {
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(300);
        assert_eq!(backoff_delay(1, base, cap), Duration::from_secs(1));
        assert_eq!(backoff_delay(2, base, cap), Duration::from_secs(2));
        assert_eq!(backoff_delay(3, base, cap), Duration::from_secs(4));
        assert_eq!(backoff_delay(4, base, cap), Duration::from_secs(8));
    }

    #[test]
    fn caps_instead_of_overflowing() {
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(300);
        assert_eq!(backoff_delay(20, base, cap), cap);
        // Also must not panic on attempt counts large enough to overflow a
        // naive 2^attempt without the shift-saturation guard.
        assert_eq!(backoff_delay(1_000_000, base, cap), cap);
    }

    #[test]
    fn is_deterministic() {
        let a = backoff_delay(5, Duration::from_millis(100), Duration::from_secs(60));
        let b = backoff_delay(5, Duration::from_millis(100), Duration::from_secs(60));
        assert_eq!(a, b);
    }
}
