//! A minimal 5-field cron expression parser (`minute hour day-of-month month
//! day-of-week`) and "next run at-or-after" calculator — just enough to
//! drive `pgqueue`'s recurring jobs (see `crate::scheduler`) without pulling
//! in a full-blown cron crate (none was already a transitive dependency of
//! this crate — checked `Cargo.lock` before writing this).
//!
//! Supports `*`, comma-separated lists (`1,15,30`), ranges (`9-17`), and
//! steps (`*/5`, `1-30/5`) in every field — enough for the vast majority of
//! real cron expressions. Deliberately **not** supported: month/weekday
//! names (`JAN`, `MON`), the non-standard `@daily`-style shorthands, and
//! seconds-level granularity (cron itself is minute-granularity; so is
//! this). Day-of-week is `0`-`6` with `0` = Sunday, the standard cron
//! convention (and it matches `chrono::Weekday::num_days_from_sunday`
//! exactly, which is why `next_after` doesn't need a translation table).

use std::collections::BTreeSet;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};

/// A parsed cron expression, pre-expanded into the concrete set of allowed
/// values per field so `next_after` is just set-membership checks against
/// candidate timestamps, not re-parsing the expression on every call.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    minute: BTreeSet<u32>,
    hour: BTreeSet<u32>,
    day_of_month: BTreeSet<u32>,
    month: BTreeSet<u32>,
    day_of_week: BTreeSet<u32>,
    // Standard (slightly surprising) cron rule: when *both*
    // day-of-month and day-of-week are restricted (neither is `*`), a date
    // matches if *either* field matches, not both. Tracked separately from
    // "does the set happen to contain every value" because `1-31` and `*`
    // mean the same set but different matching rules.
    dom_is_wildcard: bool,
    dow_is_wildcard: bool,
    source: String,
}

impl CronSchedule {
    /// Parses a 5-field cron expression. Returns an error naming the
    /// offending field rather than a bare parse failure.
    pub fn parse(expr: &str) -> Result<Self> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            bail!(
                "cron expression '{expr}' must have exactly 5 space-separated fields \
                 (minute hour day-of-month month day-of-week), found {}",
                fields.len()
            );
        }
        let minute = parse_field(fields[0], 0, 59).context("minute field")?;
        let hour = parse_field(fields[1], 0, 23).context("hour field")?;
        let day_of_month = parse_field(fields[2], 1, 31).context("day-of-month field")?;
        let month = parse_field(fields[3], 1, 12).context("month field")?;
        let day_of_week = parse_field(fields[4], 0, 6).context("day-of-week field (0=Sunday)")?;

        Ok(Self {
            dom_is_wildcard: fields[2].trim() == "*",
            dow_is_wildcard: fields[4].trim() == "*",
            minute,
            hour,
            day_of_month,
            month,
            day_of_week,
            source: expr.to_string(),
        })
    }

    /// The original expression string this was parsed from (for logging).
    pub fn source(&self) -> &str {
        &self.source
    }

    fn day_matches(&self, day_of_month: u32, day_of_week: u32) -> bool {
        match (self.dom_is_wildcard, self.dow_is_wildcard) {
            (true, true) => true,
            (false, true) => self.day_of_month.contains(&day_of_month),
            (true, false) => self.day_of_week.contains(&day_of_week),
            (false, false) => {
                self.day_of_month.contains(&day_of_month) || self.day_of_week.contains(&day_of_week)
            }
        }
    }

    /// The next time strictly after `from` that matches this schedule,
    /// truncated to minute granularity (cron's own granularity). Bounded to
    /// search at most 5 years out and returns an error rather than looping
    /// forever on an expression that can never match (e.g. `0 0 30 2 *` —
    /// February never has a 30th).
    pub fn next_after(&self, from: DateTime<Utc>) -> Result<DateTime<Utc>> {
        let mut candidate = truncate_to_minute(from + Duration::minutes(1));
        let limit = from + Duration::days(366 * 5);

        while candidate <= limit {
            if !self.month.contains(&candidate.month()) {
                candidate = start_of_next_month(candidate);
                continue;
            }
            let day_of_week = candidate.weekday().num_days_from_sunday();
            if !self.day_matches(candidate.day(), day_of_week) {
                candidate = start_of_next_day(candidate);
                continue;
            }
            if !self.hour.contains(&candidate.hour()) {
                candidate = start_of_next_hour(candidate);
                continue;
            }
            if !self.minute.contains(&candidate.minute()) {
                candidate += Duration::minutes(1);
                continue;
            }
            return Ok(candidate);
        }

        bail!(
            "cron expression '{}' has no matching run time in the next 5 years \
             (check for an impossible day, e.g. day-of-month 30 in February)",
            self.source
        );
    }
}

fn parse_field(field: &str, min: u32, max: u32) -> Result<BTreeSet<u32>> {
    let mut values = BTreeSet::new();
    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            bail!("empty segment in field '{field}'");
        }

        let (range_part, step) = match part.split_once('/') {
            Some((r, s)) => (
                r,
                s.parse::<u32>()
                    .with_context(|| format!("invalid step '{s}' in '{part}'"))?,
            ),
            None => (part, 1),
        };
        if step == 0 {
            bail!("step of 0 in '{part}' is not valid");
        }

        let (start, end) = if range_part == "*" {
            (min, max)
        } else if let Some((a, b)) = range_part.split_once('-') {
            let a: u32 = a
                .parse()
                .with_context(|| format!("invalid range start in '{part}'"))?;
            let b: u32 = b
                .parse()
                .with_context(|| format!("invalid range end in '{part}'"))?;
            if a > b {
                bail!("range '{part}' has start > end");
            }
            (a, b)
        } else {
            let v: u32 = range_part
                .parse()
                .with_context(|| format!("'{part}' is not '*', a number, a range, or a step"))?;
            (v, v)
        };

        if start < min || end > max {
            bail!("value(s) in '{part}' out of range {min}..={max}");
        }

        let mut v = start;
        while v <= end {
            values.insert(v);
            v += step;
        }
    }
    if values.is_empty() {
        bail!("field '{field}' produced no valid values");
    }
    Ok(values)
}

fn truncate_to_minute(d: DateTime<Utc>) -> DateTime<Utc> {
    d.with_second(0)
        .and_then(|d| d.with_nanosecond(0))
        .expect("zeroing seconds/nanoseconds on a valid DateTime never fails")
}

fn start_of_next_month(d: DateTime<Utc>) -> DateTime<Utc> {
    let (year, month) = if d.month() == 12 {
        (d.year() + 1, 1)
    } else {
        (d.year(), d.month() + 1)
    };
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
        .single()
        .expect("first of a month at midnight is always a valid, unambiguous timestamp")
}

fn start_of_next_day(d: DateTime<Utc>) -> DateTime<Utc> {
    let next = d + Duration::days(1);
    next.with_hour(0)
        .and_then(|d| d.with_minute(0))
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_nanosecond(0))
        .expect("midnight of the following day is always a valid timestamp")
}

fn start_of_next_hour(d: DateTime<Utc>) -> DateTime<Utc> {
    let next = d + Duration::hours(1);
    next.with_minute(0)
        .and_then(|d| d.with_second(0))
        .and_then(|d| d.with_nanosecond(0))
        .expect("the top of the following hour is always a valid timestamp")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn dt(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    #[test]
    fn every_minute() {
        let s = CronSchedule::parse("* * * * *").unwrap();
        let from = dt(2026, 1, 1, 12, 30);
        assert_eq!(s.next_after(from).unwrap(), dt(2026, 1, 1, 12, 31));
    }

    #[test]
    fn every_15_minutes_step() {
        let s = CronSchedule::parse("*/15 * * * *").unwrap();
        assert_eq!(
            s.next_after(dt(2026, 1, 1, 12, 1)).unwrap(),
            dt(2026, 1, 1, 12, 15)
        );
        assert_eq!(
            s.next_after(dt(2026, 1, 1, 12, 45)).unwrap(),
            dt(2026, 1, 1, 13, 0),
            "step wraps into the next hour"
        );
    }

    #[test]
    fn daily_at_specific_time() {
        let s = CronSchedule::parse("30 3 * * *").unwrap();
        assert_eq!(
            s.next_after(dt(2026, 1, 1, 12, 0)).unwrap(),
            dt(2026, 1, 2, 3, 30),
            "already past 3:30am today, so tomorrow"
        );
        assert_eq!(
            s.next_after(dt(2026, 1, 1, 1, 0)).unwrap(),
            dt(2026, 1, 1, 3, 30),
            "still before 3:30am today"
        );
    }

    #[test]
    fn weekday_range_business_hours() {
        // 9am on weekdays (Mon-Fri = 1-5).
        let s = CronSchedule::parse("0 9 * * 1-5").unwrap();
        // 2026-01-02 is a Friday.
        assert_eq!(dt(2026, 1, 2, 0, 0).weekday(), chrono::Weekday::Fri);
        // From Friday 10am, next match should skip the weekend to Monday.
        let next = s.next_after(dt(2026, 1, 2, 10, 0)).unwrap();
        assert_eq!(next.weekday(), chrono::Weekday::Mon);
        assert_eq!(next, dt(2026, 1, 5, 9, 0));
    }

    #[test]
    fn day_of_month_and_day_of_week_are_ored_when_both_restricted() {
        // The 1st of the month OR any Sunday - cron's OR rule when both
        // fields are non-wildcard.
        let s = CronSchedule::parse("0 0 1 * 0").unwrap();
        // 2026-01-04 is a Sunday, before the 1st-of-month case in Feb.
        let next = s.next_after(dt(2026, 1, 2, 0, 0)).unwrap();
        assert_eq!(next, dt(2026, 1, 4, 0, 0), "hits Sunday the 4th, not just the 1st");
    }

    #[test]
    fn list_of_specific_minutes() {
        let s = CronSchedule::parse("0,30 * * * *").unwrap();
        assert_eq!(
            s.next_after(dt(2026, 1, 1, 12, 10)).unwrap(),
            dt(2026, 1, 1, 12, 30)
        );
    }

    #[test]
    fn rejects_wrong_field_count() {
        assert!(CronSchedule::parse("* * * *").is_err());
        assert!(CronSchedule::parse("* * * * * *").is_err());
    }

    #[test]
    fn rejects_out_of_range_values() {
        assert!(CronSchedule::parse("60 * * * *").is_err(), "minute 60 is invalid");
        assert!(CronSchedule::parse("* 24 * * *").is_err(), "hour 24 is invalid");
        assert!(CronSchedule::parse("* * 32 * *").is_err(), "day 32 is invalid");
        assert!(CronSchedule::parse("* * * 13 *").is_err(), "month 13 is invalid");
        assert!(CronSchedule::parse("* * * * 7").is_err(), "day-of-week 7 is invalid (0-6 only)");
    }

    #[test]
    fn rejects_impossible_date_instead_of_hanging() {
        // Day-of-month 30 combined with only February selected can never
        // match (Feb has at most 29 days) - must return an error quickly,
        // not loop for the full 5-year search bound silently forever.
        let s = CronSchedule::parse("0 0 30 2 *").unwrap();
        assert!(s.next_after(dt(2026, 1, 1, 0, 0)).is_err());
    }

    #[test]
    fn leap_day_is_found() {
        let s = CronSchedule::parse("0 0 29 2 *").unwrap();
        let next = s.next_after(dt(2026, 1, 1, 0, 0)).unwrap();
        assert_eq!(next, dt(2028, 2, 29, 0, 0), "2028 is the next leap year after 2026");
    }
}
