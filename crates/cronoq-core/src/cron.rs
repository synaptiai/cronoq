//! Cron expressions and time zones, behind the `cron` and `tz` features.
//!
//! Without `cron` no schedule can be added. Without `tz` schedules run in UTC only, and the
//! time-zone database (most of the size of this crate) is not compiled in.

/// A parsed cron expression in its time zone.
pub(crate) struct Schedule {
    #[cfg(feature = "cron")]
    cron: croner::Cron,
    #[cfg(feature = "tz")]
    tz: chrono_tz::Tz,
}

impl Schedule {
    /// Parse `expr` (five fields, or six with seconds) for the IANA time zone `tz` (UTC when
    /// absent).
    #[cfg(feature = "cron")]
    pub(crate) fn parse(expr: &str, tz: Option<&str>) -> Result<Self, String> {
        let cron = croner::parser::CronParser::builder()
            .seconds(croner::parser::Seconds::Optional)
            .build()
            .parse(expr)
            .map_err(|e| format!("invalid cron '{expr}': {e}"))?;
        #[cfg(feature = "tz")]
        let tz: chrono_tz::Tz = match tz {
            Some(t) => t.parse().map_err(|_| format!("unknown time zone '{t}'"))?,
            None => chrono_tz::UTC,
        };
        #[cfg(not(feature = "tz"))]
        if let Some(t) = tz
            && !t.eq_ignore_ascii_case("UTC")
        {
            return Err(format!(
                "time zone '{t}': this build has no time-zone data (feature `tz`); \
                 schedules run in UTC"
            ));
        }
        Ok(Schedule {
            cron,
            #[cfg(feature = "tz")]
            tz,
        })
    }

    /// Without the `cron` feature there are no schedules.
    #[cfg(not(feature = "cron"))]
    pub(crate) fn parse(_expr: &str, _tz: Option<&str>) -> Result<Self, String> {
        Err("this build has no cron schedules (feature `cron`)".into())
    }

    /// The first occurrence strictly after `unix_secs`, in Unix nanoseconds.
    #[cfg(feature = "cron")]
    pub(crate) fn next_after(&self, unix_secs: i64) -> Option<i64> {
        use chrono::TimeZone;
        #[cfg(feature = "tz")]
        let dt = self.tz.timestamp_opt(unix_secs, 0).single()?;
        #[cfg(not(feature = "tz"))]
        let dt = chrono::Utc.timestamp_opt(unix_secs, 0).single()?;
        let next = self.cron.find_next_occurrence(&dt, false).ok()?;
        next.timestamp_nanos_opt()
    }

    /// Never reached: no `Schedule` exists without the `cron` feature.
    #[cfg(not(feature = "cron"))]
    pub(crate) fn next_after(&self, _unix_secs: i64) -> Option<i64> {
        None
    }
}

/// `unix_secs` as `YYYY-MM-DDTHH:MM:SSZ`, if it is a representable date.
pub(crate) fn utc_stamp(unix_secs: i64) -> Option<String> {
    #[cfg(feature = "cron")]
    {
        chrono::DateTime::from_timestamp(unix_secs, 0)
            .map(|t| t.format("%Y-%m-%dT%H:%M:%SZ").to_string())
    }
    #[cfg(not(feature = "cron"))]
    {
        let _ = unix_secs;
        None
    }
}
