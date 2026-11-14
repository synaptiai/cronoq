//! What the `cron` and `tz` features switch on and off. CI runs this file with each set of
//! features; the rest of the tests need the default features.

use std::time::Duration;

use cronoq_core::*;

fn core() -> Core<u32> {
    Core::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        ..Default::default()
    })
}

fn schedule(tz: Option<&str>) -> RecurringSpec<u32> {
    RecurringSpec {
        name: "r".into(),
        cron: "0 * * * * *".into(),
        tz: tz.map(Into::into),
        template: TaskSpec::new(1),
        deadline_after: None,
        overlap: OverlapPolicy::Skip,
        tolerance: Duration::from_secs(1),
        on_miss: MissPolicy::RunLate,
    }
}

#[test]
fn tasks_run_without_any_feature() {
    let mut c = core();
    let id = c.submit(Time(0), TaskSpec::new(7)).unwrap().id;
    let mut started = false;
    while let Some(a) = c.poll_action() {
        if let Action::Dispatch(d) = a {
            assert_eq!(d.id, id);
            started = true;
        }
    }
    assert!(started);
}

#[cfg(not(feature = "cron"))]
#[test]
fn without_cron_a_schedule_is_refused() {
    let mut c = core();
    let e = c.add_recurring(Time(0), schedule(None)).unwrap_err();
    assert!(e.contains("feature `cron`"), "{e}");
    assert!(c.list_recurring().is_empty());
}

#[cfg(feature = "cron")]
#[test]
fn with_cron_a_schedule_in_utc_is_accepted() {
    let mut c = core();
    c.add_recurring(Time(0), schedule(None)).unwrap();
    c.add_recurring(Time(0), schedule(Some("UTC"))).unwrap();
    let next = c.list_recurring()[0].next.unwrap();
    assert_eq!(next, Time(60_000_000_000));
}

#[cfg(all(feature = "cron", not(feature = "tz")))]
#[test]
fn without_tz_another_time_zone_is_refused() {
    let mut c = core();
    let e = c
        .add_recurring(Time(0), schedule(Some("Europe/Amsterdam")))
        .unwrap_err();
    assert!(e.contains("feature `tz`"), "{e}");
}

#[cfg(feature = "tz")]
#[test]
fn with_tz_a_time_zone_is_accepted_and_an_unknown_one_refused() {
    let mut c = core();
    c.add_recurring(Time(0), schedule(Some("Europe/Amsterdam")))
        .unwrap();
    let e = c
        .add_recurring(Time(0), schedule(Some("Mars/Olympus")))
        .unwrap_err();
    assert!(e.contains("unknown time zone"), "{e}");
}
