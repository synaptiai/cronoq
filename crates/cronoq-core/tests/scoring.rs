//! Priority and urgency settings: validation, and engine-wide versus per-pool behaviour.

mod common;

use common::*;
use cronoq_core::*;

fn job(d: f64) -> TaskSpec<Job> {
    TaskSpec::new(Job::ok(d))
}

fn with_cap(cap: f64) -> PriorityConfig {
    PriorityConfig {
        age_cap: [cap; 4],
        allow_overtaking_high: true,
        ..Default::default()
    }
}

// ------------------------------------------------------------------------------------------
// Validation
// ------------------------------------------------------------------------------------------

#[test]
fn defaults_are_valid_and_let_only_low_overtake_normal() {
    let p = PriorityConfig::default();
    p.validate().unwrap();
    UrgencyConfig::default().validate().unwrap();
    // Low can pass fresh normal work (10 + 800 > 100) and nothing can pass fresh high work.
    assert!(p.base[3] + p.age_cap[3] > p.base[2]);
    assert!(p.base[2] + p.age_cap[2] <= p.base[1]);
    assert!(p.base[1] + p.age_cap[1] <= p.base[0]);
}

#[test]
fn values_must_be_finite_and_not_negative() {
    for bad in [f64::NAN, f64::INFINITY, -1.0] {
        let mut p = PriorityConfig::default();
        p.aging_rate[2] = bad;
        assert!(p.validate().unwrap_err().contains("finite"), "{bad}");
        let mut p = PriorityConfig::default();
        p.age_cap[0] = bad;
        assert!(p.validate().unwrap_err().contains("finite"), "{bad}");
        let mut p = PriorityConfig::default();
        p.base[3] = bad;
        assert!(p.validate().is_err(), "{bad}");
        let u = UrgencyConfig {
            overdue_rate: bad,
            ..Default::default()
        };
        assert!(u.validate().unwrap_err().contains("finite"), "{bad}");
    }
    // Zero is allowed: no aging at all, or no urgency.
    PriorityConfig {
        aging_rate: [0.0; 4],
        age_cap: [0.0; 4],
        ..Default::default()
    }
    .validate()
    .unwrap();
    UrgencyConfig {
        max: 0.0,
        overdue_rate: 0.0,
        overdue_cap: 0.0,
        ..Default::default()
    }
    .validate()
    .unwrap();
}

#[test]
fn base_scores_must_be_strictly_ordered() {
    let equal = PriorityConfig {
        base: [10_000.0, 1_000.0, 100.0, 100.0],
        ..Default::default()
    };
    let e = equal.validate().unwrap_err();
    assert!(e.contains("strictly ordered") && e.contains("low"), "{e}");
    let swapped = PriorityConfig {
        base: [1_000.0, 10_000.0, 100.0, 10.0],
        allow_overtaking_high: true,
        ..Default::default()
    };
    // The flag covers aging only, never the order of the classes themselves.
    assert!(swapped.validate().unwrap_err().contains("strictly ordered"));
}

#[test]
fn overtaking_high_or_critical_needs_the_flag() {
    // Exactly reaching the higher base is allowed (a tie goes to the higher class).
    let edge = PriorityConfig {
        age_cap: [0.0, 9_000.0, 900.0, 990.0],
        ..Default::default()
    };
    edge.validate().unwrap();

    let normal_over_high = PriorityConfig {
        age_cap: [800.0, 800.0, 901.0, 800.0],
        ..Default::default()
    };
    let e = normal_over_high.validate().unwrap_err();
    assert!(e.contains("normal") && e.contains("high"), "{e}");
    assert!(e.contains("allow_overtaking_high"), "{e}");

    let low_over_high = PriorityConfig {
        age_cap: [800.0, 800.0, 800.0, 991.0],
        ..Default::default()
    };
    assert!(low_over_high.validate().unwrap_err().contains("low"));

    let high_over_critical = PriorityConfig {
        age_cap: [800.0, 9_001.0, 800.0, 800.0],
        ..Default::default()
    };
    let e = high_over_critical.validate().unwrap_err();
    assert!(e.contains("critical"), "{e}");

    for mut p in [normal_over_high, low_over_high, high_over_critical] {
        p.allow_overtaking_high = true;
        p.validate().unwrap();
    }

    // A class that does not age cannot overtake anything, whatever its cap.
    PriorityConfig {
        aging_rate: [2.0, 2.0, 2.0, 0.0],
        age_cap: [800.0, 800.0, 800.0, 50_000.0],
        ..Default::default()
    }
    .validate()
    .unwrap();
}

#[test]
fn engine_config_checks_the_global_and_each_pool() {
    let bad = PriorityConfig {
        age_cap: [12_000.0; 4],
        ..Default::default()
    };
    let cfg = CoreConfig {
        priority: bad.clone(),
        ..Default::default()
    };
    assert!(cfg.validate().unwrap_err().contains("overtake"));

    let mut pool = PoolConfig::fixed("batch", 1);
    pool.priority = Some(bad);
    let cfg = CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1), pool.clone()],
        ..Default::default()
    };
    let e = cfg.validate().unwrap_err();
    assert!(e.starts_with("pool 'batch':"), "{e}");

    pool.priority = None;
    pool.urgency = Some(UrgencyConfig {
        max: -1.0,
        ..Default::default()
    });
    let cfg = CoreConfig {
        pools: vec![pool],
        ..Default::default()
    };
    let e = cfg.validate().unwrap_err();
    assert!(
        e.starts_with("pool 'batch':") && e.contains("urgency"),
        "{e}"
    );
}

// ------------------------------------------------------------------------------------------
// Behaviour
// ------------------------------------------------------------------------------------------

/// A low task that has waited `waited` seconds against a high task submitted just now, on one
/// worker. Returns whether the low task started first.
fn low_runs_before_fresh_high(priority: PriorityConfig, waited: f64) -> bool {
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1)],
        priority,
        ..Default::default()
    });
    s.submit(job(waited));
    let low = s.submit(job(1.0).priority(Priority::Low));
    s.run_until(t(waited - 0.001));
    let high = s.submit(job(1.0).priority(Priority::High));
    s.run_to_completion(100.0);
    s.start_of(low).unwrap() < s.start_of(high).unwrap()
}

#[test]
fn a_raised_cap_lets_low_overtake_fresh_high_after_the_computed_time() {
    // 10 + 2w > 1000 once w > 495 s.
    assert!(!low_runs_before_fresh_high(with_cap(12_000.0), 490.0));
    assert!(low_runs_before_fresh_high(with_cap(12_000.0), 500.0));
    // With the default cap the low task tops out at 810 and never passes fresh high work.
    assert!(!low_runs_before_fresh_high(
        PriorityConfig::default(),
        5_000.0
    ));
}

#[test]
fn aging_rate_sets_how_soon_low_overtakes_fresh_normal() {
    let order = |rate: f64, waited: f64| {
        let mut s = Sim::new(CoreConfig {
            pools: vec![PoolConfig::fixed("default", 1)],
            priority: PriorityConfig {
                aging_rate: [rate; 4],
                ..Default::default()
            },
            ..Default::default()
        });
        s.submit(job(waited));
        let low = s.submit(job(1.0).priority(Priority::Low));
        s.run_until(t(waited - 0.001));
        let normal = s.submit(job(1.0));
        s.run_to_completion(100.0);
        s.start_of(low).unwrap() < s.start_of(normal).unwrap()
    };
    // Default rate 2: 10 + 2w > 100 once w > 45 s.
    assert!(!order(2.0, 44.0));
    assert!(order(2.0, 46.0));
    // Rate 9: once w > 10 s.
    assert!(!order(9.0, 9.0));
    assert!(order(9.0, 11.0));
    // No aging: never.
    assert!(!order(0.0, 10_000.0));
}

#[test]
fn a_pool_setting_overrides_the_engine_one_for_that_pool_only() {
    let mut tuned = PoolConfig::fixed("tuned", 1);
    tuned.priority = Some(with_cap(12_000.0));
    let mut s = Sim::new(CoreConfig {
        pools: vec![PoolConfig::fixed("default", 1), tuned],
        ..Default::default()
    });
    let mut pair = |pool: &'static str| {
        s.submit(job(600.0).pool(pool));
        let low = s.submit(job(1.0).pool(pool).priority(Priority::Low));
        (pool, low)
    };
    let lows = [pair("default"), pair("tuned")];
    s.run_until(t(599.999));
    let highs: Vec<TaskId> = lows
        .iter()
        .map(|(pool, _)| s.submit(job(1.0).pool(*pool).priority(Priority::High)))
        .collect();
    s.run_to_completion(100.0);
    let low_first = |i: usize| s.start_of(lows[i].1).unwrap() < s.start_of(highs[i]).unwrap();
    assert!(
        !low_first(0),
        "the default pool keeps the engine's cap of 800"
    );
    assert!(
        low_first(1),
        "the tuned pool's cap lets low pass high after 495 s"
    );

    let snap = s.core.snapshot(s.now);
    assert_eq!(
        snap.pool("default").unwrap().priority,
        PriorityConfig::default()
    );
    assert_eq!(snap.pool("tuned").unwrap().priority.age_cap, [12_000.0; 4]);
    assert_eq!(
        snap.pool("tuned").unwrap().urgency,
        UrgencyConfig::default()
    );
}

#[test]
fn a_pool_urgency_setting_changes_how_deadlines_rank_in_that_pool() {
    // A normal task whose deadline is due against fresh critical work: with the default
    // urgency (up to 5000 + 3000) it stays behind critical (10000); with a larger one it leads.
    let run = |urgency: Option<UrgencyConfig>| {
        let mut pool = PoolConfig::fixed("default", 1);
        pool.urgency = urgency;
        let mut s = Sim::with_pool(pool);
        s.submit(job(10.0));
        let due = s.submit(job(1.0).deadline(t(5.0)).duration_hint(secs(1.0)));
        s.run_until(t(9.999));
        let critical = s.submit(job(1.0).priority(Priority::Critical));
        s.run_to_completion(100.0);
        s.start_of(due).unwrap() < s.start_of(critical).unwrap()
    };
    assert!(!run(None));
    assert!(run(Some(UrgencyConfig {
        max: 20_000.0,
        ..Default::default()
    })));
}
