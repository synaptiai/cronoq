//! Weighted fair sharing of a pool between tenants.

mod common;

use common::*;
use cronoq_core::*;

fn fair_pool(weights: &[(&str, f64)]) -> Sim {
    let mut p = PoolConfig::fixed("default", 1);
    p.fairness = Some(Fairness::weights(weights.iter().map(|(k, w)| (*k, *w))));
    Sim::with_pool(p)
}

fn tenant_of(s: &Sim, id: TaskId, ids: &[(TaskId, &'static str)]) -> &'static str {
    let _ = s;
    ids.iter().find(|(i, _)| *i == id).unwrap().1
}

/// Tenants of the first `n` dispatches.
fn first_dispatches(s: &Sim, ids: &[(TaskId, &'static str)], n: usize) -> Vec<&'static str> {
    s.started
        .iter()
        .take(n)
        .map(|x| tenant_of(s, x.id, ids))
        .collect()
}

fn count(v: &[&str], t: &str) -> usize {
    v.iter().filter(|x| **x == t).count()
}

#[test]
fn backlogged_tenants_share_by_weight() {
    let mut s = fair_pool(&[("acme", 3.0), ("globex", 1.0)]);
    let mut ids = Vec::new();
    // Globex submits all of its work first: without fairness it would run first.
    for _ in 0..40 {
        ids.push((
            s.submit(TaskSpec::new(Job::ok(1.0)).tenant("globex")),
            "globex",
        ));
    }
    for _ in 0..40 {
        ids.push((s.submit(TaskSpec::new(Job::ok(1.0)).tenant("acme")), "acme"));
    }
    s.run_until(t(41.0));
    let first = first_dispatches(&s, &ids, 40);
    let (a, g) = (count(&first, "acme"), count(&first, "globex"));
    assert_eq!(a + g, 40);
    // 3:1 of 40 is 30:10; the first task started before acme arrived.
    assert!((29..=31).contains(&a), "acme {a}, globex {g}: {first:?}");
}

#[test]
fn higher_priority_classes_still_run_first() {
    let mut s = fair_pool(&[("acme", 100.0), ("globex", 1.0)]);
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push((s.submit(TaskSpec::new(Job::ok(1.0)).tenant("acme")), "acme"));
    }
    let urgent = s.submit(
        TaskSpec::new(Job::ok(1.0))
            .tenant("globex")
            .priority(Priority::Critical),
    );
    s.run_until(t(10.0));
    // The first acme task was already running; the critical globex task is next.
    assert_eq!(s.started[1].id, urgent);
}

#[test]
fn an_idle_tenant_does_not_bank_credit() {
    let mut s = fair_pool(&[]);
    let mut ids = Vec::new();
    // Acme runs alone for a while.
    for _ in 0..20 {
        ids.push((s.submit(TaskSpec::new(Job::ok(1.0)).tenant("acme")), "acme"));
    }
    s.run_until(t(10.5));
    // Globex arrives with a backlog; acme still has 10 queued. Equal weights: they alternate,
    // instead of globex taking every slot to "catch up" on the time it was idle.
    for _ in 0..10 {
        ids.push((
            s.submit(TaskSpec::new(Job::ok(1.0)).tenant("globex")),
            "globex",
        ));
    }
    s.run_until(t(40.0));
    let after: Vec<&str> = s.started[11..21]
        .iter()
        .map(|x| tenant_of(&s, x.id, &ids))
        .collect();
    assert_eq!(count(&after, "globex"), 5, "{after:?}");
}

#[test]
fn without_fairness_tenants_are_ignored() {
    let mut s = Sim::with_pool(PoolConfig::fixed("default", 1));
    let mut order = Vec::new();
    for i in 0..10 {
        let t = if i < 5 { "globex" } else { "acme" };
        order.push(s.submit(TaskSpec::new(Job::ok(1.0)).tenant(t)));
    }
    s.run_until(t(20.0));
    let got: Vec<TaskId> = s.started.iter().map(|x| x.id).collect();
    assert_eq!(got, order, "submission order");
}

#[test]
fn invalid_weights_are_rejected() {
    let mut p = PoolConfig::fixed("default", 1);
    p.fairness = Some(Fairness::weights([("acme", 0.0)]));
    let cfg = CoreConfig {
        pools: vec![p],
        ..Default::default()
    };
    assert!(cfg.validate().unwrap_err().contains("tenant weights"));
}
