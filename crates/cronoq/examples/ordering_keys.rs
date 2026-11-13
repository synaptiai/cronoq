//! Ordering keys: ledger entries of one account run one at a time, in the order they were
//! submitted, while different accounts run in parallel. One entry fails and halts its
//! account until it is resumed.
//!
//! `cargo run -p cronoq --example ordering_keys`

use std::sync::Arc;
use std::time::Duration;

use cronoq::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sched = Scheduler::<(u32, i64), ()>::builder()
        .pool(
            Pool::fixed("ledger", 4).executor_fn(|_ctx, entry: Arc<(u32, i64)>| async move {
                let (account, amount) = *entry;
                tokio::time::sleep(Duration::from_millis(50)).await;
                if amount == 0 {
                    return Err(TaskError::permanent("an entry of zero"));
                }
                println!("account {account}: {amount:+}");
                Ok(())
            }),
        )
        .build()
        .await?;

    // Three accounts, four entries each, submitted interleaved. Four workers are free, but
    // each account is served by one of them at a time, so its entries apply in order.
    let mut handles = Vec::new();
    for amount in [100, -40, 0, 25] {
        for account in 1..=3 {
            let mut entry = Task::new((account, amount))
                .pool("ledger")
                .ordering_key(format!("account:{account}"));
            if account == 2 {
                // For this account, an entry that fails stops the ones behind it.
                entry = entry.halt_on_failure().retry(RetryPolicy::none());
            }
            handles.push(sched.submit(entry).await?);
        }
    }
    // Accounts 1 and 3 run to the end (their failed entry does not stop them); account 2
    // stops after its entry of zero.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for key in sched.halted_orderings("ledger", 10).await? {
        let info = sched.ordering("ledger", &key).await?.expect("a halted key");
        println!("{key} is halted with {} entries waiting", info.tasks);
        // Look into the failure, then let the rest run.
        sched.resume_ordering("ledger", &key).await?;
    }
    for h in handles {
        let _ = h.await;
    }
    sched.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
