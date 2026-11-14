//! Ordering keys: the edits of one document run one at a time, in the order they were
//! submitted, while different documents are edited in parallel. One edit fails and halts
//! its document until it is resumed.
//!
//! `cargo run -p cronoq --example ordering_keys`

use std::sync::Arc;
use std::time::Duration;

use cronoq::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sched = Scheduler::<(u32, &'static str), ()>::builder()
        .pool(Pool::fixed("edits", 4).executor_fn(
            |_ctx, edit: Arc<(u32, &'static str)>| async move {
                let (doc, text) = *edit;
                tokio::time::sleep(Duration::from_millis(50)).await;
                if text.is_empty() {
                    return Err(TaskError::permanent("an empty edit"));
                }
                println!("doc {doc}: {text}");
                Ok(())
            },
        ))
        .build()
        .await?;

    // Three documents, four edits each, submitted interleaved. Four workers are free, but
    // each document is served by one of them at a time, so its edits apply in order.
    let mut handles = Vec::new();
    for text in ["add title", "fix typo", "", "add summary"] {
        for doc in 1..=3 {
            let mut edit = Task::new((doc, text))
                .pool("edits")
                .ordering_key(format!("doc:{doc}"));
            if doc == 2 {
                // For this document, an edit that fails stops the ones behind it.
                edit = edit.halt_on_failure().retry(RetryPolicy::none());
            }
            handles.push(sched.submit(edit).await?);
        }
    }
    // Documents 1 and 3 run to the end (their failed edit does not stop them); document 2
    // stops after its empty edit.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for key in sched.halted_orderings("edits", 10).await? {
        let info = sched.ordering("edits", &key).await?.expect("a halted key");
        println!("{key} is halted with {} edit(s) waiting", info.tasks);
        // Look into the failure, then let the rest run.
        sched.resume_ordering("edits", &key).await?;
    }
    for h in handles {
        let _ = h.await;
    }
    sched.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
