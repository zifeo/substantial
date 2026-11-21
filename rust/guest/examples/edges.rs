//! A guest at the edges its input names: calls awaited through combinators that poll only the
//! futures they were woken for, and durations longer than the longest.

// A native link would miss the host's imports, so a native build holds nothing.
#![cfg(target_family = "wasm")]

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::Deserialize;
use substantial_guest as s;

#[derive(Deserialize)]
struct Input {
    edge: String,
}

#[s::workflow]
async fn edges(c: s::Context, input: Input) -> s::Result<String> {
    let ms = std::time::Duration::from_millis;
    match input.edge.as_str() {
        "unordered" => {
            let mut sleeps: FuturesUnordered<_> =
                [10, 20].into_iter().map(|n| c.sleep(ms(n))).collect();
            let mut slept = 0;
            while let Some(done) = sleeps.next().await {
                done?;
                slept += 1;
            }
            Ok(slept.to_string())
        }
        // Past 30 futures, `join_all` polls only the woken ones.
        "join-all" => {
            let sleeps = (0..40).map(|n| c.sleep(ms(n % 4 * 10)));
            let slept = futures::future::join_all(sleeps).await;
            Ok(slept.into_iter().filter(Result::is_ok).count().to_string())
        }
        "huge-sleep" => c.sleep(ms(u64::MAX)).await.map(|()| "slept".to_owned()),
        "huge-receive" => c.receive::<String>("never").timeout(ms(u64::MAX)).await,
        "timeout-returned" => c.receive::<String>("never").timeout(ms(10)).await,
        other => Ok(format!("no edge {other}")),
    }
}

s::export!(edges);
