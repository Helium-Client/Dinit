# Dinit

Dinit is a Helium component that lets its different parts and native plugins communicate through events. For example, it signals when the client starts or stops, during Helium updates, and when a frame is rendered. This lets plugins respond to those moments without depending directly on each component's code.

Dinit is not a standalone application: it works as part of Helium and provides a shared interface for exchanging events.

## For developers

Dinit provides a typed event bus for Rust and a versioned ABI for plugins written in C or C++.

### Using Rust

For local development, if Dinit is in a neighboring directory:

```toml
dinit = { path = "../Dinit" }
```

Example subscription to an event:

```rust
use dinit::{Dinit, TickPost};
use std::sync::Arc;

let bus = Arc::new(Dinit::new());
let _subscription = bus.subscribe::<TickPost, _>(0, |event| {
    println!("Helium update {} completed", event.tick);
});
```

The typed bus also defines `PlayerJoin`, `PlayerLeave`, `WorldChanged`, `ChatMessage`,
`ScreenChanged`, `KeyPressed`, `ModuleEnabled`, and `ModuleDisabled` events. `ChatMessage`
is cancellable. UI and render integrations should publish through `publish_main_thread`,
which rejects calls from any thread other than the one that created the bus.

Network hooks must enqueue owned packet data with `queue_network_packet` instead of
dispatching plugin callbacks on the network thread. The queue is bounded (1,024 packets,
2 MiB per packet); errors are returned if it is full or a packet is oversized. The client
main thread delivers queued events with `drain_network_events(limit)`. This ensures packet
listeners—including plugins that touch UI or rendering—run only on the main thread.
Queueing returns an error if Dinit has been closed.

`inspector_snapshot` exposes event counts, total/average/maximum dispatch time, cancellation
and panic counts, active listener names and priorities, per-listener timings, recent event
traces (last 256), and the pending packet count. Use `subscribe_named` to make listener
identities meaningful in an Inspector UI. Dinit is a standalone event service, so the
client's visual Inspector UI and game hooks must be supplied by its Helium integration.

### Native plugins

C and C++ consumers include `<dinit/dinit.h>`. The version-1 ABI remains compatible and
continues to expose the lifecycle, tick, and render events. The richer Minecraft events
and Inspector data are currently available through the Rust API; the client integration
must bridge them for native plugins and render the visual Inspector.

## Validation

Requires stable Rust/Cargo and a C compiler:

```sh
cargo fmt --all -- --check
cargo test --all-targets
cc -std=c11 -Wall -Wextra -Werror -I include tests/header_smoke.c -o /tmp/dinit-header-smoke
/tmp/dinit-header-smoke
```

Native plugins run inside the client process and are not sandboxed. The `TickPre` and `TickPost` events mark the beginning and end of Helium's module update as detected by the frame hook; they do not cover Minecraft's entire vanilla tick pipeline.
