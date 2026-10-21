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

### Native plugins

C and C++ consumers include `<dinit/dinit.h>`. The header defines the events and the versioned service interface shared with the Helium SDK.

## Validation

Requires stable Rust/Cargo and a C compiler:

```sh
cargo fmt --all -- --check
cargo test --all-targets
cc -std=c11 -Wall -Wextra -Werror -I include tests/header_smoke.c -o /tmp/dinit-header-smoke
/tmp/dinit-header-smoke
```

Native plugins run inside the client process and are not sandboxed. The `TickPre` and `TickPost` events mark the beginning and end of Helium's module update as detected by the frame hook; they do not cover Minecraft's entire vanilla tick pipeline.
