#![deny(unsafe_op_in_unsafe_fn)]

use core::any::{Any, TypeId};
use core::ffi::c_void;
use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::thread::ThreadId;
use std::time::Instant;

pub type Status = i32;
pub const OK: Status = 0;
pub const ERROR: Status = -1;
pub const INCOMPATIBLE_ABI: Status = -2;
pub const NOT_FOUND: Status = -3;
pub const INVALID_ARGUMENT: Status = -4;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ServiceHeaderV1 {
    pub struct_size: u32,
    pub version: u32,
}

pub const ABI_SERVICE_ID: &str = "helium.dinit.events";
pub const ABI_SERVICE_VERSION: u32 = 1;

pub const EVENT_CLIENT_STARTED: u32 = 1;
pub const EVENT_CLIENT_STOPPING: u32 = 2;
pub const EVENT_TICK_PRE: u32 = 10;
pub const EVENT_TICK_POST: u32 = 11;
pub const EVENT_RENDER_FRAME: u32 = 20;
pub const EVENT_PLAYER_JOIN: u32 = 30;
pub const EVENT_PLAYER_LEAVE: u32 = 31;
pub const EVENT_WORLD_CHANGED: u32 = 32;
pub const EVENT_CHAT_MESSAGE: u32 = 33;
pub const EVENT_SCREEN_CHANGED: u32 = 34;
pub const EVENT_KEY_PRESSED: u32 = 35;
pub const EVENT_MODULE_ENABLED: u32 = 36;
pub const EVENT_MODULE_DISABLED: u32 = 37;
pub const EVENT_NETWORK_PACKET: u32 = 40;

pub const EVENT_FLAG_CANCELLED: u32 = 1;
const INSPECTOR_HISTORY_LIMIT: usize = 256;
const NETWORK_QUEUE_LIMIT: usize = 1024;
const NETWORK_PACKET_SIZE_LIMIT: usize = 2 * 1024 * 1024;
const NETWORK_QUEUE_BYTES_LIMIT: usize = 8 * 1024 * 1024;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct DinitEventV1 {
    pub struct_size: u32,
    pub event_type: u32,
    pub flags: u32,
    pub reserved: u32,
    pub sequence: u64,
    pub tick: u64,
    pub delta_seconds: f32,
}

pub type DinitEventCallbackV1 = Option<
    unsafe extern "C" fn(user_context: *mut core::ffi::c_void, event: *mut DinitEventV1) -> i32,
>;

pub type DinitSubscribeFnV1 = Option<
    unsafe extern "C" fn(
        service_context: *mut core::ffi::c_void,
        event_type: u32,
        priority: i32,
        callback: DinitEventCallbackV1,
        user_context: *mut core::ffi::c_void,
        out_subscription_id: *mut u64,
    ) -> i32,
>;

pub type DinitUnsubscribeFnV1 = Option<
    unsafe extern "C" fn(service_context: *mut core::ffi::c_void, subscription_id: u64) -> i32,
>;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct DinitServiceV1 {
    pub header: crate::ServiceHeaderV1,
    pub context: *mut core::ffi::c_void,
    pub subscribe: DinitSubscribeFnV1,
    pub unsubscribe: DinitUnsubscribeFnV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventPriority {
    Lowest,
    Low,
    Normal,
    High,
    Highest,
}

impl EventPriority {
    pub const fn value(self) -> i32 {
        match self {
            Self::Lowest => -200,
            Self::Low => -100,
            Self::Normal => 0,
            Self::High => 100,
            Self::Highest => 200,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DispatchReport {
    pub invoked: usize,
    pub panicked: usize,
}

type ListenerFn = Arc<dyn Fn(&mut dyn Any) + Send + Sync + 'static>;

struct Listener {
    id: u64,
    priority: i32,
    event_type: &'static str,
    name: String,
    callback: ListenerFn,
}

#[derive(Default)]
struct ListenerState {
    by_type: HashMap<TypeId, Vec<Listener>>,
}

#[derive(Default)]
struct InspectorState {
    events: HashMap<String, EventStats>,
    recent_events: VecDeque<EventTrace>,
}

#[derive(Default)]
struct NetworkQueue {
    events: VecDeque<NetworkPacketEvent>,
    payload_bytes: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EventStats {
    pub event_type: String,
    pub total_events: u64,
    pub total_duration_ns: u128,
    pub max_duration_ns: u128,
    pub cancelled_events: u64,
    pub listener_panics: u64,
}

impl EventStats {
    pub fn average_duration_ns(&self) -> u128 {
        if self.total_events == 0 {
            0
        } else {
            self.total_duration_ns / u128::from(self.total_events)
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListenerInfo {
    pub id: u64,
    pub event_type: String,
    pub name: String,
    pub priority: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ListenerTrace {
    pub id: u64,
    pub name: String,
    pub duration_ns: u128,
    pub panicked: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventTrace {
    pub sequence: u64,
    pub event_type: String,
    pub duration_ns: u128,
    pub cancelled: bool,
    pub listeners: Vec<ListenerTrace>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InspectorSnapshot {
    pub events: Vec<EventStats>,
    pub recent_events: Vec<EventTrace>,
    pub listeners: Vec<ListenerInfo>,
    pub queued_network_packets: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThreadAffinityError;

impl core::fmt::Display for ThreadAffinityError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("operation must run on the Dinit main thread")
    }
}

impl std::error::Error for ThreadAffinityError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkQueueError {
    QueueFull,
    PacketTooLarge,
    Closed,
}

impl core::fmt::Display for NetworkQueueError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::QueueFull => formatter.write_str("Dinit network event queue is full"),
            Self::PacketTooLarge => formatter.write_str("network packet exceeds the 2 MiB limit"),
            Self::Closed => formatter.write_str("Dinit is closed"),
        }
    }
}

impl std::error::Error for NetworkQueueError {}

pub struct Dinit {
    listeners: RwLock<ListenerState>,
    dispatch_gate: RwLock<()>,
    next_id: AtomicU64,
    next_event_sequence: AtomicU64,
    accepting: std::sync::atomic::AtomicBool,
    main_thread: ThreadId,
    network_queue: Mutex<NetworkQueue>,
    inspector: Mutex<InspectorState>,
}

impl Dinit {
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(ListenerState::default()),
            dispatch_gate: RwLock::new(()),
            next_id: AtomicU64::new(1),
            next_event_sequence: AtomicU64::new(1),
            accepting: std::sync::atomic::AtomicBool::new(true),
            main_thread: std::thread::current().id(),
            network_queue: Mutex::new(NetworkQueue::default()),
            inspector: Mutex::new(InspectorState::default()),
        }
    }

    pub fn subscribe<E, F>(self: &Arc<Self>, priority: i32, callback: F) -> Subscription
    where
        E: Any + Send + 'static,
        F: Fn(&mut E) + Send + Sync + 'static,
    {
        self.subscribe_named::<E, F>(priority, core::any::type_name::<E>(), callback)
    }

    pub fn subscribe_named<E, F>(
        self: &Arc<Self>,
        priority: i32,
        name: impl Into<String>,
        callback: F,
    ) -> Subscription
    where
        E: Any + Send + 'static,
        F: Fn(&mut E) + Send + Sync + 'static,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let event_type = TypeId::of::<E>();
        let wrapped: ListenerFn = Arc::new(move |event| {
            if let Some(event) = event.downcast_mut::<E>() {
                callback(event);
            }
        });

        let mut state = self
            .listeners
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let listeners = state.by_type.entry(event_type).or_default();
        listeners.push(Listener {
            id,
            priority,
            event_type: core::any::type_name::<E>(),
            name: name.into(),
            callback: wrapped,
        });
        listeners.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.id.cmp(&right.id))
        });

        Subscription {
            dinit: Arc::downgrade(self),
            event_type,
            id,
            active: true,
        }
    }

    pub fn publish<E>(&self, event: &mut E) -> DispatchReport
    where
        E: Any + Send + 'static,
    {
        let _dispatch_guard = self
            .dispatch_gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.accepting.load(Ordering::Acquire) {
            return DispatchReport::default();
        }
        self.dispatch(event)
    }

    pub fn publish_main_thread<E>(
        &self,
        event: &mut E,
    ) -> Result<DispatchReport, ThreadAffinityError>
    where
        E: Any + Send + 'static,
    {
        self.ensure_main_thread()?;
        Ok(self.publish(event))
    }

    pub fn queue_network_packet(&self, event: NetworkPacketEvent) -> Result<(), NetworkQueueError> {
        if event.payload.len() > NETWORK_PACKET_SIZE_LIMIT {
            return Err(NetworkQueueError::PacketTooLarge);
        }
        let mut queue = self
            .network_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !self.accepting.load(Ordering::Acquire) {
            return Err(NetworkQueueError::Closed);
        }
        if queue.events.len() >= NETWORK_QUEUE_LIMIT
            || queue.payload_bytes.saturating_add(event.payload.len()) > NETWORK_QUEUE_BYTES_LIMIT
        {
            return Err(NetworkQueueError::QueueFull);
        }
        queue.payload_bytes += event.payload.len();
        queue.events.push_back(event);
        Ok(())
    }

    pub fn drain_network_events(&self, limit: usize) -> Result<usize, ThreadAffinityError> {
        self.ensure_main_thread()?;
        let events = {
            let mut queue = self
                .network_queue
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let count = limit.min(queue.events.len());
            let events = queue.events.drain(..count).collect::<Vec<_>>();
            queue.payload_bytes -= events
                .iter()
                .map(|event| event.payload.len())
                .sum::<usize>();
            events
        };
        let count = events.len();
        for mut event in events {
            self.publish(&mut event);
        }
        Ok(count)
    }

    pub fn inspector_snapshot(&self) -> InspectorSnapshot {
        let inspector = self
            .inspector
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let listeners = self
            .listeners
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut events = inspector.events.values().cloned().collect::<Vec<_>>();
        events.sort_by(|left, right| left.event_type.cmp(&right.event_type));
        let mut active_listeners = listeners
            .by_type
            .values()
            .flat_map(|entries| {
                entries.iter().map(move |listener| ListenerInfo {
                    id: listener.id,
                    event_type: listener.event_type.to_owned(),
                    name: listener.name.clone(),
                    priority: listener.priority,
                })
            })
            .collect::<Vec<_>>();
        active_listeners.sort_by(|left, right| {
            left.event_type
                .cmp(&right.event_type)
                .then_with(|| left.priority.cmp(&right.priority).reverse())
                .then_with(|| left.id.cmp(&right.id))
        });
        InspectorSnapshot {
            events,
            recent_events: inspector.recent_events.iter().cloned().collect(),
            listeners: active_listeners,
            queued_network_packets: self
                .network_queue
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .events
                .len(),
        }
    }

    fn ensure_main_thread(&self) -> Result<(), ThreadAffinityError> {
        if std::thread::current().id() == self.main_thread {
            Ok(())
        } else {
            Err(ThreadAffinityError)
        }
    }

    pub fn publish_terminal<E: TerminalEvent>(&self, event: &mut E) -> DispatchReport {
        let _dispatch_guard = self
            .dispatch_gate
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.dispatch(event)
    }

    pub fn close(&self) {
        let _dispatch_guard = self
            .dispatch_gate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.accepting.store(false, Ordering::Release);
        let mut queue = self
            .network_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        queue.events.clear();
        queue.payload_bytes = 0;
    }

    fn dispatch<E>(&self, event: &mut E) -> DispatchReport
    where
        E: Any + Send + 'static,
    {
        let callbacks: Vec<(u64, String, ListenerFn)> = {
            let state = self
                .listeners
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state
                .by_type
                .get(&TypeId::of::<E>())
                .map(|listeners| {
                    listeners
                        .iter()
                        .map(|listener| {
                            (
                                listener.id,
                                listener.name.clone(),
                                Arc::clone(&listener.callback),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

        let started = Instant::now();
        let event_name = core::any::type_name::<E>().to_owned();
        let mut listener_traces = Vec::with_capacity(callbacks.len());
        let mut report = DispatchReport::default();
        for (id, name, callback) in callbacks {
            let listener_started = Instant::now();
            report.invoked += 1;
            let panicked = catch_unwind(AssertUnwindSafe(|| callback(event))).is_err();
            if panicked {
                report.panicked += 1;
            }
            listener_traces.push(ListenerTrace {
                id,
                name,
                duration_ns: listener_started.elapsed().as_nanos(),
                panicked,
            });
        }
        self.record_event(
            event_name,
            started.elapsed().as_nanos(),
            is_cancelled(event as &dyn Any),
            report.panicked,
            listener_traces,
        );
        report
    }

    fn record_event(
        &self,
        event_type: String,
        duration_ns: u128,
        cancelled: bool,
        panicked: usize,
        listeners: Vec<ListenerTrace>,
    ) {
        let sequence = self.next_event_sequence.fetch_add(1, Ordering::Relaxed);
        let mut inspector = self
            .inspector
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stats = inspector.events.entry(event_type.clone()).or_default();
        stats.event_type = event_type.clone();
        stats.total_events += 1;
        stats.total_duration_ns += duration_ns;
        stats.max_duration_ns = stats.max_duration_ns.max(duration_ns);
        stats.cancelled_events += u64::from(cancelled);
        stats.listener_panics += panicked as u64;
        inspector.recent_events.push_back(EventTrace {
            sequence,
            event_type,
            duration_ns,
            cancelled,
            listeners,
        });
        if inspector.recent_events.len() > INSPECTOR_HISTORY_LIMIT {
            inspector.recent_events.pop_front();
        }
    }

    pub fn listener_count<E: Any>(&self) -> usize {
        self.listeners
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .by_type
            .get(&TypeId::of::<E>())
            .map_or(0, Vec::len)
    }

    pub fn synchronize(&self) {
        let _guard = self
            .dispatch_gate
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }

    fn remove(&self, event_type: TypeId, id: u64) {
        let mut state = self
            .listeners
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(listeners) = state.by_type.get_mut(&event_type) {
            listeners.retain(|listener| listener.id != id);
            if listeners.is_empty() {
                state.by_type.remove(&event_type);
            }
        }
    }
}

impl Default for Dinit {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Subscription {
    dinit: Weak<Dinit>,
    event_type: TypeId,
    id: u64,
    active: bool,
}

impl Subscription {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn unsubscribe(&mut self) {
        if !self.active {
            return;
        }
        if let Some(dinit) = self.dinit.upgrade() {
            dinit.remove(self.event_type, self.id);
        }
        self.active = false;
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.unsubscribe();
    }
}

struct AbiSubscription {
    owner_id: u64,
    subscription: Subscription,
}

pub struct DinitServiceHost {
    bus: Arc<Dinit>,
    next_owner_id: AtomicU64,
    next_subscription_id: AtomicU64,
    active_owners: Mutex<HashSet<u64>>,
    subscriptions: Mutex<HashMap<u64, AbiSubscription>>,
}

impl DinitServiceHost {
    pub fn new(bus: Arc<Dinit>) -> Arc<Self> {
        Arc::new(Self {
            bus,
            next_owner_id: AtomicU64::new(1),
            next_subscription_id: AtomicU64::new(1),
            active_owners: Mutex::new(HashSet::new()),
            subscriptions: Mutex::new(HashMap::new()),
        })
    }

    pub fn create_service(self: &Arc<Self>) -> DinitServiceHandle {
        let owner_id = self.next_owner_id.fetch_add(1, Ordering::Relaxed);
        self.active_owners
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(owner_id);

        let mut context = Box::new(DinitServiceContext {
            host: Arc::clone(self),
            owner_id,
        });
        let context_ptr = (&mut *context) as *mut DinitServiceContext as *mut c_void;
        let service = Box::new(DinitServiceV1 {
            header: crate::ServiceHeaderV1 {
                struct_size: core::mem::size_of::<DinitServiceV1>() as u32,
                version: ABI_SERVICE_VERSION,
            },
            context: context_ptr,
            subscribe: Some(abi_subscribe),
            unsubscribe: Some(abi_unsubscribe),
        });
        DinitServiceHandle {
            owner_id,
            context,
            service,
        }
    }

    pub fn close_owner(&self, owner_id: u64) {
        self.active_owners
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&owner_id);
        let removed = {
            let mut subscriptions = self
                .subscriptions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let ids: Vec<u64> = subscriptions
                .iter()
                .filter_map(|(id, entry)| (entry.owner_id == owner_id).then_some(*id))
                .collect();
            ids.into_iter()
                .filter_map(|id| subscriptions.remove(&id))
                .collect::<Vec<_>>()
        };
        drop(removed);
        self.bus.synchronize();
    }

    fn subscribe(
        &self,
        owner_id: u64,
        event_type: u32,
        priority: i32,
        callback: unsafe extern "C" fn(*mut c_void, *mut DinitEventV1) -> i32,
        user_context: *mut c_void,
    ) -> Result<u64, i32> {
        let active_owners = self
            .active_owners
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active_owners.contains(&owner_id) {
            return Err(crate::ERROR);
        }
        let id = self.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let user_context = user_context as usize;
        let listener_name = format!("native-owner-{owner_id}");
        let subscription = match event_type {
            EVENT_CLIENT_STARTED => {
                let sub = self.bus.subscribe_named::<ClientStarted, _>(
                    priority,
                    listener_name,
                    move |_| {
                        let _ = call_abi(callback, user_context, event_type, 0, 0, 0.0, 0);
                    },
                );
                sub
            }
            EVENT_CLIENT_STOPPING => {
                let sub = self.bus.subscribe_named::<ClientStopping, _>(
                    priority,
                    listener_name,
                    move |_| {
                        let _ = call_abi(callback, user_context, event_type, 0, 0, 0.0, 0);
                    },
                );
                sub
            }
            EVENT_TICK_PRE => {
                let sub =
                    self.bus
                        .subscribe_named::<TickPre, _>(priority, listener_name, move |event| {
                            let flags = if event.cancelled {
                                EVENT_FLAG_CANCELLED
                            } else {
                                0
                            };
                            if let Some(abi_event) = call_abi(
                                callback,
                                user_context,
                                event_type,
                                event.tick,
                                0,
                                0.0,
                                flags,
                            ) {
                                if abi_event.flags & EVENT_FLAG_CANCELLED != 0 {
                                    event.cancel();
                                }
                            }
                        });
                sub
            }
            EVENT_TICK_POST => {
                let sub = self.bus.subscribe_named::<TickPost, _>(
                    priority,
                    listener_name,
                    move |event| {
                        let flags = if event.was_cancelled {
                            EVENT_FLAG_CANCELLED
                        } else {
                            0
                        };
                        let _ = call_abi(
                            callback,
                            user_context,
                            event_type,
                            event.tick,
                            0,
                            0.0,
                            flags,
                        );
                    },
                );
                sub
            }
            EVENT_RENDER_FRAME => {
                let sub = self.bus.subscribe_named::<RenderFrame, _>(
                    priority,
                    listener_name,
                    move |event| {
                        let flags = if event.cancelled {
                            EVENT_FLAG_CANCELLED
                        } else {
                            0
                        };
                        if let Some(abi_event) = call_abi(
                            callback,
                            user_context,
                            event_type,
                            0,
                            event.frame,
                            event.delta_seconds,
                            flags,
                        ) {
                            if abi_event.flags & EVENT_FLAG_CANCELLED != 0 {
                                event.cancel();
                            }
                        }
                    },
                );
                sub
            }
            _ => return Err(crate::NOT_FOUND),
        };
        self.subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                id,
                AbiSubscription {
                    owner_id,
                    subscription,
                },
            );
        drop(active_owners);
        Ok(id)
    }

    fn unsubscribe(&self, owner_id: u64, id: u64) -> Result<(), i32> {
        let mut subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match subscriptions.get(&id) {
            Some(entry) if entry.owner_id == owner_id => {
                subscriptions.remove(&id);
                Ok(())
            }
            _ => Err(crate::NOT_FOUND),
        }
    }
}

pub struct DinitServiceHandle {
    owner_id: u64,
    context: Box<DinitServiceContext>,
    service: Box<DinitServiceV1>,
}

impl DinitServiceHandle {
    pub fn owner_id(&self) -> u64 {
        self.owner_id
    }

    pub fn service_header(&self) -> *const crate::ServiceHeaderV1 {
        &self.service.header
    }
}

impl Drop for DinitServiceHandle {
    fn drop(&mut self) {
        self.context.host.close_owner(self.owner_id);
    }
}

struct DinitServiceContext {
    host: Arc<DinitServiceHost>,
    owner_id: u64,
}

unsafe impl Send for DinitServiceHandle {}

unsafe extern "C" fn abi_subscribe(
    service_context: *mut c_void,
    event_type: u32,
    priority: i32,
    callback: DinitEventCallbackV1,
    user_context: *mut c_void,
    out_subscription_id: *mut u64,
) -> i32 {
    if out_subscription_id.is_null() {
        return crate::INVALID_ARGUMENT;
    }
    unsafe { *out_subscription_id = 0 };
    if service_context.is_null() {
        return crate::INVALID_ARGUMENT;
    }
    let Some(callback) = callback else {
        return crate::INVALID_ARGUMENT;
    };
    let context = unsafe { &*(service_context as *const DinitServiceContext) };
    match context.host.subscribe(
        context.owner_id,
        event_type,
        priority,
        callback,
        user_context,
    ) {
        Ok(id) => {
            unsafe { *out_subscription_id = id };
            crate::OK
        }
        Err(status) => status,
    }
}

unsafe extern "C" fn abi_unsubscribe(service_context: *mut c_void, subscription_id: u64) -> i32 {
    if service_context.is_null() {
        return crate::INVALID_ARGUMENT;
    }
    let context = unsafe { &*(service_context as *const DinitServiceContext) };
    match context.host.unsubscribe(context.owner_id, subscription_id) {
        Ok(()) => crate::OK,
        Err(status) => status,
    }
}

fn call_abi(
    callback: unsafe extern "C" fn(*mut c_void, *mut DinitEventV1) -> i32,
    user_context: usize,
    event_type: u32,
    tick: u64,
    sequence: u64,
    delta_seconds: f32,
    flags: u32,
) -> Option<DinitEventV1> {
    let mut event = DinitEventV1 {
        struct_size: core::mem::size_of::<DinitEventV1>() as u32,
        event_type,
        flags,
        reserved: 0,
        sequence,
        tick,
        delta_seconds,
    };
    let status = unsafe { callback(user_context as *mut c_void, &mut event as *mut DinitEventV1) };
    if status == crate::OK {
        Some(event)
    } else {
        None
    }
}

pub trait TerminalEvent: Any + Send + 'static {}

#[derive(Debug, Clone, Copy, Default)]
pub struct ClientStarted;

#[derive(Debug, Clone, Copy, Default)]
pub struct ClientStopping;

impl TerminalEvent for ClientStopping {}

#[derive(Debug, Clone, Copy)]
pub struct TickPre {
    pub tick: u64,
    pub cancelled: bool,
}

impl TickPre {
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TickPost {
    pub tick: u64,
    pub was_cancelled: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct RenderFrame {
    pub frame: u64,
    pub delta_seconds: f32,
    pub cancelled: bool,
}

impl RenderFrame {
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PlayerJoin {
    pub uuid: String,
    pub name: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PlayerLeave {
    pub uuid: String,
    pub name: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WorldChanged {
    pub previous: Option<String>,
    pub current: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ChatMessage {
    pub sender: Option<String>,
    pub message: String,
    pub cancelled: bool,
}

impl ChatMessage {
    pub fn cancel(&mut self) {
        self.cancelled = true;
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ScreenChanged {
    pub previous: Option<String>,
    pub current: Option<String>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct KeyPressed {
    pub key_code: i32,
    pub modifiers: u32,
    pub repeat: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModuleEnabled {
    pub module_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ModuleDisabled {
    pub module_id: String,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PacketDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NetworkPacketEvent {
    pub direction: PacketDirection,
    pub packet_id: u32,
    pub payload: Vec<u8>,
}

fn is_cancelled(event: &dyn Any) -> bool {
    event
        .downcast_ref::<TickPre>()
        .is_some_and(|event| event.cancelled)
        || event
            .downcast_ref::<RenderFrame>()
            .is_some_and(|event| event.cancelled)
        || event
            .downcast_ref::<TickPost>()
            .is_some_and(|event| event.was_cancelled)
        || event
            .downcast_ref::<ChatMessage>()
            .is_some_and(|event| event.cancelled)
}

#[cfg(test)]
mod tests {
    use super::{Dinit, RenderFrame, TickPost, TickPre};
    use std::sync::{Arc, Mutex};

    #[test]
    fn priorities_are_descending_and_registration_order_is_stable() {
        let bus = Arc::new(Dinit::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut subscriptions = Vec::new();
        for (priority, value) in [(0, 1), (100, 2), (100, 3), (-100, 4)] {
            let order = Arc::clone(&order);
            subscriptions.push(bus.subscribe::<TickPost, _>(priority, move |_| {
                order.lock().unwrap().push(value);
            }));
        }
        let mut event = TickPost {
            tick: 1,
            was_cancelled: false,
        };
        bus.publish(&mut event);
        assert_eq!(*order.lock().unwrap(), vec![2, 3, 1, 4]);
        drop(subscriptions);
    }

    #[test]
    fn dropping_subscription_unregisters_it() {
        let bus = Arc::new(Dinit::new());
        let subscription = bus.subscribe::<TickPre, _>(0, |_| {});
        assert_eq!(bus.listener_count::<TickPre>(), 1);
        drop(subscription);
        assert_eq!(bus.listener_count::<TickPre>(), 0);
    }

    #[test]
    fn listener_can_cancel_event_and_panics_do_not_stop_dispatch() {
        let bus = Arc::new(Dinit::new());
        let _panic = bus.subscribe::<RenderFrame, _>(100, |_| panic!("test listener panic"));
        let _cancel = bus.subscribe::<RenderFrame, _>(0, |event| event.cancel());
        let mut event = RenderFrame {
            frame: 1,
            delta_seconds: 0.016,
            cancelled: false,
        };
        let report = bus.publish(&mut event);
        assert!(event.cancelled);
        assert_eq!(report.invoked, 2);
        assert_eq!(report.panicked, 1);
    }

    #[test]
    fn inspector_tracks_named_listeners_duration_cancellation_and_panics() {
        use super::{ChatMessage, Dinit};

        let bus = Arc::new(Dinit::new());
        let _cancel = bus.subscribe_named::<ChatMessage, _>(10, "chat-filter", |event| {
            event.cancel();
        });
        let mut event = ChatMessage {
            sender: Some("player".to_owned()),
            message: "hello".to_owned(),
            cancelled: false,
        };
        bus.publish(&mut event);

        let snapshot = bus.inspector_snapshot();
        assert_eq!(snapshot.listeners.len(), 1);
        assert_eq!(snapshot.listeners[0].name, "chat-filter");
        assert_eq!(
            snapshot.listeners[0].event_type,
            core::any::type_name::<ChatMessage>()
        );
        assert_eq!(snapshot.recent_events.len(), 1);
        assert!(snapshot.recent_events[0].cancelled);
        assert_eq!(snapshot.recent_events[0].listeners.len(), 1);
        assert_eq!(snapshot.events[0].total_events, 1);
        assert_eq!(snapshot.events[0].cancelled_events, 1);
        assert_eq!(
            snapshot.events[0].average_duration_ns(),
            snapshot.events[0].total_duration_ns
        );
    }

    #[test]
    fn network_events_are_queued_until_drained_on_the_main_thread() {
        use super::{NetworkPacketEvent, PacketDirection};

        let bus = Arc::new(Dinit::new());
        let dispatch_thread = Arc::new(Mutex::new(None));
        let observed_thread = Arc::clone(&dispatch_thread);
        let _subscription = bus.subscribe::<NetworkPacketEvent, _>(0, move |_| {
            *observed_thread.lock().unwrap() = Some(std::thread::current().id());
        });

        let network_bus = Arc::clone(&bus);
        std::thread::spawn(move || {
            network_bus
                .queue_network_packet(NetworkPacketEvent {
                    direction: PacketDirection::Incoming,
                    packet_id: 7,
                    payload: vec![1, 2, 3],
                })
                .unwrap();
        })
        .join()
        .unwrap();

        assert!(dispatch_thread.lock().unwrap().is_none());
        assert_eq!(bus.inspector_snapshot().queued_network_packets, 1);
        assert_eq!(bus.drain_network_events(1).unwrap(), 1);
        assert_eq!(
            *dispatch_thread.lock().unwrap(),
            Some(std::thread::current().id())
        );
        assert_eq!(bus.inspector_snapshot().queued_network_packets, 0);
    }

    #[test]
    fn network_queue_rejects_closed_and_oversized_packets() {
        use super::{NetworkPacketEvent, NetworkQueueError, PacketDirection};

        let bus = Dinit::new();
        let oversized = NetworkPacketEvent {
            direction: PacketDirection::Incoming,
            packet_id: 1,
            payload: vec![0; super::NETWORK_PACKET_SIZE_LIMIT + 1],
        };
        assert_eq!(
            bus.queue_network_packet(oversized),
            Err(NetworkQueueError::PacketTooLarge)
        );
        bus.close();
        assert_eq!(
            bus.queue_network_packet(NetworkPacketEvent {
                direction: PacketDirection::Incoming,
                packet_id: 1,
                payload: Vec::new(),
            }),
            Err(NetworkQueueError::Closed)
        );
    }

    #[test]
    fn checked_main_thread_publish_rejects_other_threads() {
        let bus = Arc::new(Dinit::new());
        let worker_bus = Arc::clone(&bus);
        let result = std::thread::spawn(move || {
            let mut event = TickPre {
                tick: 1,
                cancelled: false,
            };
            worker_bus.publish_main_thread(&mut event)
        })
        .join()
        .unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn close_blocks_regular_events_but_allows_terminal_event() {
        use super::{ClientStopping, TerminalEvent};

        let bus = Arc::new(Dinit::new());
        let calls = Arc::new(Mutex::new(Vec::new()));
        let started_calls = Arc::clone(&calls);
        let _started = bus.subscribe::<TickPre, _>(0, move |_| {
            started_calls.lock().unwrap().push("tick");
        });
        let stopped_calls = Arc::clone(&calls);
        let _stopping = bus.subscribe::<ClientStopping, _>(0, move |_| {
            stopped_calls.lock().unwrap().push("stopping");
        });

        bus.close();
        let mut tick = TickPre {
            tick: 2,
            cancelled: false,
        };
        bus.publish(&mut tick);
        assert!(calls.lock().unwrap().is_empty());
        let mut stopping = ClientStopping;
        bus.publish_terminal(&mut stopping);
        assert_eq!(*calls.lock().unwrap(), vec!["stopping"]);
        fn assert_terminal<T: TerminalEvent>() {}
        assert_terminal::<ClientStopping>();
    }

    #[test]
    fn closing_owner_removes_listeners_before_returning() {
        use super::{DinitServiceHost, EVENT_TICK_PRE};
        use core::ffi::c_void;

        unsafe extern "C" fn cancel_tick(
            _context: *mut c_void,
            event: *mut super::DinitEventV1,
        ) -> i32 {
            if !event.is_null() {
                unsafe { (*event).flags |= super::EVENT_FLAG_CANCELLED };
            }
            crate::OK
        }

        let bus = Arc::new(Dinit::new());
        let host = DinitServiceHost::new(Arc::clone(&bus));
        let mut service = host.create_service();
        let service_table = unsafe { &*service.service };
        let mut subscription_id = 0;
        let status = unsafe {
            service_table.subscribe.unwrap()(
                service_table.context,
                EVENT_TICK_PRE,
                0,
                Some(cancel_tick),
                core::ptr::null_mut(),
                &mut subscription_id,
            )
        };
        assert_eq!(status, crate::OK);
        assert_eq!(bus.listener_count::<TickPre>(), 1);
        let mut before_close = TickPre {
            tick: 3,
            cancelled: false,
        };
        bus.publish(&mut before_close);
        assert!(before_close.cancelled);
        host.close_owner(service.owner_id());
        assert_eq!(bus.listener_count::<TickPre>(), 0);
        let mut event = TickPre {
            tick: 4,
            cancelled: false,
        };
        bus.publish(&mut event);
        assert!(!event.cancelled);
        drop(service);
    }
}
