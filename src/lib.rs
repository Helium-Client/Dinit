#![deny(unsafe_op_in_unsafe_fn)]

use core::any::{Any, TypeId};
use core::ffi::c_void;
use core::sync::atomic::{AtomicU64, Ordering};
use std::collections::{HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Mutex, RwLock, Weak};

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

pub const EVENT_FLAG_CANCELLED: u32 = 1;

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

pub type DinitEventCallbackV1 = Option<unsafe extern "C" fn(
    user_context: *mut core::ffi::c_void,
    event: *mut DinitEventV1,
) -> i32>;

pub type DinitSubscribeFnV1 = Option<unsafe extern "C" fn(
    service_context: *mut core::ffi::c_void,
    event_type: u32,
    priority: i32,
    callback: DinitEventCallbackV1,
    user_context: *mut core::ffi::c_void,
    out_subscription_id: *mut u64,
) -> i32>;

pub type DinitUnsubscribeFnV1 = Option<unsafe extern "C" fn(
    service_context: *mut core::ffi::c_void,
    subscription_id: u64,
) -> i32>;

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
    callback: ListenerFn,
}

#[derive(Default)]
struct ListenerState {
    by_type: HashMap<TypeId, Vec<Listener>>,
}

pub struct Dinit {
    listeners: RwLock<ListenerState>,
    dispatch_gate: RwLock<()>,
    next_id: AtomicU64,
    accepting: std::sync::atomic::AtomicBool,
}

impl Dinit {
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(ListenerState::default()),
            dispatch_gate: RwLock::new(()),
            next_id: AtomicU64::new(1),
            accepting: std::sync::atomic::AtomicBool::new(true),
        }
    }

    pub fn subscribe<E, F>(self: &Arc<Self>, priority: i32, callback: F) -> Subscription
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
    }

    fn dispatch<E>(&self, event: &mut E) -> DispatchReport
    where
        E: Any + Send + 'static,
    {
        let callbacks: Vec<ListenerFn> = {
            let state = self
                .listeners
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state
                .by_type
                .get(&TypeId::of::<E>())
                .map(|listeners| listeners.iter().map(|listener| Arc::clone(&listener.callback)).collect())
                .unwrap_or_default()
        };

        let mut report = DispatchReport::default();
        for callback in callbacks {
            report.invoked += 1;
            if catch_unwind(AssertUnwindSafe(|| callback(event))).is_err() {
                report.panicked += 1;
            }
        }
        report
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
        let subscription = match event_type {
            EVENT_CLIENT_STARTED => {
                let sub = self.bus.subscribe::<ClientStarted, _>(priority, move |_| {
                    let _ = call_abi(callback, user_context, event_type, 0, 0, 0.0, 0);
                });
                sub
            }
            EVENT_CLIENT_STOPPING => {
                let sub = self.bus.subscribe::<ClientStopping, _>(priority, move |_| {
                    let _ = call_abi(callback, user_context, event_type, 0, 0, 0.0, 0);
                });
                sub
            }
            EVENT_TICK_PRE => {
                let sub = self.bus.subscribe::<TickPre, _>(priority, move |event| {
                    let flags = if event.cancelled { EVENT_FLAG_CANCELLED } else { 0 };
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
                let sub = self.bus.subscribe::<TickPost, _>(priority, move |event| {
                    let flags = if event.was_cancelled { EVENT_FLAG_CANCELLED } else { 0 };
                    let _ = call_abi(callback, user_context, event_type, event.tick, 0, 0.0, flags);
                });
                sub
            }
            EVENT_RENDER_FRAME => {
                let sub = self.bus.subscribe::<RenderFrame, _>(priority, move |event| {
                    let flags = if event.cancelled { EVENT_FLAG_CANCELLED } else { 0 };
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
                });
                sub
            }
            _ => return Err(crate::NOT_FOUND),
        };
        self.subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id, AbiSubscription { owner_id, subscription });
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
    match context.host.subscribe(context.owner_id, event_type, priority, callback, user_context) {
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
    let status = unsafe {
        callback(user_context as *mut c_void, &mut event as *mut DinitEventV1)
    };
    if status == crate::OK { Some(event) } else { None }
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
