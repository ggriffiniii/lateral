//! # published_value
//! `published_value` allows one thread to "publish" a value to a number of
//! other threads. The other threads can wait for the value to be published and
//! receive an immutable reference to the value once it is. Once published the
//! value is immutable. Requests for the value after it's been published are
//! efficient and do not require obtaining a lock.
//! # Examples
//! ```
//! let (publisher, waiter) = published_value::new();
//! let thread1 = std::thread::spawn({
//!     let waiter = waiter.clone();
//!     move || {
//!         let value = waiter.wait_for_value();
//!         format!("thread1 received value {}", value)
//!     }
//! });
//! let thread2 = std::thread::spawn({
//!     let waiter = waiter.clone();
//!     move || {
//!         let value = waiter.wait_for_value();
//!         format!("thread2 received value {}", value)
//!     }
//! });
//!
//! publisher.publish(42);
//! assert_eq!(thread1.join().unwrap(), "thread1 received value 42".to_string());
//! assert_eq!(thread2.join().unwrap(), "thread2 received value 42".to_string());
//! ```
use std::cell::Cell;
use std::fmt::{self, Debug};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// Creates a new published value. Returns a Publisher and Waiter pair. The
/// `Publisher` and `Waiter` are safe to send to different threads. Waiters can be
/// cloned and each instance of the clone will receive the published value.
pub fn new<T>() -> (Publisher<T>, Waiter<T>) {
    let value = Arc::new(Value {
        published: AtomicBool::new(false),
        mutex: Mutex::new(()),
        condvar: Condvar::new(),
        value: Cell::new(None),
    });
    (Publisher(value.clone()), Waiter(value))
}

/// `Publisher` publishes an immutable value to waiters.
#[derive(Debug)]
pub struct Publisher<T>(Arc<Value<T>>);

impl<T> Publisher<T> {
    /// Publishes the provided value to waiters. Once published, waiters will
    /// have access to an immutable reference of the provided value.
    pub fn publish(self, value: T) {
        let inner: &Value<T> = &self.0;
        let _lock = inner.mutex.lock().expect("lock poisoned");
        let ptr: *mut Option<T> = inner.value.as_ptr();
        unsafe {
            *ptr = Some(value);
        }
        inner.published.store(true, Ordering::Release);
        inner.condvar.notify_all();
    }
}

/// `Waiter` waits for a value that has been published by Publisher. A Waiter can
/// be cloned and cloned instances all receive the same published value.
#[derive(Debug)]
pub struct Waiter<T>(Arc<Value<T>>);

impl<T> Clone for Waiter<T> {
    fn clone(&self) -> Self {
        Waiter(self.0.clone())
    }
}

impl<T> Waiter<T> {
    /// Wait for the published value and return an immutable reference to it.
    pub fn wait_for_value(&self) -> &T {
        match self.0.try_get_value() {
            Some(value) => value,
            None => self._wait_for_value(),
        }
    }

    /// Attempt to get an immutable reference to the value, and return None if
    /// the value has not been published yet. This is guaranteed to succeed if
    /// wait_for_value has previously returned.
    pub fn try_get_value(&self) -> Option<&T> {
        self.0.try_get_value()
    }

    /// Attempt to gain exclusive ownership of the underlying value. This will
    /// only succeed if a value has been published *and* this is the only Waiter
    /// remaining. Otherwise, an [`Err`][result] is returned with the same
    /// `Waiter` that was passed in.
    ///
    /// [result]: ../../std/result/enum.Result.html
    pub fn into_value(self) -> Result<T, Waiter<T>> {
        Arc::try_unwrap(self.0)
            .map_err(Waiter)
            .and_then(|value| value.into_value().map_err(|value| Waiter(Arc::new(value))))
    }

    fn _wait_for_value(&self) -> &T {
        let inner: &Value<T> = &self.0;
        if !inner.published.load(Ordering::Acquire) {
            let mut lock = inner.mutex.lock().expect("lock poisoned");
            if !inner.published.load(Ordering::Relaxed) {
                while unsafe { inner.get_value() }.is_none() {
                    lock = inner.condvar.wait(lock).expect("lock poisoned");
                }
            }
        }
        unsafe { inner.get_value() }.unwrap()
    }
}

struct Value<T> {
    published: AtomicBool,
    mutex: Mutex<()>,
    condvar: Condvar,
    value: Cell<Option<T>>,
}

impl<T> Value<T> {
    fn try_get_value(&self) -> Option<&T> {
        if !self.published.load(Ordering::Acquire) {
            None
        } else {
            unsafe { self.get_value() }
        }
    }

    fn into_value(mut self) -> Result<T, Value<T>> {
        if !self.published.load(Ordering::Acquire) {
            Err(self)
        } else {
            Ok(self.value.get_mut().take().unwrap())
        }
    }

    unsafe fn get_value(&self) -> Option<&T> {
        (&*self.value.as_ptr()).as_ref()
    }
}

unsafe impl<T> Sync for Value<T> where T: Sync {}

impl<T> Debug for Value<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.try_get_value() {
            Some(value) => write!(f, "Published({:?})", value),
            None => write!(f, "NotPublished"),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn basic() {
        let (publisher, waiter) = ::new();
        let thread1 = ::std::thread::spawn({
            let waiter = waiter.clone();
            move || {
                let value = waiter.wait_for_value();
                format!("thread1 received value {}", value)
            }
        });
        let thread2 = ::std::thread::spawn({
            let waiter = waiter.clone();
            move || {
                let value = waiter.wait_for_value();
                format!("thread2 received value {}", value)
            }
        });

        publisher.publish(42);
        assert_eq!(
            thread1.join().unwrap(),
            "thread1 received value 42".to_string()
        );
        assert_eq!(
            thread2.join().unwrap(),
            "thread2 received value 42".to_string()
        );
    }

    #[test]
    fn try_get_value() {
        let (publisher, waiter) = ::new();
        assert_eq!(waiter.try_get_value(), None);
        publisher.publish(42);
        assert_eq!(waiter.try_get_value(), Some(&42));
    }

    #[test]
    fn into_value() {
        let (publisher, waiter) = ::new();
        // into_value on the clone will fail because there are multiple
        // instances of waiter.
        assert!(waiter.clone().into_value().is_err());
        // this is true even after a value is published.
        publisher.publish(42);
        assert!(waiter.clone().into_value().is_err());
        // once a value has been published and only one waiter remains that
        // waiter can be turned into the value.
        assert_eq!(waiter.into_value().unwrap(), 42);
    }

    #[test]
    fn not_cloneable() {
        #[derive(Debug, PartialEq)]
        struct NotClone(i64);
        let (publisher, waiter) = ::new();
        publisher.publish(NotClone(42));
        assert_eq!(waiter.wait_for_value(), &NotClone(42));
        assert_eq!(waiter.into_value().unwrap(), NotClone(42));
    }

    #[test]
    fn debug() {
        let (publisher, waiter) = ::new();
        assert_eq!(format!("{:?}", publisher), "Publisher(NotPublished)");
        assert_eq!(format!("{:?}", waiter), "Waiter(NotPublished)");
        publisher.publish(42);
        assert_eq!(format!("{:?}", waiter), "Waiter(Published(42))");
    }
}
