pub mod dynamic_pool;
pub mod static_pool;

use self::dynamic_pool::DynamicPool;
use self::static_pool::StaticPool;
use reducer::{CollectReducer, Reducer};
use std::fmt::Debug;
use std::iter::FromIterator;
use std::marker::PhantomData;
use worker::Worker;

/// Pool allows for parallel processing of items. When creating a pool a `Worker`
/// and `Reducer` are provided. The worker accepts input and produces one output
/// value for every input value. The reducer reduces the output from the worker
/// into a single resulting value. Once created you can add input values to the
/// pool using `add()`. Once all input has been added you can invoke either
/// `wait()` or `wait_handle()` to wait for all input to be processed and
/// retrieve the reduced output value.
pub trait Pool<I> {
    type Output;
    type WaitHandle: WaitHandle<Output = Self::Output>;

    fn add(&self, input: I);
    fn wait_handle(self) -> Self::WaitHandle;
    fn wait(self) -> Self::Output;
}

/// WaitHandle provides a handle to wait for the reduced output value.
pub trait WaitHandle: Clone {
    type Output;
    fn wait(&self) -> &Self::Output;
}

// This is an over engineered builder for learning purposes. The goal is to
// enforce at compile time that all necessary arguments are provided.
#[derive(Debug)]
pub struct Yes<T>(T);
#[derive(Debug)]
pub struct No;

pub trait ToAssign {}
pub trait Assigned: ToAssign {
    type T;
    fn value(self) -> Self::T;
}
pub trait NotAssigned: ToAssign {}

impl<T> ToAssign for Yes<T> {}
impl ToAssign for No {}

impl<T> Assigned for Yes<T> {
    type T = T;
    fn value(self) -> Self::T {
        self.0
    }
}
impl NotAssigned for No {}

/// Builder provides a fluent builder api for Pool.
#[derive(Debug)]
pub struct Builder<WorkerInputT, WorkerT, ReducerT, LimitT> {
    worker_input: PhantomData<WorkerInputT>,
    worker: WorkerT,
    reducer: ReducerT,
    concurrency_limit: LimitT,
}

/// Create a new pool Builder.
pub fn new() -> Builder<No, No, No, No> {
    Builder {
        worker_input: PhantomData {},
        worker: No,
        reducer: No,
        concurrency_limit: No,
    }
}

impl<WorkerInputT, WorkerT, ReducerT, LimitT> Builder<WorkerInputT, WorkerT, ReducerT, LimitT>
where
    WorkerInputT: ToAssign,
    WorkerT: ToAssign,
    ReducerT: ToAssign,
    LimitT: ToAssign,
{
    /// Set the worker to be used.
    pub fn set_worker<I, W>(self, w: W) -> Builder<Yes<I>, Yes<W>, ReducerT, LimitT>
    where
        WorkerT: NotAssigned,
        W: Worker<I>,
    {
        Builder {
            worker_input: PhantomData::<Yes<I>> {},
            worker: Yes(w),
            reducer: self.reducer,
            concurrency_limit: self.concurrency_limit,
        }
    }

    /// Set the reducer to be used. Note that set_reducer and collect_into are
    /// mutually exclusive. You must use one or the other, but not both.
    pub fn set_reducer<R>(self, r: R) -> Builder<WorkerInputT, WorkerT, Yes<R>, LimitT>
    where
        ReducerT: NotAssigned,
    {
        Builder {
            worker_input: PhantomData {},
            worker: self.worker,
            reducer: Yes(r),
            concurrency_limit: self.concurrency_limit,
        }
    }

    /// Collect the results of the workout output into the specified collection.
    /// Note that set_reducer and collect_into are mutually exclusive. You must
    /// use one or the other, but not both.
    #[cfg_attr(feature = "cargo-clippy", allow(type_complexity))]
    pub fn collect_into<C>(
        self,
    ) -> Builder<
        WorkerInputT,
        WorkerT,
        Yes<
            CollectReducer<
                <<WorkerT as Assigned>::T as Worker<<WorkerInputT as Assigned>::T>>::Output,
                C,
            >,
        >,
        LimitT,
    >
    where
        WorkerInputT: Assigned,
        WorkerT: Assigned,
        WorkerT::T: Worker<WorkerInputT::T>,
        ReducerT: NotAssigned,
        <<WorkerT as Assigned>::T as Worker<<WorkerInputT as Assigned>::T>>::Output: Send + 'static,
        C: FromIterator<
                <<WorkerT as Assigned>::T as Worker<<WorkerInputT as Assigned>::T>>::Output,
            > + Send
            + 'static,
    {
        self.set_reducer(CollectReducer::<
            <<WorkerT as Assigned>::T as Worker<WorkerInputT::T>>::Output,
            C,
        >::new())
    }

    /// Set the concurrency limit of the pool.
    pub fn set_concurrency_limit(
        self,
        concurrency_limit: i64,
    ) -> Builder<WorkerInputT, WorkerT, ReducerT, Yes<i64>> {
        Builder {
            worker_input: PhantomData {},
            worker: self.worker,
            reducer: self.reducer,
            concurrency_limit: Yes(concurrency_limit),
        }
    }
}

impl<I, W, R> Builder<Yes<I>, Yes<W>, Yes<R>, Yes<i64>> {
    /// Create a pool that allows the concurrency limit to be adjusted
    /// dynamically at runtime.
    pub fn create_dynamic_pool(self) -> DynamicPool<I, W, R>
    where
        I: Send + Debug + 'static,
        W: Worker<I> + Send + Sync + 'static,
        W::Output: Send + Debug,
        R: Reducer<W::Output> + Send + 'static,
        R::Output: Send + Sync + Debug,
    {
        DynamicPool::create(
            self.worker.value(),
            self.reducer.value(),
            self.concurrency_limit.value(),
        )
    }

    /// Create a pool that has a static concurrency limit.
    pub fn create_static_pool(self) -> StaticPool<I, W, R>
    where
        I: Send + Debug + 'static,
        W: Worker<I> + Send + Sync + 'static,
        W::Output: Send + Debug,
        R: Reducer<W::Output> + Send + 'static,
        R::Output: Send + Sync + Debug,
    {
        StaticPool::create(
            self.worker.value(),
            self.reducer.value(),
            self.concurrency_limit.value(),
        )
    }
}
