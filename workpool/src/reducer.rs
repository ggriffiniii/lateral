use crossbeam_channel as channel;
use std::iter::FromIterator;
use std::thread;
use thread_util::JoinOnDrop;

/// Reducer is responsible for reducing multiple inputs into a single output.
/// This is used by the workpool to condense multiple work outputs to a single
/// output.
pub trait Reducer<I> {
    type Output;

    fn reduce(&mut self, input: I);
    fn output(self) -> Self::Output;
}

/// CollectReducer is a type that implements the reducer trait by collecting
/// items into arbitrary collections that implement FromIterator. See the
/// collect_into method on Builder.
pub struct CollectReducer<I, O> {
    sender: channel::Sender<I>,
    thread_handle: JoinOnDrop<O>,
}

impl<I, O> CollectReducer<I, O> {
    pub(crate) fn new() -> CollectReducer<I, O>
    where
        O: FromIterator<I> + Send + 'static,
        I: Send + 'static,
    {
        let (tx, rx) = channel::bounded(0);
        let thread_handle = JoinOnDrop::wrap(thread::spawn(move || {
            let rx2 = rx.clone();
            let output = O::from_iter(rx2);
            // FromIterator implementations are not required to iterate until
            // None is reached. Even in those cases the channel should continue
            // to be drained, ignoring the values, to avoid blocking the
            // producer thread.
            rx.for_each(drop);
            output
        }));
        CollectReducer {
            sender: tx,
            thread_handle,
        }
    }
}

impl<I, O> Reducer<I> for CollectReducer<I, O> {
    type Output = O;

    fn reduce(&mut self, input: I) {
        self.sender.send(input);
    }

    fn output(self) -> Self::Output {
        drop(self.sender);
        self.thread_handle.join().expect("thread panicked")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    #[test]
    fn collect_vec() {
        let mut reducer = CollectReducer::<_, Vec<_>>::new();
        reducer.reduce(100);
        reducer.reduce(200);
        assert_eq!(reducer.output(), vec![100, 200]);
    }

    #[test]
    fn collect_results_err() {
        let mut reducer = CollectReducer::<_, Result<Vec<_>, _>>::new();
        reducer.reduce(Ok(1));
        reducer.reduce(Ok(3));
        reducer.reduce(Err(4));
        reducer.reduce(Ok(4));
        assert_eq!(reducer.output(), Err(4));
    }

    #[test]
    fn collect_results_ok() {
        let mut reducer = CollectReducer::<_, Result<Vec<_>, ()>>::new();
        reducer.reduce(Ok(1));
        reducer.reduce(Ok(3));
        reducer.reduce(Ok(4));
        assert_eq!(reducer.output(), Ok(vec![1, 3, 4]));
    }

    #[test]
    fn collect_hashmap() {
        let mut reducer = CollectReducer::<_, HashMap<i32, i32>>::new();
        reducer.reduce((1, 2));
        reducer.reduce((2, 4));
        reducer.reduce((1, 8));
        assert_eq!(reducer.output(), [(1, 8), (2, 4)].iter().cloned().collect());
    }

    #[test]
    fn dropped_without_output() {
        let mut reducer = CollectReducer::<_, Vec<_>>::new();
        reducer.reduce(100);
        reducer.reduce(200);
    }
}
