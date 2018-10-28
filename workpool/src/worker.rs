// The worker trait defines what should be done when provided with input. This
// trait is automatically implemented for any Fn(I) -> O.
pub trait Worker<I> {
    type Output;

    fn run(&self, input: I) -> Self::Output;
}

impl<T, I, O> Worker<I> for T
where
    T: Fn(I) -> O,
{
    type Output = O;

    fn run(&self, input: I) -> O {
        self(input)
    }
}
