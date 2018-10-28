extern crate mio;
use std::io;

mod traits;

#[derive(Debug)]
pub struct Listener<T>
where
    T: traits::IntoListener,
{
    listener: T::Listener,
    poll: mio::Poll,
    stop_registration: mio::Registration,
    events: mio::Events,
}

const BREAK: mio::Token = mio::Token(0);
const LISTENER: mio::Token = mio::Token(1);

impl<T> Listener<T>
where
    T: traits::IntoListener,
{
    pub fn from_listener(listener: T) -> io::Result<(StopHandle, Self)>
    where
        T: traits::IntoListener,
    {
        let listener = listener.into_listener()?;
        let poll = mio::Poll::new()?;
        let (stop_registration, readiness) = mio::Registration::new2();
        poll.register(
            &stop_registration,
            BREAK,
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        )?;
        poll.register(
            &listener,
            LISTENER,
            mio::Ready::readable(),
            mio::PollOpt::edge(),
        )?;
        let events = mio::Events::with_capacity(2);
        let stoppable_listener = Listener {
            listener,
            poll,
            stop_registration,
            events,
        };
        Ok((StopHandle(readiness), stoppable_listener))
    }
}

impl<'a, T> Iterator for &'a mut Listener<T>
where
    T: traits::IntoListener,
{
    type Item = io::Result<<T::Listener as traits::Listener>::Stream>;
    fn next(&mut self) -> Option<Self::Item> {
        use traits::Listener;
        loop {
            if self.events.iter().any(|event| event.token() == BREAK) {
                return None;
            }
            match self.listener.accept() {
                Ok(x) => return Some(Ok(x)),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Some(Err(e)),
            }
            let poll_result = self.poll.poll(&mut self.events, None);
            if let Err(e) = poll_result {
                return Some(Err(e));
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct StopHandle(mio::SetReadiness);

impl StopHandle {
    pub fn stop_listening(&self) -> io::Result<()> {
        self.0.set_readiness(mio::Ready::readable())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tcp() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("[::1]:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop_handle, mut listener) = Listener::from_listener(listener).unwrap();
        let listen_thread = std::thread::spawn(move || loop {
            match (&mut listener).next() {
                Some(Ok((mut socket, _))) => write!(socket, "hello world").unwrap(),
                Some(Err(e)) => panic!("{}", e),
                None => return,
            }
        });
        for _ in 0..10 {
            let stream = std::net::TcpStream::connect(addr).unwrap();
            for _ in stream.bytes() {}
        }
        stop_handle.stop_listening().unwrap();
        let thread_result = listen_thread.join().unwrap();
        assert_eq!(thread_result, ());
    }

    #[cfg(unix)]
    #[test]
    fn uds() {
        use std::io::{Read, Write};
        let mut attempt = 0;
        let listener = loop {
            attempt += 1;
            if attempt > 3 {
                panic!("unable to bind unix socket to temp file");
            }

            let tmp_filename = tempfile::NamedTempFile::new().unwrap().path().to_path_buf();
            if let Ok(listener) = std::os::unix::net::UnixListener::bind(tmp_filename) {
                break listener;
            }
        };
        let socket_path = listener
            .local_addr()
            .unwrap()
            .as_pathname()
            .unwrap()
            .to_path_buf();
        let (stop_handle, mut listener) = Listener::from_listener(listener).unwrap();
        let listen_thread = std::thread::spawn(move || loop {
            match (&mut listener).next() {
                Some(Ok((mut socket, _))) => write!(socket, "hello world").unwrap(),
                Some(Err(e)) => panic!("{}", e),
                None => return,
            }
        });
        for _ in 0..10 {
            let stream = std::os::unix::net::UnixStream::connect(&socket_path).unwrap();
            for _ in stream.bytes() {}
        }
        stop_handle.stop_listening().unwrap();
        let thread_result = listen_thread.join().unwrap();
        assert_eq!(thread_result, ());
    }
}
