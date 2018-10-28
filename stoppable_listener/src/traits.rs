use std::io;

pub trait Listener: mio::event::Evented + private::Sealed {
    type Stream;
    fn accept(&self) -> io::Result<Self::Stream>;
}

pub trait IntoListener: private::Sealed {
    type Listener: Listener;
    fn into_listener(self) -> io::Result<Self::Listener>;
}

mod private {
    pub trait Sealed {}
}

pub mod tcp {
    use super::{private, IntoListener, Listener};
    use std::io;
    use std::net::{SocketAddr, TcpStream};

    impl private::Sealed for mio::net::TcpListener {}

    impl Listener for mio::net::TcpListener {
        type Stream = (TcpStream, SocketAddr);
        fn accept(&self) -> io::Result<Self::Stream> {
            self.accept_std()
        }
    }

    impl private::Sealed for std::net::TcpListener {}

    impl IntoListener for std::net::TcpListener {
        type Listener = mio::net::TcpListener;
        fn into_listener(self) -> io::Result<Self::Listener> {
            Ok(mio::net::TcpListener::from_std(self)?)
        }
    }
}

#[cfg(unix)]
pub mod unix {
    use super::{private, IntoListener, Listener};
    use std::io;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::net as unix_net;

    pub struct UnixListener(unix_net::UnixListener);

    impl private::Sealed for UnixListener {}

    impl Listener for UnixListener {
        type Stream = (unix_net::UnixStream, unix_net::SocketAddr);
        fn accept(&self) -> io::Result<Self::Stream> {
            self.0.accept().and_then(|(stream, addr)| {
                stream.set_nonblocking(false)?;
                Ok((stream, addr))
            })
        }
    }

    impl private::Sealed for unix_net::UnixListener {}

    impl IntoListener for unix_net::UnixListener {
        type Listener = UnixListener;
        fn into_listener(self) -> io::Result<Self::Listener> {
            self.set_nonblocking(true)?;
            Ok(UnixListener(self))
        }
    }

    impl mio::Evented for UnixListener {
        fn register(
            &self,
            poll: &mio::Poll,
            token: mio::Token,
            events: mio::Ready,
            opts: mio::PollOpt,
        ) -> io::Result<()> {
            mio::unix::EventedFd(&self.0.as_raw_fd()).register(poll, token, events, opts)
        }

        fn reregister(
            &self,
            poll: &mio::Poll,
            token: mio::Token,
            events: mio::Ready,
            opts: mio::PollOpt,
        ) -> io::Result<()> {
            mio::unix::EventedFd(&self.0.as_raw_fd()).reregister(poll, token, events, opts)
        }

        fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
            mio::unix::EventedFd(&self.0.as_raw_fd()).deregister(poll)
        }
    }
}
