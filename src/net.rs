use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use nix;
use serde;
use serde_json;
use std::fmt::Debug;
use std::io;
use std::os::unix::io::AsRawFd;
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
use {ClientReq, Error, Req, RunSpec, ServerReq};

pub(crate) fn write_resp<W, RESP>(mut w: W, resp: RESP) -> Result<(), io::Error>
where
    W: io::Write,
    RESP: serde::Serialize + Debug,
{
    debug!("Writing response: {:?}", &resp);
    let buf = serde_json::to_vec(&resp)?;
    w.write_u32::<BigEndian>(buf.len() as u32)?;
    w.write_all(&buf)?;
    Ok(())
}

pub(crate) fn read_resp<R, RESP>(r: R) -> Result<RESP, io::Error>
where
    R: io::Read,
    RESP: serde::de::DeserializeOwned + Debug,
{
    let resp = real_read_resp(r);
    debug!("Read response: {:?}", resp);
    resp
}

fn real_read_resp<R, RESP>(mut r: R) -> Result<RESP, io::Error>
where
    R: io::Read,
    RESP: serde::de::DeserializeOwned + Debug,
{
    let len = r.read_u32::<BigEndian>()?;
    Ok(serde_json::from_reader(r.take(u64::from(len)))?)
}

pub(crate) fn write_req(socket: &mut UnixStream, req: &ClientReq) -> Result<(), Error> {
    use nix::sys::socket::{sendmsg, ControlMessage, MsgFlags};
    use nix::sys::uio::IoVec;
    debug!("Writing request: {:?}", req);

    let json_buf = serde_json::to_vec(req)?;
    let mut length_buf = [0; 4];
    BigEndian::write_u32(&mut length_buf, json_buf.len() as u32);

    let mut cmsg = [ControlMessage::ScmRights(&[]); 1];
    let cmsgs = if let Req::Run(RunSpec { ref fds, .. }) = *req {
        cmsg[0] = ControlMessage::ScmRights(fds);
        &cmsg[0..1]
    } else {
        &cmsg[0..0]
    };
    let n = sendmsg(
        socket.as_raw_fd(),
        &[IoVec::from_slice(&length_buf), IoVec::from_slice(&json_buf)],
        cmsgs,
        MsgFlags::empty(),
        None,
    )?;

    if n != json_buf.len() + length_buf.len() {
        return Err(Error::new("failed to write the entire request"));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum ClosedOr<T> {
    Closed,
    Other(T),
}

pub(crate) fn read_req(socket: &UnixStream) -> Result<ClosedOr<ServerReq>, Error> {
    let req = real_read_req(socket);
    debug!("Read request: {:?}", req);
    req
}

enum SliceOrVec<'a, T: 'a> {
    Slice(&'a mut [T]),
    Vec(Vec<T>),
}

impl<'a, T> std::ops::Deref for SliceOrVec<'a, T> {
    type Target = [T];
    fn deref(&self) -> &Self::Target {
        match self {
            SliceOrVec::Slice(slice) => slice,
            SliceOrVec::Vec(vec) => vec,
        }
    }
}

impl<'a, T> std::ops::DerefMut for SliceOrVec<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            SliceOrVec::Slice(slice) => slice,
            SliceOrVec::Vec(vec) => vec,
        }
    }
}

fn real_read_req(socket: &UnixStream) -> Result<ClosedOr<ServerReq>, Error> {
    let mut msg_buf = [0; 4096];
    let mut fds: Vec<RawFd> = Vec::new();
    let res = read_exact(socket, &mut msg_buf[..4], &mut fds)?;
    if let ClosedOr::Closed = res {
        return Ok(ClosedOr::Closed);
    }
    let msg_size = BigEndian::read_u32(&msg_buf) as usize;
    let mut buf: SliceOrVec<u8> = if msg_size < msg_buf.len() {
        SliceOrVec::Slice(&mut msg_buf[..msg_size])
    } else {
        SliceOrVec::Vec(vec![0; msg_size])
    };
    read_exact(socket, &mut buf, &mut fds)?;
    let req: ClientReq = serde_json::from_slice(&buf)?;
    Ok(ClosedOr::Other(req.with_linked_fds(fds)?))
}

fn read_exact(
    socket: &UnixStream,
    mut buf: &mut [u8],
    fds: &mut Vec<RawFd>,
) -> Result<ClosedOr<()>, Error> {
    while !buf.is_empty() {
        match read(socket, buf, fds) {
            Ok(0) => return Ok(ClosedOr::Closed),
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
            }
            Err(nix::Error::Sys(nix::errno::Errno::EINTR)) => {}
            Err(e) => return Err(Error::new(format!("{}", e))),
        }
    }
    Ok(ClosedOr::Other(()))
}

fn read(socket: &UnixStream, buf: &mut [u8], fds: &mut Vec<RawFd>) -> Result<usize, nix::Error> {
    use nix::sys::socket::{recvmsg, CmsgSpace, ControlMessage, MsgFlags};
    use nix::sys::uio::IoVec;

    let mut cmsg: CmsgSpace<([RawFd; 1024])> = CmsgSpace::new();
    let recv = recvmsg(
        socket.as_raw_fd(),
        &[IoVec::from_mut_slice(buf)],
        Some(&mut cmsg),
        MsgFlags::empty(),
    )?;

    let received_fds = recv
        .cmsgs()
        .filter_map(|x| {
            if let ControlMessage::ScmRights(fds) = x {
                Some(fds)
            } else {
                None
            }
        }).flat_map(|fds| fds);
    let mut fd_count = fds.len();
    fds.extend(received_fds);
    fd_count = fds.len() - fd_count;
    debug!("read {} bytes and {} fds", recv.bytes, fd_count);
    Ok(recv.bytes)
}
