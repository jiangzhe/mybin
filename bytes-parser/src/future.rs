//! async read and write
use crate::error::{Error, Needed, Result};
use crate::{read_number_future, write_number_future};
use bytes::{BufMut, Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncWrite};
use futures::ready;
use std::future::Future;
use std::io::ErrorKind;
use std::pin::Pin;
use std::task::{Context, Poll};

pub trait AsyncReadBytesExt: AsyncRead {
    fn read_u8(&mut self) -> ReadU8Future<Self>
    where
        Self: Unpin,
    {
        ReadU8Future(self)
    }

    fn read_le_u16(&mut self) -> ReadLeU16Future<Self>
    where
        Self: Unpin,
    {
        ReadLeU16Future(self)
    }

    fn read_le_u24(&mut self) -> ReadLeU24Future<Self>
    where
        Self: Unpin,
    {
        ReadLeU24Future(self)
    }

    fn read_le_u32(&mut self) -> ReadLeU32Future<Self>
    where
        Self: Unpin,
    {
        ReadLeU32Future(self)
    }

    fn read_le_u64(&mut self) -> ReadLeU64Future<Self>
    where
        Self: Unpin,
    {
        ReadLeU64Future(self)
    }

    fn read_len(&mut self, n: usize) -> ReadLenFuture<Self>
    where
        Self: Unpin,
    {
        ReadLenFuture { reader: self, n }
    }

    fn read_len_out<'a, 'b>(
        &'a mut self,
        n: usize,
        out: &'b mut BytesMut,
    ) -> ReadLenOutFuture<'a, 'b, Self>
    where
        Self: Unpin,
    {
        ReadLenOutFuture {
            reader: self,
            n,
            out,
        }
    }

    fn read_until(&mut self, b: u8, inclusive: bool) -> ReadUntilFuture<Self>
    where
        Self: Unpin,
    {
        ReadUntilFuture {
            reader: self,
            b,
            inclusive,
        }
    }
}

impl<R: AsyncRead + ?Sized> AsyncReadBytesExt for R {}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadU8Future<'a, R: Unpin + ?Sized>(pub &'a mut R);

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadU8Future<'_, R> {
    type Output = Result<u8>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut b = 0;
        let mut reader = Pin::new(&mut self.0);
        loop {
            match ready!(reader.as_mut().poll_read(cx, std::slice::from_mut(&mut b))) {
                Ok(0) => {
                    return Poll::Ready(Err(Error::InputIncomplete(Bytes::new(), Needed::Size(1))))
                }
                Ok(..) => return Poll::Ready(Ok(b)),
                Err(ref e) if e.kind() == ErrorKind::Interrupted => (),
                Err(e) => return Poll::Ready(Err(Error::from(e))),
            }
        }
    }
}

read_number_future!(ReadLeU16Future, u16, 2, to_le_u16);

#[inline]
fn to_le_u16(bs: &[u8]) -> u16 {
    debug_assert_eq!(2, bs.len());
    bs[0] as u16 + ((bs[1] as u16) << 8) as u16
}

read_number_future!(ReadLeU24Future, u32, 3, to_le_u24);

#[inline]
fn to_le_u24(bs: &[u8]) -> u32 {
    debug_assert_eq!(3, bs.len());
    bs[0] as u32 + ((bs[1] as u32) << 8) + ((bs[2] as u32) << 16)
}

read_number_future!(ReadLeU32Future, u32, 4, to_le_u32);

#[inline]
fn to_le_u32(bs: &[u8]) -> u32 {
    debug_assert_eq!(4, bs.len());
    bs[0] as u32 + ((bs[1] as u32) << 8) + ((bs[2] as u32) << 16) + ((bs[3] as u32) << 24)
}

read_number_future!(ReadLeU64Future, u64, 8, to_le_u64);

#[inline]
fn to_le_u64(bs: &[u8]) -> u64 {
    debug_assert_eq!(8, bs.len());
    bs[0] as u64
        + ((bs[1] as u64) << 8)
        + ((bs[2] as u64) << 16)
        + ((bs[3] as u64) << 24)
        + ((bs[4] as u64) << 32)
        + ((bs[5] as u64) << 40)
        + ((bs[6] as u64) << 48)
        + ((bs[7] as u64) << 56)
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadLenOutFuture<'a, 'b, T: Unpin + ?Sized> {
    pub reader: &'a mut T,
    pub n: usize,
    pub out: &'b mut BytesMut,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadLenOutFuture<'_, '_, R> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { reader, n, out } = &mut *self;
        if *n == 0 {
            return Poll::Ready(Ok(()));
        }
        read_len_out_internal(reader, cx, *n, out)
    }
}

fn read_len_out_internal<'a, 'b, R: AsyncRead + Unpin + ?Sized>(
    reader: &'a mut R,
    cx: &mut Context<'_>,
    required: usize,
    out: &'b mut BytesMut,
) -> Poll<Result<()>> {
    struct Guard<'b> {
        out: &'b mut BytesMut,
        len: usize,
    }
    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            self.out.resize(self.len, 0);
        }
    }
    let len = out.len();
    let mut g = Guard { out, len };
    let mut read = 0;
    g.out.resize(g.len + required, 0);
    let mut reader = Pin::new(reader);
    loop {
        match ready!(reader.as_mut().poll_read(cx, &mut g.out[g.len..])) {
            Ok(0) => {
                return Poll::Ready(Err(Error::InputIncomplete(
                    Bytes::new(),
                    Needed::Size(required - read),
                )))
            }
            Ok(n) if read + n == required => {
                g.len += n;
                return Poll::Ready(Ok(()));
            }
            Ok(n) => {
                read += n;
                g.len += n;
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => (),
            Err(e) => return Poll::Ready(Err(Error::from(e))),
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadLenFuture<'a, T: Unpin + ?Sized> {
    reader: &'a mut T,
    n: usize,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadLenFuture<'_, R> {
    type Output = Result<Bytes>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self { reader, n } = &mut *self;
        if *n == 0 {
            return Poll::Ready(Ok(Bytes::new()));
        }
        let mut out = BytesMut::new();
        match ready!(read_len_out_internal(reader, cx, *n, &mut out)) {
            Ok(..) => Poll::Ready(Ok(out.freeze())),
            Err(Error::InputIncomplete(out, needed)) => {
                Poll::Ready(Err(Error::InputIncomplete(out, needed)))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct ReadUntilFuture<'a, T: Unpin + ?Sized> {
    reader: &'a mut T,
    b: u8,
    inclusive: bool,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for ReadUntilFuture<'_, R> {
    type Output = Result<Bytes>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            reader,
            b,
            inclusive,
        } = &mut *self;
        read_until_internal(reader, cx, *b, *inclusive)
    }
}

fn read_until_internal<'a, R: AsyncRead + Unpin + ?Sized>(
    reader: &'a mut R,
    cx: &mut Context<'_>,
    b0: u8,
    inclusive: bool,
) -> Poll<Result<Bytes>> {
    let mut reader = Pin::new(reader);
    let mut b = 0u8;
    let mut bs = BytesMut::new();
    loop {
        match ready!(reader.as_mut().poll_read(cx, std::slice::from_mut(&mut b))) {
            Ok(0) => return Poll::Ready(Err(Error::InputIncomplete(bs.freeze(), Needed::Unknown))),
            Ok(..) if b == b0 => {
                if inclusive {
                    bs.put_u8(b);
                }
                return Poll::Ready(Ok(bs.freeze()));
            }
            Ok(..) => {
                bs.put_u8(b);
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => (),
            Err(e) => return Poll::Ready(Err(Error::from(e))),
        }
    }
}

pub trait AsyncWriteBytesExt: AsyncWrite {
    fn write_u8(&mut self, n: u8) -> WriteU8Future<Self>
    where
        Self: Unpin,
    {
        WriteU8Future { writer: self, n }
    }

    fn write_le_u16(&mut self, n: u16) -> WriteU16Future<Self>
    where
        Self: Unpin,
    {
        WriteU16Future { writer: self, n }
    }

    fn write_le_u24(&mut self, n: u32) -> WriteU24Future<Self>
    where
        Self: Unpin,
    {
        WriteU24Future { writer: self, n }
    }

    fn write_le_u32(&mut self, n: u32) -> WriteU32Future<Self>
    where
        Self: Unpin,
    {
        WriteU32Future { writer: self, n }
    }

    fn write_le_u64(&mut self, n: u64) -> WriteU64Future<Self>
    where
        Self: Unpin,
    {
        WriteU64Future { writer: self, n }
    }
}

impl<W: AsyncWrite> AsyncWriteBytesExt for W {}

write_number_future!(WriteU8Future, u8, 1, u8::to_le_bytes);

write_number_future!(WriteU16Future, u16, 2, u16::to_le_bytes);

write_number_future!(WriteU24Future, u32, 3, u24_to_le_bytes);

fn u24_to_le_bytes(n: u32) -> [u8; 3] {
    [
        (n & 0xff) as u8,
        ((n >> 8) & 0xff) as u8,
        ((n >> 16) & 0xff) as u8,
    ]
}

write_number_future!(WriteU32Future, u32, 4, u32::to_le_bytes);

write_number_future!(WriteU64Future, u64, 8, u64::to_le_bytes);

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    #[smol_potat::test]
    async fn test_read_u8() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        for i in 1u8..=5 {
            let b = reader.read_u8().await.unwrap();
            assert_eq!(i, b);
        }
    }

    #[smol_potat::test]
    async fn test_read_u16() {
        let bs = [1u8, 1];
        let mut reader = &bs[..];
        let n = reader.read_le_u16().await.unwrap();
        assert_eq!(256 + 1, n);
    }

    #[smol_potat::test]
    async fn test_read_u24() {
        let bs = [1u8, 1, 1, 0];
        let mut reader = &bs[..];
        let n = reader.read_le_u24().await.unwrap();
        assert_eq!(256 * 256 + 256 + 1, n);
    }

    #[smol_potat::test]
    async fn test_read_u32() {
        let bs = [1u8, 1, 0, 1];
        let mut reader = &bs[..];
        let n = reader.read_le_u32().await.unwrap();
        assert_eq!(256 * 256 * 256 + 256 + 1, n);
    }

    #[smol_potat::test]
    async fn test_read_u64() {
        let bs = [1u8, 1, 1, 1, 0, 0, 0, 1];
        let mut reader = &bs[..];
        let n = reader.read_le_u64().await.unwrap();
        assert_eq!(
            (1u64 << 56) + (1u64 << 24) + (1u64 << 16) + (1u64 << 8) + 1u64,
            n
        );
    }

    #[smol_potat::test]
    async fn test_read_incomplete_u32() {
        let bs = [1u8, 1];
        let mut reader = &bs[..];
        let rst = reader.read_le_u32().await;
        assert!(rst.is_err());
    }

    #[smol_potat::test]
    async fn test_write_u8() {
        let mut bs = Vec::new();
        let writer = &mut bs;
        writer.write_u8(0x01).await.unwrap();
        assert_eq!(vec![0x01], bs);
    }

    #[smol_potat::test]
    async fn test_write_u16() {
        let mut bs = Vec::new();
        let writer = &mut bs;
        writer.write_le_u16(0xf102).await.unwrap();
        assert_eq!(vec![0x02, 0xf1], bs);
    }

    #[smol_potat::test]
    async fn test_write_u24() {
        let mut bs = Vec::new();
        let writer = &mut bs;
        writer.write_le_u24(0x20a133).await.unwrap();
        assert_eq!(vec![0x33, 0xa1, 0x20], bs);
    }

    #[smol_potat::test]
    async fn test_write_u32() {
        let mut bs = Vec::new();
        let writer = &mut bs;
        writer.write_le_u32(0x1324ffb0).await.unwrap();
        assert_eq!(vec![0xb0, 0xff, 0x24, 0x13], bs);
    }

    #[smol_potat::test]
    async fn test_write_u64() {
        let mut bs = Vec::new();
        let writer = &mut bs;
        writer.write_le_u64(0x0001020304050607).await.unwrap();
        assert_eq!(vec![0x07, 0x06, 0x05, 0x04, 03, 02, 01, 00], bs);
    }

    #[smol_potat::test]
    async fn test_write_unavailable_u32() {
        let mut bs = [0u8; 3];
        let mut writer = smol::io::Cursor::new(&mut bs[..]);
        let rst = writer.write_le_u32(0x510b33).await;
        assert!(rst.is_err());
    }

    #[smol_potat::test]
    async fn test_read_out_0() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let mut out = BytesMut::new();
        let _ = reader.read_len_out(0, &mut out).await.unwrap();
        assert_eq!(Vec::<u8>::new(), out.freeze().bytes());
    }

    #[smol_potat::test]
    async fn test_take_out_3() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let mut out = BytesMut::new();
        let _ = reader.read_len_out(3, &mut out).await.unwrap();
        assert_eq!(vec![1u8, 2, 3], out.freeze().bytes());
    }

    #[smol_potat::test]
    async fn test_take_out_6() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let mut out = BytesMut::new();
        let rst = reader.read_len_out(6, &mut out).await;
        dbg!(&rst);
        dbg!(out);
        assert!(rst.is_err());
    }

    #[smol_potat::test]
    async fn test_take_0() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let out = reader.read_len(0).await.unwrap();
        assert_eq!(Vec::<u8>::new(), out);
    }

    #[smol_potat::test]
    async fn test_take_3() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let out = reader.read_len(3).await.unwrap();
        assert_eq!(vec![1u8, 2, 3], out);
    }

    #[smol_potat::test]
    async fn test_take_6() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let rst = reader.read_len(6).await;
        dbg!(&rst);
        assert!(rst.is_err());
    }

    #[smol_potat::test]
    async fn test_take_until_3_inclusive() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let rs = reader.read_until(3, true).await.unwrap();
        assert_eq!(vec![1u8, 2, 3], rs);
    }

    #[smol_potat::test]
    async fn test_take_until_3_exclusive() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let rs = reader.read_until(3, false).await.unwrap();
        assert_eq!(vec![1u8, 2], rs);
    }

    #[smol_potat::test]
    async fn test_take_until_6() {
        let bs = [1u8, 2, 3, 4, 5];
        let mut reader = &bs[..];
        let rs = reader.read_until(3, false).await.unwrap();
        assert_eq!(vec![1u8, 2], rs);
    }
}
