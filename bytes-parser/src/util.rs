#[macro_export]
macro_rules! read_number_future {
    ($struct_name:ident, $ty:ty, $len:expr, $fname:expr) => {
        #[must_use = "futures do nothing unless you `.await` or poll them"]
        pub struct $struct_name<'a, R: Unpin + ?Sized> {
            reader: &'a mut R,
            buf: [u8; $len],
            read: usize,
        }

        impl<'a, R> $struct_name<'a, R>
        where
            R: AsyncRead + Unpin + ?Sized,
        {
            pub fn new(reader: &'a mut R) -> Self {
                Self {
                    reader,
                    buf: [0u8; $len],
                    read: 0,
                }
            }
        }

        impl<R: AsyncRead + Unpin + ?Sized> Future for $struct_name<'_, R> {
            type Output = Result<$ty>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let Self { reader, buf, read } = &mut *self;
                // let mut reader = Pin::new(&mut self.0);
                let mut reader = Pin::new(reader);
                // let read = *read;
                // let mut read = 0;
                // let mut bs = vec![0u8; $len];
                loop {
                    match ready!(reader.as_mut().poll_read(cx, &mut buf[*read..])) {
                        Ok(0) => {
                            return Poll::Ready(Err(crate::error::Error::InputIncomplete(
                                Bytes::copy_from_slice(&buf[..]),
                                Needed::Size($len - *read),
                            )))
                        }
                        Ok(n) => {
                            *read += n;
                            if *read == $len {
                                return Poll::Ready(Ok($fname(&buf[..])));
                            }
                        }
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => (),
                        Err(e) => return Poll::Ready(Err(crate::error::Error::from(e))),
                    }
                }
            }
        }
    };
}

#[macro_export]
macro_rules! write_number_future {
    ($struct_name:ident, $ty:ty, $len:expr, $fname:expr) => {
        #[must_use = "futures do nothing unless you `.await` or poll them"]
        #[derive(Debug)]
        pub struct $struct_name<'a, W: Unpin + ?Sized> {
            pub writer: &'a mut W,
            pub n: $ty,
        }

        impl<W: AsyncWrite + Unpin + ?Sized> Future for $struct_name<'_, W> {
            type Output = Result<()>;

            fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
                let ns = $fname(self.n);
                let mut write = 0;
                let mut reader = Pin::new(&mut self.writer);
                loop {
                    match ready!(reader.as_mut().poll_write(cx, &ns)) {
                        Ok(0) => return Poll::Ready(Err(crate::error::Error::OutputUnavailable)),
                        Ok(n) => {
                            if write + n == $len {
                                return Poll::Ready(Ok(()));
                            } else {
                                write += n;
                            }
                        }
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => (),
                        Err(e) => return Poll::Ready(Err(Error::from(e))),
                    }
                }
            }
        }
    };
}
