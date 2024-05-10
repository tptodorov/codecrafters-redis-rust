use std::io::{self, Write};

pub struct CountingWriter<W: Write> {
    inner: W,
    count: usize,
}

impl<W> CountingWriter<W>
    where W: Write
{
    pub fn new(inner: W) -> Self {
        CountingWriter {
            inner,
            count: 0,
        }
    }

    pub fn bytes_written(&self) -> usize {
        self.count
    }
}

impl<W> Write for CountingWriter<W>
    where W: Write
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let res = self.inner.write(buf);
        if let Ok(size) = res {
            self.count += size
        }
        res
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
