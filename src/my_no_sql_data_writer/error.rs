pub enum DataWriterError {
    EntityExists,
    Error(String),
    HyperError(hyper::Error),
}

impl From<hyper::Error> for DataWriterError {
    fn from(src: hyper::Error) -> Self {
        Self::HyperError(src)
    }
}
