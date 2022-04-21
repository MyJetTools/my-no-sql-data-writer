use std::{str::Utf8Error, string::FromUtf8Error};

pub enum DataWriterError {
    TableAlreadyExists(String),
    TableNotFound(String),
    RecordAlreadyExists(String),
    RecordIsChanged(String),
    RequieredEntityFieldIsMissing(String),
    ServerCouldNotParseJson(String),
    FromUtf8Error(FromUtf8Error),
    Utf8Error(Utf8Error),
    Error(String),

    HyperError(hyper::Error),
}

impl From<hyper::Error> for DataWriterError {
    fn from(src: hyper::Error) -> Self {
        Self::HyperError(src)
    }
}

impl From<FromUtf8Error> for DataWriterError {
    fn from(src: FromUtf8Error) -> Self {
        Self::FromUtf8Error(src)
    }
}

impl From<Utf8Error> for DataWriterError {
    fn from(src: Utf8Error) -> Self {
        Self::Utf8Error(src)
    }
}
