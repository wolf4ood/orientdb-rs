use crate::common::types::error::RequestError;
use crate::common::OrientCommonError;
use r2d2;
use std::error;
use std::fmt;
use std::io;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum OrientError {
    Io(io::Error),
    Request(RequestError),
    Protocol(String),
    Decoder(String),
    UTF8(FromUtf8Error),
    Pool(r2d2::Error),
    Field(String),
    Conversion(String),
}

impl From<OrientCommonError> for OrientError {
    fn from(err: OrientCommonError) -> OrientError {
        match err {
            OrientCommonError::Io(e) => OrientError::Io(e),
            OrientCommonError::Field(e) => OrientError::Field(e),
            OrientCommonError::Conversion(e) => OrientError::Conversion(e),
            OrientCommonError::Decoder(e) => OrientError::Decoder(e),
        }
    }
}

impl From<io::Error> for OrientError {
    fn from(err: io::Error) -> OrientError {
        OrientError::Io(err)
    }
}

impl From<FromUtf8Error> for OrientError {
    fn from(err: FromUtf8Error) -> OrientError {
        OrientError::UTF8(err)
    }
}

impl From<r2d2::Error> for OrientError {
    fn from(err: r2d2::Error) -> OrientError {
        OrientError::Pool(err)
    }
}

impl fmt::Display for OrientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OrientError::Io(ref err) => write!(f, "IO error: {}", err),
            OrientError::Request(ref err) => write!(f, "Request error: {}", err),
            OrientError::Protocol(ref err) => write!(f, "Protocol error: {}", err),
            OrientError::UTF8(ref err) => write!(f, "UTF8 error: {}", err),
            OrientError::Pool(ref err) => write!(f, "Pool error: {}", err),
            OrientError::Field(ref err) => write!(f, "Field error: {}", err),
            OrientError::Conversion(ref err) => write!(f, "Conversion error: {}", err),
            OrientError::Decoder(ref err) => write!(f, "Conversion error: {}", err),
        }
    }
}

impl error::Error for OrientError {
    fn description(&self) -> &str {
        match *self {
            OrientError::Io(ref err) => err.description(),
            OrientError::Protocol(ref err) => err,
            OrientError::Request(ref err) => err.description(),
            OrientError::UTF8(ref err) => err.description(),
            OrientError::Pool(ref err) => err.description(),
            OrientError::Field(ref err) => err,
            OrientError::Conversion(ref err) => err,
            OrientError::Decoder(ref err) => err,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            OrientError::Io(ref err) => Some(err),
            OrientError::Protocol(_) => None,
            OrientError::Request(ref err) => Some(err),
            OrientError::UTF8(ref err) => Some(err),
            OrientError::Pool(ref err) => Some(err),
            OrientError::Field(ref _err) => None,
            OrientError::Conversion(ref _err) => None,
            OrientError::Decoder(ref _err) => None,
        }
    }
}
