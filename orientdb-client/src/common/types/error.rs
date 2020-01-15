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
    Generic(String),
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
            OrientError::Generic(ref err) => write!(f, "Generic error: {}", err),
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
            OrientError::Generic(ref err) => err,
        }
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        match *self {
            OrientError::Io(ref err) => Some(err),
            OrientError::Protocol(_) => None,
            OrientError::Request(ref err) => Some(err),
            OrientError::UTF8(ref err) => Some(err),
            OrientError::Pool(ref err) => Some(err),
            OrientError::Field(ref _err) => None,
            OrientError::Conversion(ref _err) => None,
            OrientError::Decoder(ref _err) => None,
            OrientError::Generic(ref _err) => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct RequestError {
    pub session_id: i32,
    pub code: i32,
    pub identifier: i32,
    pub errors: Vec<OError>,
    pub serialized: Vec<u8>,
}

#[derive(Debug)]
pub struct OError {
    pub err_type: String,
    pub err_msg: String,
}

impl OError {
    pub fn new(err_type: String, err_msg: String) -> OError {
        OError { err_type, err_msg }
    }
}

impl error::Error for RequestError {
    fn description(&self) -> &str {
        &self.errors[0].err_msg
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        None
    }
}

impl RequestError {
    pub fn new() -> RequestError {
        RequestError {
            code: -1,
            identifier: -1,
            session_id: -1,
            errors: vec![],
            serialized: vec![],
        }
    }

    pub fn new_with_code(code: i32, identifier: i32) -> RequestError {
        RequestError {
            code,
            identifier,
            session_id: -1,
            errors: vec![],
            serialized: vec![],
        }
    }

    pub fn add_error(&mut self, err: OError) {
        self.errors.push(err);
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("RequestError")
            .field("session_id", &self.session_id)
            .field("code", &self.code)
            .field("identifier", &self.identifier)
            .field("type", &self.errors[0].err_type)
            .field("message", &self.errors[0].err_msg)
            .finish()
    }
}
