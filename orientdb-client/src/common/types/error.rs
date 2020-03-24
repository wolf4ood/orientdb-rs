use r2d2;
use std::error;
use std::fmt;
use std::io;
use std::string::FromUtf8Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrientError {
    #[error("")]
    Io(#[from] io::Error),
    #[error("Request error: {}", .0.errors[0].err_msg)]
    Request(RequestError),
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Decoder error: {0}")]
    Decoder(String),
    #[error("UTF8 error: {0}")]
    UTF8(#[from] FromUtf8Error),
    #[error("Pool error: {0}")]
    Pool(#[from] r2d2::Error),
    #[error("Field error: {0}")]
    Field(String),
    #[error("Conversion error: {0}")]
    Conversion(String),
    #[error("Generic error: {0}")]
    Generic(String),
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
