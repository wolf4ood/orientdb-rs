use std::error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum OrientCommonError {
    Io(io::Error),
    Field(String),
    Conversion(String),
    Decoder(String),
}

impl error::Error for OrientCommonError {
    fn description(&self) -> &str {
        match *self {
            OrientCommonError::Io(ref err) => err.description(),
            OrientCommonError::Field(ref err) => err,
            OrientCommonError::Conversion(ref err) => err,
            OrientCommonError::Decoder(ref err) => err,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            OrientCommonError::Io(ref err) => Some(err),
            OrientCommonError::Field(ref _err) => None,
            OrientCommonError::Conversion(ref _err) => None,
            OrientCommonError::Decoder(ref _err) => None,
        }
    }
}

impl fmt::Display for OrientCommonError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OrientCommonError::Io(ref err) => write!(f, "IO error: {}", err),
            OrientCommonError::Field(ref err) => write!(f, "Field error: {}", err),
            OrientCommonError::Conversion(ref err) => write!(f, "Conversion error: {}", err),
            OrientCommonError::Decoder(ref err) => write!(f, "Conversion error: {}", err),
        }
    }
}

impl From<io::Error> for OrientCommonError {
    fn from(err: io::Error) -> OrientCommonError {
        OrientCommonError::Io(err)
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

    fn cause(&self) -> Option<&error::Error> {
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
