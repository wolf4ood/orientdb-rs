use async_std::io;

#[derive(Debug)]
pub enum C3p0Error {
    IO(io::Error),
    User(Box<dyn std::error::Error + Send>),
}

impl From<io::Error> for C3p0Error {
    fn from(e: io::Error) -> Self {
        C3p0Error::IO(e)
    }
}

impl std::error::Error for C3p0Error {
    fn description(&self) -> &str {
        match *self {
            C3p0Error::IO(ref err) => err.description(),
            C3p0Error::User(ref err) => err.description(),
        }
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        match *self {
            C3p0Error::IO(ref err) => Some(err),
            C3p0Error::User(ref err) => Some(err.as_ref()),
        }
    }
}

impl std::fmt::Display for C3p0Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            C3p0Error::IO(ref err) => write!(f, "IO error: {}", err),
            C3p0Error::User(ref err) => write!(f, "User error: {}", err.description()),
        }
    }
}
