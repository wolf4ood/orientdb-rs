use async_std::io;

#[derive(Debug)]
pub enum C3p0Error {
    IO(io::Error),
    User(Box<std::error::Error + Send>)
}

impl From<io::Error> for C3p0Error {
    fn from(e: io::Error) -> Self {
        C3p0Error::IO(e)
    }
}
