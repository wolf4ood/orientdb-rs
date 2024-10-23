use super::client::OrientDBClientInternal;
use crate::common::protocol::messages::request::ServerQuery;
use crate::common::types::value::{IntoOValue, OValue};
use crate::common::types::OResult;
#[cfg(feature = "sugar")]
use crate::types::result::FromResult;
use crate::OrientResult;
use futures::Stream;
use std::collections::HashMap;

#[cfg(feature = "sugar")]
use futures::StreamExt;

pub struct ServerStatement<'a> {
    client: &'a OrientDBClientInternal,
    pub(crate) user: String,
    pub(crate) password: String,
    stm: String,
    params: HashMap<String, OValue>,
    language: String,
    page_size: i32,
    mode: i8,
    named: bool,
}

impl<'a> ServerStatement<'a> {
    pub(crate) fn new(
        client: &'a OrientDBClientInternal,
        user: String,
        password: String,
        stm: String,
    ) -> ServerStatement<'a> {
        ServerStatement {
            client,
            user,
            password,
            stm,
            params: HashMap::new(),
            named: true,
            mode: 1,
            language: String::from("sql"),
            page_size: 150,
        }
    }

    pub fn positional(mut self, params: &[&dyn IntoOValue]) -> Self {
        let mut p = HashMap::new();
        for (i, elem) in params.iter().enumerate() {
            p.insert(i.to_string(), elem.into_ovalue());
        }
        self.params = p;
        self.named = false;
        self
    }
    pub fn named(mut self, params: &[(&str, &dyn IntoOValue)]) -> Self {
        self.params = params
            .iter()
            .map(|&(k, ref v)| (String::from(k), v.into_ovalue()))
            .collect();

        self.named = true;
        self
    }

    pub async fn run(self) -> OrientResult<impl Stream<Item = OrientResult<OResult>>> {
        self.client.run(self.into()).await
    }

    #[cfg(feature = "sugar")]
    pub async fn fetch_one<T>(self) -> OrientResult<Option<T>>
    where
        T: FromResult,
    {
        let mut stream = self
            .client
            .run(self.into())
            .await?
            .map(|r| r.and_then(T::from_result));

        match stream.next().await {
            Some(r) => Ok(Some(r?)),
            None => Ok(None),
        }
    }

    #[cfg(feature = "sugar")]
    pub async fn fetch<T>(self) -> OrientResult<Vec<T>>
    where
        T: FromResult,
    {
        let mut stream = self
            .client
            .run(self.into())
            .await?
            .map(|r| r.and_then(T::from_result));

        let mut results = Vec::new();

        while let Some(r) = stream.next().await {
            results.push(r?);
        }
        Ok(results)
    }

    #[cfg(feature = "sugar")]
    pub async fn stream<T>(self) -> OrientResult<impl Stream<Item = OrientResult<T>>>
    where
        T: FromResult,
    {
        Ok(self
            .client
            .run(self.into())
            .await?
            .map(|r| r.and_then(T::from_result)))
    }

    pub(crate) fn into_query(self, session_id: i32, token: Option<Vec<u8>>) -> ServerQuery {
        ServerQuery {
            session_id,
            token,
            query: self.stm,
            language: self.language,
            named: self.named,
            parameters: self.params,
            page_size: self.page_size,
            mode: self.mode,
        }
    }
}
