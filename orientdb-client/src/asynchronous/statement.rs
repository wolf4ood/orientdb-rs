use super::session::OSession;
use crate::common::protocol::messages::request::Query;
use crate::common::types::value::{IntoOValue, OValue};
use crate::common::types::OResult;
#[cfg(feature = "sugar")]
use crate::types::result::FromResult;
use crate::OrientResult;
use futures::Stream;
use std::collections::HashMap;

#[cfg(feature = "sugar")]
use futures::StreamExt;

pub struct Statement<'a> {
    session: &'a OSession,
    stm: String,
    params: HashMap<String, OValue>,
    language: String,
    page_size: i32,
    mode: i8,
    named: bool,
}

impl<'a> Statement<'a> {
    pub(crate) fn new(session: &'a OSession, stm: String) -> Statement<'a> {
        Statement {
            session,
            stm,
            params: HashMap::new(),
            named: true,
            mode: 1,
            language: String::from("sql"),
            page_size: 150,
        }
    }
    pub(crate) fn mode(mut self, mode: i8) -> Self {
        self.mode = mode;
        self
    }

    pub(crate) fn language(mut self, language: String) -> Self {
        self.language = language;
        self
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

    pub fn page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }
    pub async fn run(self) -> OrientResult<impl Stream<Item = OrientResult<OResult>>> {
        self.session.run(self.into()).await
    }

    #[cfg(feature = "sugar")]
    pub async fn fetch_one<T>(self) -> OrientResult<Option<T>>
    where
        T: FromResult,
    {
        let mut stream = self
            .session
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
            .session
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
            .session
            .run(self.into())
            .await?
            .map(|r| r.and_then(T::from_result)))
    }
}

impl<'a> From<Statement<'a>> for Query {
    fn from(x: Statement) -> Query {
        Query {
            session_id: x.session.session_id,
            token: x.session.token.clone(),
            query: x.stm,
            language: x.language,
            named: x.named,
            parameters: x.params,
            page_size: x.page_size,
            mode: x.mode,
        }
    }
}
