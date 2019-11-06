use super::live::Unsubscriber;
use super::session::OSession;
use crate::common::protocol::messages::request::LiveQuery;
use crate::common::types::value::{IntoOValue, OValue};
use crate::types::LiveResult;
use crate::OrientResult;
use futures::Stream;
use std::collections::HashMap;

pub struct LiveStatement<'a> {
    session: &'a OSession,
    stm: String,
    params: HashMap<String, OValue>,
    named: bool,
}

impl<'a> LiveStatement<'a> {
    pub(crate) fn new(session: &'a OSession, stm: String) -> LiveStatement<'a> {
        LiveStatement {
            session,
            stm,
            params: HashMap::new(),
            named: true,
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

    pub async fn run(
        self,
    ) -> OrientResult<(Unsubscriber, impl Stream<Item = OrientResult<LiveResult>>)> {
        self.session.live_run(self.into()).await
    }
}

impl<'a> From<LiveStatement<'a>> for LiveQuery {
    fn from(x: LiveStatement) -> LiveQuery {
        LiveQuery::new(
            x.session.session_id,
            x.session.token.clone(),
            x.stm,
            x.params,
            x.named,
        )
    }
}
