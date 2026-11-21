//! What runs cost: the store's prices, the chain's cost as `c.cost()` returns it, and budgets.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::proto::boundary::v1 as b;
use crate::proto::layout::v1 as layout;

/// The largest amount or limit, as the core counts them.
const MAX: u64 = i64::MAX as u64;

/// What the store charges: integer nano-units of `currency` per 1,000 requests of each kind
/// (`create` is S3's PUT class, `get` its GET class, `list` its LIST class). A request costs its
/// share rounded up.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prices {
    pub currency: String,
    pub create: u64,
    pub get: u64,
    pub list: u64,
    pub delete: u64,
}

impl Prices {
    /// A shipped price table: `aws-s3-standard-us-east-1-2026-11-09`.
    pub fn named(name: &str) -> Result<Prices> {
        match name {
            // Source: https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonS3/current/us-east-1/index.json,
            // price list version 20261109154310 (published 2026-11-09), read 2026-11-15: S3
            // Standard, US East (N. Virginia), "$0.005 per 1,000 PUT, COPY, POST, or LIST
            // requests", "$0.004 per 10,000 GET and all other requests"; DELETE is free
            // (https://aws.amazon.com/s3/pricing/, "DELETE and CANCEL requests are free").
            "aws-s3-standard-us-east-1-2026-11-09" => Ok(Prices {
                currency: "USD".into(),
                create: 5_000_000,
                get: 400_000,
                list: 5_000_000,
                delete: 0,
            }),
            _ => Err(Error::ConfigError(format!("no price table named {name:?}"))),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if !is_currency(&self.currency) {
            return Err(Error::ConfigError(format!(
                "prices: the currency {:?} is not three uppercase ASCII letters",
                self.currency
            )));
        }
        let fields = [
            ("create", self.create),
            ("get", self.get),
            ("list", self.list),
            ("delete", self.delete),
        ];
        if let Some((kind, price)) = fields.iter().find(|(_, p)| *p > MAX) {
            return Err(Error::ConfigError(format!(
                "prices: {kind} is {price}, over the limit of {MAX}"
            )));
        }
        Ok(())
    }

    /// What `attempts` requests of the store operation `op` cost.
    pub(crate) fn of(&self, op: &str, attempts: u32) -> Amount {
        // § vg9yp9xsf@f6w2c
        let per_1000 = match op {
            "create" => self.create,
            "get" => self.get,
            "list" => self.list,
            "delete" => self.delete,
            other => unreachable!("no store operation {other}"),
        };
        Amount {
            currency: self.currency.clone(),
            nano: per_1000.div_ceil(1000) * u64::from(attempts),
        }
    }

    pub(crate) fn to_proto(&self) -> b::Prices {
        b::Prices {
            currency: self.currency.clone(),
            create: self.create,
            get: self.get,
            list: self.list,
            delete: self.delete,
        }
    }
}

fn is_currency(code: &str) -> bool {
    code.len() == 3 && code.bytes().all(|c| c.is_ascii_uppercase())
}

/// An integer amount of nano-units of a currency.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Amount {
    pub currency: String,
    pub nano: u64,
}

/// Store requests by kind, their total, and amounts by currency in nano-units.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Meters {
    pub create: u64,
    pub get: u64,
    pub list: u64,
    pub delete: u64,
    pub requests: u64,
    #[serde(with = "decimal")]
    pub amounts: BTreeMap<String, u64>,
}

/// The chain's cost so far: its own store requests (`store`), its children's totals
/// (`children`) and its steps' charges by step name (`steps`); the top-level fields are the
/// total of the three. In JSON, as the core gives it, amounts are decimal strings.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cost {
    pub create: u64,
    pub get: u64,
    pub list: u64,
    pub delete: u64,
    pub requests: u64,
    #[serde(with = "decimal")]
    pub amounts: BTreeMap<String, u64>,
    pub store: Meters,
    pub children: Meters,
    #[serde(with = "steps")]
    pub steps: BTreeMap<String, BTreeMap<String, u64>>,
}

/// Amounts as decimal strings, as they may pass 2^53.
mod decimal {
    // § hzh3dr6yh@sp5ww
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        amounts: &BTreeMap<String, u64>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.collect_map(amounts.iter().map(|(c, n)| (c, n.to_string())))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<String, u64>, D::Error> {
        let text = BTreeMap::<String, String>::deserialize(d)?;
        text.into_iter()
            .map(|(currency, n)| {
                let nano = n.parse().map_err(serde::de::Error::custom)?;
                Ok((currency, nano))
            })
            .collect()
    }
}

mod steps {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct Amounts(#[serde(with = "super::decimal")] BTreeMap<String, u64>);

    pub(super) fn serialize<S: Serializer>(
        steps: &BTreeMap<String, BTreeMap<String, u64>>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        s.collect_map(steps.iter().map(|(step, a)| (step, Amounts(a.clone()))))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<String, BTreeMap<String, u64>>, D::Error> {
        let steps = BTreeMap::<String, Amounts>::deserialize(d)?;
        Ok(steps.into_iter().map(|(step, a)| (step, a.0)).collect())
    }
}

/// Limits on a run's cost, by meter: `create`, `get`, `list`, `delete`, `requests`, or a
/// currency code (`USD`), each limit at most `i64::MAX`.
///
/// ```
/// let budget = substantial::Budget::from([("requests", 1000), ("USD", 2_000_000_000)]);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Budget(BTreeMap<String, u64>);

impl<K: Into<String>, const N: usize> From<[(K, u64); N]> for Budget {
    fn from(limits: [(K, u64); N]) -> Self {
        limits.into_iter().collect()
    }
}

impl<K: Into<String>> FromIterator<(K, u64)> for Budget {
    fn from_iter<I: IntoIterator<Item = (K, u64)>>(limits: I) -> Self {
        Budget(limits.into_iter().map(|(m, l)| (m.into(), l)).collect())
    }
}

impl Budget {
    /// Its limits as the core takes them, sorted by meter; `InvalidArgument` for a malformed
    /// meter or limit.
    pub(crate) fn to_proto(&self) -> Result<layout::Budget> {
        let request = ["create", "get", "list", "delete", "requests"];
        let mut limits = Vec::new();
        for (meter, limit) in &self.0 {
            if !request.contains(&meter.as_str()) && !is_currency(meter) {
                return Err(Error::InvalidArgument(format!(
                    "the budget meter {meter:?} is none of create, get, list, delete, requests \
                     or a currency of three uppercase letters"
                )));
            }
            if *limit > MAX {
                return Err(Error::InvalidArgument(format!(
                    "the budget limit of {meter} is {limit}, over the limit of {MAX}"
                )));
            }
            limits.push(layout::Limit {
                meter: meter.clone(),
                limit: *limit,
            });
        }
        Ok(layout::Budget { limits })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_named_table_and_its_shares() {
        let p = Prices::named("aws-s3-standard-us-east-1-2026-11-09").unwrap();
        // § vg9yp9xsf@pnr4n
        assert_eq!(p.of("create", 2).nano, 10_000);
        assert_eq!(p.of("get", 1).nano, 400);
        assert_eq!(p.of("delete", 3).nano, 0);
        let odd = Prices {
            get: 1001,
            ..p.clone()
        };
        assert_eq!(odd.of("get", 1).nano, 2, "a share rounds up");
        assert!(matches!(Prices::named("zero"), Err(Error::ConfigError(_))));
    }

    #[test]
    fn costs_read_the_core_json() {
        let json = r#"{"create": 1, "get": 2, "list": 3, "delete": 0, "requests": 6,
            "amounts": {"USD": "9007199254740993"},
            "store": {"create": 1, "get": 2, "list": 3, "delete": 0, "requests": 6, "amounts": {}},
            "steps": {"pay": {"USD": "5"}},
            "children": {"create": 0, "get": 0, "list": 0, "delete": 0, "requests": 0, "amounts": {}}}"#;
        // § hzh3dr6yh@cgzqa
        let cost: Cost = serde_json::from_str(json).unwrap();
        assert_eq!(cost.amounts["USD"], 9_007_199_254_740_993);
        assert_eq!(cost.steps["pay"]["USD"], 5);
        assert_eq!(cost.store.requests, 6);
        let again: Cost = serde_json::from_slice(&serde_json::to_vec(&cost).unwrap()).unwrap();
        assert_eq!(again, cost);
    }
}
