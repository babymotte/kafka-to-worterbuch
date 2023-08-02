use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json_path::{JsonPath, ParseError};

pub enum Action {
    Set,
    Publish,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicFilter {
    pub name: String,
    pub set: Option<FilterType>,
    pub publish: Option<FilterType>,
    pub delete: Option<FilterType>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum FilterType {
    Unconditional(bool),
    Conditional(String),
}

impl TopicFilter {
    pub fn apply(&self, message: &Value) -> Option<Action> {
        let set_defined = match &self.set {
            Some(FilterType::Unconditional(true)) => return Some(Action::Set),
            Some(FilterType::Unconditional(false)) => true,
            Some(FilterType::Conditional(expr)) => {
                if matches(message.clone(), expr) {
                    return Some(Action::Set);
                }
                true
            }
            None => false,
        };

        let publish_defined = match &self.publish {
            Some(FilterType::Unconditional(true)) => return Some(Action::Publish),
            Some(FilterType::Unconditional(false)) => true,
            Some(FilterType::Conditional(expr)) => {
                if matches(message.clone(), expr) {
                    return Some(Action::Publish);
                }
                true
            }
            None => false,
        };

        let delete_defined = match &self.delete {
            Some(FilterType::Unconditional(true)) => return Some(Action::Delete),
            Some(FilterType::Unconditional(false)) => true,
            Some(FilterType::Conditional(expr)) => {
                if matches(message.clone(), expr) {
                    return Some(Action::Delete);
                }
                true
            }
            None => false,
        };

        if set_defined && publish_defined && delete_defined {
            None
        } else if set_defined {
            Some(Action::Publish)
        } else {
            Some(Action::Set)
        }
    }
}

fn matches(value: Value, filter: &str) -> bool {
    let array = Value::Array(vec![value]);
    let path = format!("$[?({filter})]");
    select(&array, &path).unwrap_or(false)
}

fn select<'a>(array: &Value, path: &str) -> Result<bool, ParseError> {
    let path = JsonPath::parse(path)?;
    Ok(!path.query(&array).is_empty())
}
