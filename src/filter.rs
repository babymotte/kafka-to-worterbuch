use serde_json::Value;
use serde_json_path::{JsonPath, ParseError};

pub fn matches(value: Value, filter: &str) -> bool {
    let array = Value::Array(vec![value]);
    let path = format!("$[?({filter})]");
    select(&array, &path).unwrap_or(false)
}

fn select<'a>(array: &Value, path: &str) -> Result<bool, ParseError> {
    let path = JsonPath::parse(path)?;
    Ok(!path.query(&array).is_empty())
}
