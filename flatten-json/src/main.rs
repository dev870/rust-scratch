extern crate flatten_json;
#[macro_use]
extern crate serde_json;

use serde_json::Value;
use json;
use flatten_json::flatten;


fn main() {
    let input = json!({
        "code": 200,
        "success": true,
        "payload": {
            "features": [
                "serde",
                "json"
            ]
        }
    });

    let mut flat_value: Value = json!({});
    flatten(&input, &mut flat_value, None, true).unwrap();
    print!("{:?}",serde_json::to_string(&flat_value).unwrap());
}
