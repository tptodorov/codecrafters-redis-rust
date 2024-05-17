use std::str::FromStr;

/// finds a name in a list of strings and returns the following value if it exists.
///
///  E.g. for arguments ["--port", "123"] it returns "123"
pub fn named_option<R: FromStr>(args: &[String], name: &str) -> Result<Option<R>, R::Err> {
    let option_name = name.to_uppercase();
    args.iter()
        .position(|a| *a.to_uppercase() == option_name)
        .and_then(|i| args.get(i + 1))
        .map(|a| a.parse::<R>())
        .transpose()
}

/// finds a name in a list of strings and returns the following values
pub fn named_option_list<'a>(params: &'a [String], name: &str) -> Option<&'a [String]> {
    let option_name = name.to_uppercase();
    params.iter()
        .position(|e| e.to_string().to_uppercase() == option_name)
        .map(|i| &params[i + 1..])
}
