const RDB_EMPTY_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";


fn hex_to_bytes(hex: &str) -> Vec<u8> {
    // hex string is of 2 chars per byte
    assert_eq!(hex.len() % 2, 0);
    let mut result = vec![];
    let mut iter = hex.chars();
    loop {
        if let Some(c1) = iter.next() {
            let c2 = iter.next().unwrap();
            let byte = u8::from_str_radix(&format!("{}{}", c1, c2), 16).unwrap();
            result.push(byte);
        } else {
            break;
        }
    }
    result
}

pub fn empty_rdb() -> Vec<u8> {
    hex_to_bytes(RDB_EMPTY_HEX)
}