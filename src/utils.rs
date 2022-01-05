pub fn md5sum(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(bytes);
    let result = hasher.finalize();
    format!("{:0x}", result)
}

pub fn taos_hash_id(bytes: &[u8]) -> i32 {
    bytes.iter().fold(0, |mut sum, byte| {
        sum += (byte.wrapping_sub(b'0')) as i32;
        sum
    })
}
#[test]
fn test_md5hash() {
    use hex_literal::hex;
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(b"abc");
    let result = hasher.finalize();
    assert_eq!(result[..], hex!("900150983cd24fb0d6963f7d28e17f72"));
    let s = format!("{:0x}", &result);
    assert_eq!(&s, "900150983cd24fb0d6963f7d28e17f72");

    assert_eq!(md5sum(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
}

pub fn tag_value_escape(value: &str) -> String {
    value.replace("\"", "\\\"")
}