use std::ffi::CString;

//rust 字符串转C风格字符串
pub fn str_to_c(val: &str) -> CString {
    CString::new(val).expect("Error converting str to CString")
}
