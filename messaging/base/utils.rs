use std::{ffi::CString};

pub fn str_to_c(val: &str) -> CString {
    CString::new(val).expect("Error converting str to CString")
}