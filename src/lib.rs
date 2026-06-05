#![expect(
    non_snake_case,
    reason = "rvs_ functions use uppercase capability suffixes (A/B/I/M/P/S/T/U)"
)]

pub mod client;
pub mod error;
pub mod proxy;
pub mod server;
pub mod transport;

#[cfg(test)]
pub mod test_util {
    pub fn write_snapshot(name: &str, content: &str) {
        std::fs::write(format!("test_out/{name}.out"), content).expect("never: writeable");
    }
}
