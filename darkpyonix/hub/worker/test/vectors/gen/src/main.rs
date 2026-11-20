//! Prints the hub test vectors as JSON (see Cargo.toml).
//!
//! The key is the fixed seed 00..1f. The link signature is what a device posts to
//! `POST /device-links/{link_id}/token`; the packet is what iroh's `PkarrPublisher` puts to
//! `PUT /pkarr/<z32>`.

use std::net::SocketAddr;

use iroh_base::{RelayUrl, SecretKey};
use iroh_dns::endpoint_info::EndpointInfo;

const LINK_ID: &str = "l_000102030405060708090a0b0c0d0e0f";
const CHALLENGE: &str = "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff000102030405060708090a0b0c0d0e0f";
const RELAY: &str = "https://relay.example.test/";
const ADDRS: [&str; 2] = ["203.0.113.7:4433", "[2001:db8::7]:4433"];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn main() {
    let seed: [u8; 32] = core::array::from_fn(|i| i as u8);
    let key = SecretKey::from_bytes(&seed);
    let public = key.public();

    let message = format!("darkpyonix-hub/v2/link\n{LINK_ID}\n{CHALLENGE}");
    let signature = key.sign(message.as_bytes());

    let relay: RelayUrl = RELAY.parse().expect("relay url");
    let addrs: Vec<SocketAddr> = ADDRS.iter().map(|a| a.parse().expect("socket address")).collect();
    let info = EndpointInfo::new(public).with_relay_url(relay).with_ip_addrs(addrs);
    let packet = info.to_pkarr_signed_packet(&key, 30).expect("signed packet");

    println!("{{");
    println!("  \"generator\": \"iroh-base {} and iroh-dns {} (test/vectors/gen)\",", "1.5.0", "1.5.0");
    println!("  \"seed\": \"{}\",", hex(&seed));
    println!("  \"endpoint_id\": \"{}\",", hex(public.as_bytes()));
    println!("  \"z32\": \"{}\",", public.to_z32());
    println!("  \"link\": {{");
    println!("    \"link_id\": \"{LINK_ID}\",");
    println!("    \"challenge\": \"{CHALLENGE}\",");
    println!("    \"signature\": \"{}\"", hex(&signature.to_bytes()));
    println!("  }},");
    println!("  \"pkarr\": {{");
    println!("    \"timestamp_us\": {},", packet.timestamp().as_micros());
    println!("    \"relay\": \"{RELAY}\",");
    println!("    \"addrs\": [\"{}\", \"{}\"],", ADDRS[0], ADDRS[1]);
    println!("    \"payload\": \"{}\"", hex(&packet.to_relay_payload()));
    println!("  }}");
    println!("}}");
}
