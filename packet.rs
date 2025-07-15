#[derive(Debug, Clone)]
pub struct PacketData {
    pub dest_port: u16,       // Destination port - used for routing
    pub payload: Vec<u8>,     // Packet payload - the actual data to send
    pub timestamp: f64,       // Timestamp from PCAP (seconds since epoch)
}

impl PacketData {
    pub fn new(dest_port: u16, payload: Vec<u8>, timestamp: f64) -> Self {
        Self {
            dest_port,
            payload,
            timestamp,
        }
    }
}