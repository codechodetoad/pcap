use anyhow::{Context, Result};
use pcap::{Capture, Offline, Error as PcapError};
use pnet::packet::ethernet::{EtherTypes, EthernetPacket};
use pnet::packet::ip::IpNextHeaderProtocols;
use pnet::packet::ipv4::Ipv4Packet;
use pnet::packet::udp::UdpPacket;
use pnet::packet::Packet;
use std::collections::HashSet;
use std::path::Path;
use tracing::{debug, info, warn};

use crate::packet::PacketData;

#[derive(Debug)]
pub struct PcapAnalysis {
    pub ports: HashSet<u16>,
    pub total_packets: u64,
    pub udp_packets: u64,
    pub calculated_rate: u64,
    pub duration_seconds: f64,
}

pub struct PcapReader {
    capture: Capture<Offline>,
    sequence: u64,
    file_path: String,
}

impl PcapReader {
    pub fn new(file_path: &str) -> Result<Self> {
        debug!("Opening PCAP file with libpcap: {}", file_path);
        
        let path = Path::new(file_path);
        let capture = Capture::from_file(path)
            .with_context(|| format!("Failed to open PCAP file: {}", file_path))?;

        debug!("PCAP file opened successfully using libpcap");
        
        Ok(Self {
            capture,
            sequence: 0,
            file_path: file_path.to_string(),
        })
    }

    /// Scan through PCAP file to discover all unique destination ports and calculate rate
    pub fn discover_ports(&self) -> Result<PcapAnalysis> {
        info!("Analyzing PCAP file: {}", self.file_path);
        
        // Create a new capture for analysis
        let path = Path::new(&self.file_path);
        let mut temp_capture = Capture::from_file(path)
            .with_context(|| format!("Failed to reopen PCAP file for analysis: {}", self.file_path))?;

        let mut ports = HashSet::new();
        let mut packet_count = 0;
        let mut udp_count = 0;
        let mut first_timestamp: Option<f64> = None;
        let mut last_timestamp: Option<f64> = None;
        let start_time = std::time::Instant::now();

        loop {
            match temp_capture.next_packet() {
                Ok(packet) => {
                    packet_count += 1;

                    // Track timing for rate calculation
                    let packet_time = packet.header.ts.tv_sec as f64 + packet.header.ts.tv_usec as f64 / 1_000_000.0;
                    if first_timestamp.is_none() {
                        first_timestamp = Some(packet_time);
                    }
                    last_timestamp = Some(packet_time);

                    // Parse Ethernet frame
                    let ethernet = match EthernetPacket::new(&packet.data) {
                        Some(eth) => eth,
                        None => continue,
                    };

                    // Only process IPv4 packets
                    if ethernet.get_ethertype() != EtherTypes::Ipv4 {
                        continue;
                    }

                    // Parse IPv4 packet
                    let ipv4 = match Ipv4Packet::new(ethernet.payload()) {
                        Some(ip) => ip,
                        None => continue,
                    };

                    // Only process UDP packets
                    if ipv4.get_next_level_protocol() != IpNextHeaderProtocols::Udp {
                        continue;
                    }

                    // Parse UDP packet
                    let udp = match UdpPacket::new(ipv4.payload()) {
                        Some(udp) => udp,
                        None => continue,
                    };

                    // Skip empty payloads
                    if udp.payload().is_empty() {
                        continue;
                    }

                    ports.insert(udp.get_destination());
                    udp_count += 1;

                    // Print progress periodically
                    if packet_count % 1_000_000 == 0 {
                        info!("Analyzed {} packets so far ({} UDP packets)...", 
                             packet_count, udp_count);
                    }
                }
                Err(PcapError::NoMorePackets) => break,
                Err(e) => {
                    warn!("Error reading packet #{}: {}", packet_count + 1, e);
                    continue;
                }
            }
        }

        let analysis_time = start_time.elapsed();
        info!("PCAP analysis completed in {:.2} seconds", analysis_time.as_secs_f64());

        // Calculate rate from actual timing
        let duration_seconds = if let (Some(first), Some(last)) = (first_timestamp, last_timestamp) {
            (last - first).max(0.001) // Minimum 1ms to avoid division by zero
        } else {
            1.0 // Default fallback
        };

        let calculated_rate = if udp_count > 0 && duration_seconds > 0.0 {
            (udp_count as f64 / duration_seconds).round() as u64
        } else {
            1000 // Fallback default
        };

        let mut sorted_ports: Vec<u16> = ports.iter().cloned().collect();
        sorted_ports.sort();

        if sorted_ports.is_empty() {
            warn!("No UDP packets with payload found in PCAP file");
            warn!("This tool only replays UDP packets. Check if your PCAP contains UDP traffic:");
            warn!("  tcpdump -r {} udp -c 10", self.file_path);
        }

        Ok(PcapAnalysis {
            ports,
            total_packets: packet_count,
            udp_packets: udp_count,
            calculated_rate,
            duration_seconds,
        })
    }

    pub fn next_packet(&mut self) -> Result<Option<PacketData>> {
        loop {
            let packet = match self.capture.next_packet() {
                Ok(packet) => packet,
                Err(PcapError::NoMorePackets) => return Ok(None),
                Err(e) => {
                    warn!("Error reading packet: {}", e);
                    continue;
                }
            };

            self.sequence += 1;
            
            // Extract packet timestamp
            let packet_timestamp = packet.header.ts.tv_sec as f64 + 
                                 packet.header.ts.tv_usec as f64 / 1_000_000.0;

            // Parse Ethernet frame
            let ethernet = match EthernetPacket::new(&packet.data) {
                Some(eth) => eth,
                None => {
                    debug!("Failed to parse Ethernet packet");
                    continue;
                }
            };

            // Only process IPv4 packets
            if ethernet.get_ethertype() != EtherTypes::Ipv4 {
                continue;
            }

            // Parse IPv4 packet
            let ipv4 = match Ipv4Packet::new(ethernet.payload()) {
                Some(ip) => ip,
                None => {
                    debug!("Failed to parse IPv4 packet");
                    continue;
                }
            };

            // Only process UDP packets
            if ipv4.get_next_level_protocol() != IpNextHeaderProtocols::Udp {
                continue;
            }

            // Parse UDP packet
            let udp = match UdpPacket::new(ipv4.payload()) {
                Some(udp) => udp,
                None => {
                    debug!("Failed to parse UDP packet");
                    continue;
                }
            };

            // Extract packet data
            let dest_port = udp.get_destination();
            let payload = udp.payload().to_vec();

            // Skip empty payloads
            if payload.is_empty() {
                continue;
            }

            let packet_data = PacketData::new(dest_port, payload, packet_timestamp);

            debug!(
                "Parsed packet {}: dest_port={} ({} bytes), timestamp={}",
                self.sequence,
                dest_port,
                packet_data.payload.len(),
                packet_timestamp
            );

            return Ok(Some(packet_data));
        }
    }
}