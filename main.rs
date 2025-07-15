use anyhow::Result;
use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{info, warn, debug};
use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use core_affinity;
use num_cpus;

mod packet;
mod pcap_reader;

use packet::PacketData;
use pcap_reader::PcapReader;

#[derive(Parser, Debug)]
#[command(name = "cboe-sequential-replayer")]
#[command(about = "High-performance sequential CBOE packet replayer with ordering guarantee")]
struct Args {
    /// Path to PCAP file
    #[arg(short, long)]
    file: String,

    /// Target IP address
    #[arg(short, long)]
    target: String,

    /// Rate limit in packets per second (default: replay with original PCAP timing)
    #[arg(short, long)]
    rate: Option<u64>,

    /// Loop replay indefinitely
    #[arg(long)]
    r#loop: bool,
}

/// Replay mode for packet timing
#[derive(Clone)]
enum RateLimiterMode {
    /// Fixed rate in packets per second
    FixedRate(u64),
    /// Replay with original timing from PCAP
    PcapTiming,
}

/// High-performance packet processor with per-port queues
struct PortBasedSender {
    socket: Arc<Socket>,
    target_ip: IpAddr,
    // Lock-free port queues - one queue per port
    port_queues: Arc<DashMap<u16, Arc<SegQueue<PacketData>>>>,
    // Track ports assigned to each worker thread
    worker_assignments: Arc<DashMap<usize, HashSet<u16>>>,
    running: Arc<AtomicBool>,
    stats: Stats,
    mode: RateLimiterMode,
    worker_count: usize,
}

#[derive(Default)]
struct Stats {
    sent_packets: AtomicU64,
    sent_bytes: AtomicU64,
    errors: AtomicU64,
    total_packets: AtomicU64,
}

/// High-precision rate limiter for proper packet timing
struct PortRateLimiter {
    mode: RateLimiterMode,
    interval_ns: u64,  // Used for fixed rate mode
    last_send: Instant,
    base_time: Option<(f64, Instant)>, // (PCAP base time, real time reference)
}

impl PortRateLimiter {
    fn new(mode: RateLimiterMode) -> Self {
        let interval_ns = match &mode {
            RateLimiterMode::FixedRate(rate) => {
                if *rate > 0 { 1_000_000_000 / rate } else { 1000 }
            },
            RateLimiterMode::PcapTiming => 0,
        };
        
        Self {
            mode,
            interval_ns,
            last_send: Instant::now(),
            base_time: None,
        }
    }

    fn acquire(&mut self, packet_timestamp: f64) {
        match &self.mode {
            RateLimiterMode::FixedRate(_) => {
                let now = Instant::now();
                let elapsed = now.duration_since(self.last_send);
                let required_interval = Duration::from_nanos(self.interval_ns);
                
                if elapsed < required_interval {
                    let sleep_time = required_interval - elapsed;
                    
                    if sleep_time > Duration::from_micros(10) {
                        thread::sleep(sleep_time);
                    } else {
                        // Busy wait for sub-10us precision
                        while Instant::now().duration_since(now) < sleep_time {
                            std::hint::spin_loop();
                        }
                    }
                }
                
                self.last_send = Instant::now();
            },
            RateLimiterMode::PcapTiming => {
                // Initialize base time with first packet
                if self.base_time.is_none() {
                    self.base_time = Some((packet_timestamp, Instant::now()));
                    return;
                }
                
                let (base_ts, base_instant) = self.base_time.unwrap();
                
                // Calculate target time based on PCAP timestamp delta
                let pcap_elapsed = packet_timestamp - base_ts;
                let target_time = base_instant + Duration::from_secs_f64(pcap_elapsed);
                
                // Wait until the target time
                let now = Instant::now();
                if now < target_time {
                    let sleep_time = target_time.duration_since(now);
                    
                    if sleep_time > Duration::from_micros(10) {
                        thread::sleep(sleep_time);
                    } else {
                        // Busy wait for sub-10us precision
                        while Instant::now() < target_time {
                            std::hint::spin_loop();
                        }
                    }
                }
            }
        }
    }
}

impl PortBasedSender {
    fn new(
        target_ip: IpAddr,
        rate: Option<u64>,
        worker_count: usize,
    ) -> Result<Self> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        
        // Optimize socket for performance
        if let Err(e) = socket.set_send_buffer_size(8 * 1024 * 1024) {
            warn!("Failed to set send buffer: {}", e);
        }
        
        // Always configure for multicast
        if target_ip.is_ipv4() {
            if let IpAddr::V4(addr) = target_ip {
                if addr.is_multicast() {
                    socket.set_multicast_ttl_v4(64)?;
                    
                    // Try to find the best interface for multicast
                    let interfaces = pnet::datalink::interfaces();
                    let default_iface = interfaces.iter()
                        .find(|iface| iface.is_up() && !iface.is_loopback() && !iface.ips.is_empty());
                    
                    if let Some(iface) = default_iface {
                        if let Some(ipv4) = iface.ips.iter().find(|ip| ip.is_ipv4()) {
                            if let IpAddr::V4(local_addr) = ipv4.ip() {
                                socket.set_multicast_if_v4(&local_addr)?;
                                info!("Using interface {} (IP: {}) for multicast", iface.name, local_addr);
                            }
                        }
                    }
                    
                    info!("Configured for UDP multicast to {}", addr);
                }
            }
        }
        
        let mode = match rate {
            Some(rate) => RateLimiterMode::FixedRate(rate),
            None => RateLimiterMode::PcapTiming,
        };
        
        Ok(Self {
            socket: Arc::new(socket),
            target_ip,
            port_queues: Arc::new(DashMap::new()),
            worker_assignments: Arc::new(DashMap::new()),
            running: Arc::new(AtomicBool::new(true)),
            stats: Stats::default(),
            mode,
            worker_count,
        })
    }

    // Add packet to the appropriate port queue
    fn add_packet(&self, packet: PacketData) {
        // Get or create the queue for this port
        let port = packet.dest_port;
        let queue = self.port_queues
            .entry(port)
            .or_insert_with(|| Arc::new(SegQueue::new()));
        
        // Push packet to the queue (lock-free operation)
        queue.push(packet);
        
        // Warn if any queue gets too large
        if queue.len() > 100000 {
            warn!("Queue size large for port {}: approx {} packets", port, queue.len());
        }
    }

    // Distribute ports evenly among worker threads
    fn distribute_ports(&self, detected_ports: &HashSet<u16>) {
        info!("Distributing {} ports across {} worker threads", detected_ports.len(), self.worker_count);
        
        // Initialize all workers with empty port sets
        for worker_id in 0..self.worker_count {
            self.worker_assignments.insert(worker_id, HashSet::new());
        }
        
        // Distribute ports round-robin to workers
        for (i, port) in detected_ports.iter().enumerate() {
            let worker_id = i % self.worker_count;
            if let Some(mut ports) = self.worker_assignments.get_mut(&worker_id) {
                ports.insert(*port);
            }
        }
        
        // Log the distribution
        for worker_id in 0..self.worker_count {
            if let Some(ports) = self.worker_assignments.get(&worker_id) {
                let mut port_list: Vec<u16> = ports.iter().cloned().collect();
                port_list.sort();
                info!("Worker {}: assigned {} ports: {:?}", worker_id, ports.len(), port_list);
            }
        }
    }

    // Start worker threads with cross-platform CPU affinity
    fn start_worker_threads(self: Arc<Self>) -> Vec<thread::JoinHandle<()>> {
        let mut handles = Vec::with_capacity(self.worker_count);
        
        // Try to get available CPU cores (cross-platform approach)
        let core_ids = match core_affinity::get_core_ids() {
            Some(ids) => {
                info!("Detected {} CPU cores for affinity", ids.len());
                Some(Arc::new(ids))
            },
            None => {
                info!("CPU affinity not supported on this platform, continuing without core pinning");
                None
            }
        };
        
        for worker_id in 0..self.worker_count {
            let sender = self.clone();
            let core_ids_clone = core_ids.clone();
            
            let handle = thread::spawn(move || {
                // Set CPU affinity if supported
                if let Some(cores) = &core_ids_clone {
                    if !cores.is_empty() {
                        let core = cores[worker_id % cores.len()];
                        if core_affinity::set_for_current(core) {
                            debug!("Worker {} pinned to CPU core {}", worker_id, core.id);
                        } else {
                            debug!("Failed to set CPU affinity for worker {}", worker_id);
                        }
                    }
                }
                
                // Get assigned ports for this worker
                let assigned_ports = match sender.worker_assignments.get(&worker_id) {
                    Some(ports) => ports.iter().cloned().collect::<HashSet<_>>(),
                    None => {
                        warn!("No ports assigned to worker {}", worker_id);
                        HashSet::new()
                    }
                };
                
                if assigned_ports.is_empty() {
                    info!("Worker {} has no assigned ports, exiting", worker_id);
                    return;
                }
                
                // Create per-port rate limiters to maintain ordering within each port
                let mut port_rate_limiters: HashMap<u16, PortRateLimiter> = HashMap::new();
                for &port in &assigned_ports {
                    port_rate_limiters.insert(port, PortRateLimiter::new(sender.mode.clone()));
                }
                
                // Process packets from all assigned ports
                while sender.running.load(Ordering::Relaxed) {
                    let mut processed = false;
                    
                    // Check each assigned port for packets
                    for &port in &assigned_ports {
                        if let Some(queue) = sender.port_queues.get(&port) {
                            // Try to get a packet from this port's queue (non-blocking)
                            if let Some(packet) = queue.pop() {
                                processed = true;
                                
                                // Apply rate limiting for this port
                                if let Some(rate_limiter) = port_rate_limiters.get_mut(&port) {
                                    rate_limiter.acquire(packet.timestamp);
                                }
                                
                                // Send packet to original destination port (no mapping)
                                let dest_port = packet.dest_port;
                                let dest_addr = SocketAddr::new(sender.target_ip, dest_port);
                                
                                // Send packet (sequential order for each port is guaranteed)
                                match sender.socket.send_to(&packet.payload, &dest_addr.into()) {
                                    Ok(bytes_sent) => {
                                        sender.stats.sent_packets.fetch_add(1, Ordering::Relaxed);
                                        sender.stats.sent_bytes.fetch_add(bytes_sent as u64, Ordering::Relaxed);
                                    }
                                    Err(e) => {
                                        sender.stats.errors.fetch_add(1, Ordering::Relaxed);
                                        warn!("Worker {} failed to send to {}:{} - {}", 
                                            worker_id, sender.target_ip, dest_port, e);
                                    }
                                }
                            }
                        }
                    }
                    
                    // If no packets were processed, sleep briefly to avoid busy-waiting
                    if !processed {
                        thread::sleep(Duration::from_micros(100));
                    }
                }
                
                debug!("Worker {} shutting down", worker_id);
            });
            
            handles.push(handle);
        }
        
        info!("Started {} worker threads", handles.len());
        handles
    }

    fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    fn set_total_packets(&self, total: u64) {
        self.stats.total_packets.store(total, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (u64, u64, u64, usize, u64) {
        let queue_size: usize = self.port_queues
            .iter()
            .map(|entry| entry.len())
            .sum();
        
        (
            self.stats.sent_packets.load(Ordering::Relaxed),
            self.stats.sent_bytes.load(Ordering::Relaxed),
            self.stats.errors.load(Ordering::Relaxed),
            queue_size,
            self.stats.total_packets.load(Ordering::Relaxed),
        )
    }

    fn get_port_queue_sizes(&self) -> HashMap<u16, usize> {
        let mut sizes = HashMap::new();
        for entry in self.port_queues.iter() {
            sizes.insert(*entry.key(), entry.len());
        }
        sizes
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging with smart defaults
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();
    
    info!("Starting CBOE PCAP Replayer (High-Performance)");
    info!("Target IP: {}", args.target);
    info!("PCAP file: {}", args.file);
    
    // Parse target IP
    let target_ip: IpAddr = args.target.parse()?;
    
    // Analyze PCAP file first to get the ports and packet count
    let reader = PcapReader::new(&args.file)?;
    let pcap_analysis = reader.discover_ports()?;
    
    // Extract ports from analysis
    let ports: HashSet<u16> = pcap_analysis.ports;
    let ports_vec: Vec<u16> = ports.iter().cloned().collect();
    
    // Log PCAP file analysis summary
    info!("PCAP analysis summary:");
    info!("  Total packets: {}", pcap_analysis.total_packets);
    info!("  UDP packets: {}", pcap_analysis.udp_packets);
    info!("  Duration: {:.3} seconds", pcap_analysis.duration_seconds);
    info!("  Original rate: {} packets/second", pcap_analysis.calculated_rate);
    info!("  Detected {} unique ports: {:?}", ports.len(), ports_vec);
    
    // Determine optimal number of worker threads based on detected ports
    let cpu_count = num_cpus::get();
    let worker_count = if ports.is_empty() {
        info!("No ports detected in PCAP, defaulting to {} CPU cores", cpu_count);
        cpu_count
    } else {
        // Use one thread per port, up to the number of CPU cores
        let optimal_count = ports.len().min(cpu_count);
        info!("Auto-configuring {} worker threads for {} detected ports", 
             optimal_count, ports.len());
        optimal_count
    };
    
    // Show rate info
    let rate_info = match args.rate {
        Some(rate) => format!("{} pps (user-specified)", rate),
        None => "original PCAP timing".to_string(),
    };
    info!("Using rate: {}", rate_info);

    // Create optimized sender with per-port queues
    let sender = Arc::new(PortBasedSender::new(
        target_ip,
        args.rate,
        worker_count,
    )?);

    // Set total packet count for progress tracking
    sender.set_total_packets(pcap_analysis.udp_packets);

    // Distribute ports to workers for optimal load balancing
    sender.distribute_ports(&ports);

    // Start worker threads with CPU affinity
    let worker_handles = sender.clone().start_worker_threads();

    // Statistics reporting
    let stats_handle = {
        let sender = sender.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            let mut last_packets = 0u64;
            let mut last_time = Instant::now();
            let total_packets = sender.stats.total_packets.load(Ordering::Relaxed);

            loop {
                interval.tick().await;
                
                let (packets, bytes, errors, queue_len, _) = sender.get_stats();
                let now = Instant::now();
                let elapsed = now.duration_since(last_time).as_secs_f64();
                let pps = (packets - last_packets) as f64 / elapsed;
                
                // Calculate progress percentage
                let progress = if total_packets > 0 {
                    (packets as f64 / total_packets as f64 * 100.0).min(100.0)
                } else {
                    0.0
                };
                
                info!(
                    "Progress: {:.1}% ({}/{}) - Rate: {} pps - Sent: {} bytes - Errors: {} - Queue: {}",
                    progress, packets, total_packets, pps as u64, bytes, errors, queue_len
                );
                
                // Periodically show queue sizes for ports with significant backlogs
                let port_sizes = sender.get_port_queue_sizes();
                let mut backlogs = Vec::new();
                for (port, size) in port_sizes {
                    if size > 10000 {
                        backlogs.push((port, size));
                    }
                }
                
                if !backlogs.is_empty() {
                    warn!("Port queue backlogs: {:?}", backlogs);
                }
                
                last_packets = packets;
                last_time = now;
            }
        })
    };

    // Setup graceful shutdown
    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Received Ctrl+C, shutting down...");
        running_clone.store(false, Ordering::Relaxed);
    });

    // Main packet reading and distribution loop
    let start_time = Instant::now();
    let mut replay_count = 0;
    
    loop {
        replay_count += 1;
        let mut reader = PcapReader::new(&args.file)?;
        let mut packet_count = 0;

        info!("Starting replay #{} with per-port queues...", replay_count);
        
        while let Some(packet) = reader.next_packet()? {
            if !running.load(Ordering::Relaxed) {
                break;
            }

            sender.add_packet(packet);
            packet_count += 1;
            
            // Progress reporting
            if packet_count % 100000 == 0 {
                debug!("Queued {} packets", packet_count);
            }
            
            // Brief yield to prevent overwhelming the system
            if packet_count % 10000 == 0 {
                tokio::task::yield_now().await;
            }
        }

        info!("Queued {} packets in replay #{}", packet_count, replay_count);

        if !args.r#loop {
            break;
        }

        if !running.load(Ordering::Relaxed) {
            break;
        }
    }

    // Wait for remaining packets to be sent
    info!("Waiting for remaining packets to be sent...");
    while sender.get_stats().3 > 0 && running.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Shutdown
    sender.stop();
    stats_handle.abort();
    
    // Wait for all worker threads to finish
    for (i, handle) in worker_handles.into_iter().enumerate() {
        if let Err(e) = handle.join() {
            warn!("Error joining worker thread {}: {:?}", i, e);
        }
    }

    // Calculate and display final stats
    let (packets, bytes, errors, _, total) = sender.get_stats();
    let duration = start_time.elapsed();
    let avg_rate = if duration.as_secs() > 0 {
        packets / duration.as_secs()
    } else {
        packets
    };
    
    // Print summary
    info!("======= REPLAY SUMMARY =======");
    info!("Total time: {:.2} seconds", duration.as_secs_f64());
    info!("Packets processed: {}/{} ({:.2}%)", 
          packets, total, 
          if total > 0 { (packets as f64 / total as f64) * 100.0 } else { 0.0 });
    info!("Bytes sent: {} ({:.2} MB)", 
          bytes, bytes as f64 / (1024.0 * 1024.0));
    info!("Errors: {}", errors);
    info!("Average rate: {} packets/second", avg_rate);
    info!("Replay count: {}", replay_count);
    info!("==============================");
    
    info!("CBOE PCAP replayer shutdown complete");

    Ok(())
}