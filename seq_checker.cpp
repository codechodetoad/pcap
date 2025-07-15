#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <cstdint>   
#include <fstream>
#include <vector>
#include <filesystem>
#include <algorithm>

struct Packet {
    int unit;
    uint32_t seq;
    std::string packet_type;    // DATA, HEARTBEAT, ADMIN, etc.
    std::string message_types;  // The actual CBOE message types
    std::string filename;
};

class SDPLogger {
private:
    static const size_t MAX_FILE_SIZE = 100 * 1024 * 1024; // 100MB
    static const int MAX_FILES = 40;
    
    std::string base_filename;
    std::ofstream current_file;
    int current_file_index;
    size_t current_file_size;
    
    void rotate_file() {
        if (current_file.is_open()) {
            current_file.close();
        }
        
        current_file_index = (current_file_index + 1) % MAX_FILES;
        std::string filename = base_filename + "_" + std::to_string(current_file_index) + ".log";
        
        // Remove existing file if it exists (for circular rotation)
        if (std::filesystem::exists(filename)) {
            std::filesystem::remove(filename);
        }
        
        current_file.open(filename, std::ios::out | std::ios::trunc);
        current_file_size = 0;
        
        std::cout << "Rotated to new log file: " << filename << std::endl;
    }
    
public:
    SDPLogger(const std::string& base_name) 
        : base_filename(base_name), current_file_index(0), current_file_size(0) {
        std::string filename = base_filename + "_0.log";
        current_file.open(filename, std::ios::out | std::ios::trunc);
        
        if (!current_file.is_open()) {
            throw std::runtime_error("Failed to open initial log file: " + filename);
        }
        
        std::cout << "Started logging to: " << filename << std::endl;
    }
    
    ~SDPLogger() {
        if (current_file.is_open()) {
            current_file.close();
        }
    }
    
    void write(const std::string& message) {
        // Check if we need to rotate
        if (current_file_size + message.length() > MAX_FILE_SIZE) {
            rotate_file();
        }
        
        current_file << message;
        current_file.flush(); // Ensure data is written immediately
        current_file_size += message.length();
    }
    
    void write_summary(const std::string& summary) {
        // Always write summary to current file, even if it exceeds size limit
        current_file << summary;
        current_file.flush();
        current_file_size += summary.length();
    }
    
    std::string get_current_filename() const {
        return base_filename + "_" + std::to_string(current_file_index) + ".log";
    }
    
    int get_current_file_index() const {
        return current_file_index;
    }
};

bool parse_log_line(const std::string& line, Packet& packet) {
    std::istringstream ss(line);
    std::string token;
    
    // Initialize packet with invalid values
    packet.seq = 0;
    packet.unit = -1;
    packet.packet_type.clear();
    packet.message_types.clear();

    while (std::getline(ss, token, '|')) {
        size_t pos;
        if ((pos = token.find("Seq:")) != std::string::npos) {
            std::string seq_str = token.substr(pos + 4);
            // Trim whitespace
            while (!seq_str.empty() && seq_str.front() == ' ') seq_str.erase(0, 1);
            while (!seq_str.empty() && seq_str.back() == ' ') seq_str.pop_back();
            
            try {
                packet.seq = static_cast<uint32_t>(std::stoul(seq_str));
            } catch (const std::exception& e) {
                std::cerr << "Warning: Invalid sequence number '" << seq_str << "' in line: " << line.substr(0, 100) << "..." << std::endl;
                return false;
            }
        } 
        else if ((pos = token.find("Unit:")) != std::string::npos) {
            std::string unit_str = token.substr(pos + 5);
            // Trim whitespace
            while (!unit_str.empty() && unit_str.front() == ' ') unit_str.erase(0, 1);
            while (!unit_str.empty() && unit_str.back() == ' ') unit_str.pop_back();
            
            try {
                packet.unit = std::stoi(unit_str);
                // Validate unit number is reasonable
                if (packet.unit < 0 || packet.unit > 1000) {
                    std::cerr << "Warning: Suspicious unit number " << packet.unit << " in line: " << line.substr(0, 100) << "..." << std::endl;
                    return false;
                }
            } catch (const std::exception& e) {
                std::cerr << "Warning: Invalid unit number '" << unit_str << "' in line: " << line.substr(0, 100) << "..." << std::endl;
                return false;
            }
        }
        else if ((pos = token.find("Type:")) != std::string::npos) {
            // This is the packet type (DATA, HEARTBEAT, etc.)
            packet.packet_type = token.substr(pos + 5);
            // Trim whitespace
            while (!packet.packet_type.empty() && packet.packet_type.front() == ' ')
                packet.packet_type.erase(0, 1);
            while (!packet.packet_type.empty() && packet.packet_type.back() == ' ')
                packet.packet_type.pop_back();
        }
        else if ((pos = token.find("Messages:")) != std::string::npos) {
            // Extract the entire Messages section
            std::string messages_part = token.substr(pos + 9);
            // Remove leading/trailing whitespace and brackets
            while (!messages_part.empty() && (messages_part.front() == ' ' || messages_part.front() == '['))
                messages_part.erase(0, 1);
            while (!messages_part.empty() && (messages_part.back() == ' ' || messages_part.back() == ']'))
                messages_part.pop_back();
            
            packet.message_types = messages_part;
        }
    }

    return true;
}

// Structure to hold gap information for final reporting
struct SequenceGap {
    int unit;
    uint32_t start_seq;
    uint32_t end_seq;
    uint32_t gap_size;
    std::string last_seen_type;
};

void find_sequence_gaps(const std::unordered_map<int, std::unordered_set<uint32_t>>& seen_sequences,
                       const std::unordered_map<int, std::string>& unit_last_types,
                       std::vector<SequenceGap>& gaps) {
    
    for (const auto& unit_pair : seen_sequences) {
        int unit = unit_pair.first;
        const auto& sequences = unit_pair.second;
        
        if (sequences.empty()) continue;
        
        // Convert set to sorted vector for gap analysis
        std::vector<uint32_t> sorted_seqs(sequences.begin(), sequences.end());
        std::sort(sorted_seqs.begin(), sorted_seqs.end());
        
        // Find gaps between consecutive sequences
        for (size_t i = 1; i < sorted_seqs.size(); i++) {
            uint32_t current_seq = sorted_seqs[i];
            uint32_t prev_seq = sorted_seqs[i-1];
            
            if (current_seq > prev_seq + 1) {
                // Found a gap
                SequenceGap gap;
                gap.unit = unit;
                gap.start_seq = prev_seq + 1;
                gap.end_seq = current_seq - 1;
                gap.gap_size = current_seq - prev_seq - 1;
                gap.last_seen_type = unit_last_types.count(unit) ? unit_last_types.at(unit) : "Unknown";
                gaps.push_back(gap);
            }
        }
    }
}

void process_file(const std::string& filename, 
                  std::unordered_map<int, uint32_t>& highest_seen_seq,
                  std::unordered_map<int, std::unordered_set<uint32_t>>& seen_sequences,
                  std::unordered_map<int, std::string>& unit_last_types,
                  SDPLogger& logger,
                  int& duplicate_count,
                  int& out_of_order_count) {
    
    std::ifstream infile(filename);
    if (!infile) {
        std::cerr << "Warning: Could not open file " << filename << std::endl;
        return;
    }

    std::cout << "Processing file: " << filename << std::endl;

    std::string line;
    int line_count = 0;
    int processed_count = 0;
    int skipped_count = 0;
    
    while (std::getline(infile, line)) {
        line_count++;
        Packet pkt;
        pkt.filename = filename;
        
        if (!parse_log_line(line, pkt)) {
            skipped_count++;
            continue;
        }
        if (pkt.seq == 0 || pkt.unit == -1) {
            skipped_count++;
            continue; // Skip invalid packets
        }
        
        processed_count++;

        // Determine what to show for message info
        std::string msg_info;
        if (!pkt.message_types.empty()) {
            msg_info = pkt.message_types;
        } else if (!pkt.packet_type.empty()) {
            msg_info = pkt.packet_type;
        } else {
            msg_info = "Unknown";
        }

        // Store the last seen message type for this unit
        unit_last_types[pkt.unit] = msg_info;

        // Check for duplicate FIRST and skip processing if duplicate
        if (seen_sequences[pkt.unit].count(pkt.seq)) {
            std::string log_entry = "DUPLICATE,Unit=" + std::to_string(pkt.unit) + 
                                   ",Seq=" + std::to_string(pkt.seq) + 
                                   ",Type=" + msg_info + 
                                   ",File=" + filename + "\n";
            logger.write(log_entry);
            duplicate_count++;
            continue; // IMPORTANT: Skip the rest of processing for duplicates
        }

        // Check for out-of-order packets (only if we've seen packets for this unit before)
        if (highest_seen_seq.find(pkt.unit) != highest_seen_seq.end()) {
            uint32_t highest_seq = highest_seen_seq[pkt.unit];
            
            // Check if this packet is out of order (came after a later packet)
            if (pkt.seq < highest_seq) {
                int32_t difference = static_cast<int32_t>(pkt.seq) - static_cast<int32_t>(highest_seq);
                
                std::string log_entry = "OUT-OF-ORDER,Unit=" + std::to_string(pkt.unit) + 
                                       ",Seq=" + std::to_string(pkt.seq) + 
                                       ",Type=" + msg_info + 
                                       ",HighestSeq=" + std::to_string(highest_seq) + 
                                       ",Diff=" + std::to_string(difference) + 
                                       ",File=" + filename + "\n";
                logger.write(log_entry);
                out_of_order_count++;
            }
        }

        // Always update tracking for non-duplicates
        seen_sequences[pkt.unit].insert(pkt.seq);
        
        // Only update highest_seen_seq if this packet is newer
        if (highest_seen_seq.find(pkt.unit) == highest_seen_seq.end() || 
            pkt.seq > highest_seen_seq[pkt.unit]) {
            highest_seen_seq[pkt.unit] = pkt.seq;
        }
    }

    std::cout << "  Lines read: " << line_count << ", Packets processed: " << processed_count << ", Skipped: " << skipped_count << std::endl;
}

void process_stdin(std::unordered_map<int, uint32_t>& highest_seen_seq,
                   std::unordered_map<int, std::unordered_set<uint32_t>>& seen_sequences,
                   std::unordered_map<int, std::string>& unit_last_types,
                   SDPLogger& logger,
                   int& duplicate_count,
                   int& out_of_order_count) {
    
    std::cout << "Reading from stdin..." << std::endl;
    
    std::string line;
    int line_count = 0;
    int processed_count = 0;
    int skipped_count = 0;
    
    while (std::getline(std::cin, line)) {
        line_count++;
        Packet pkt;
        pkt.filename = "stdin";
        
        if (!parse_log_line(line, pkt)) {
            skipped_count++;
            continue;
        }
        if (pkt.seq == 0 || pkt.unit == -1) {
            skipped_count++;
            continue;
        }
        
        processed_count++;

        // Determine what to show for message info
        std::string msg_info;
        if (!pkt.message_types.empty()) {
            msg_info = pkt.message_types;
        } else if (!pkt.packet_type.empty()) {
            msg_info = pkt.packet_type;
        } else {
            msg_info = "Unknown";
        }

        // Store the last seen message type for this unit
        unit_last_types[pkt.unit] = msg_info;

        // Check for duplicate FIRST and skip processing if duplicate
        if (seen_sequences[pkt.unit].count(pkt.seq)) {
            std::string log_entry = "DUPLICATE,Unit=" + std::to_string(pkt.unit) + 
                                   ",Seq=" + std::to_string(pkt.seq) + 
                                   ",Type=" + msg_info + 
                                   ",File=stdin\n";
            logger.write(log_entry);
            duplicate_count++;
            continue; // IMPORTANT: Skip the rest of processing for duplicates
        }

        // Check for out-of-order packets (only if we've seen packets for this unit before)
        if (highest_seen_seq.find(pkt.unit) != highest_seen_seq.end()) {
            uint32_t highest_seq = highest_seen_seq[pkt.unit];
            
            // Check if this packet is out of order (came after a later packet)
            if (pkt.seq < highest_seq) {
                int32_t difference = static_cast<int32_t>(pkt.seq) - static_cast<int32_t>(highest_seq);
                
                std::string log_entry = "OUT-OF-ORDER,Unit=" + std::to_string(pkt.unit) + 
                                       ",Seq=" + std::to_string(pkt.seq) + 
                                       ",Type=" + msg_info + 
                                       ",HighestSeq=" + std::to_string(highest_seq) + 
                                       ",Diff=" + std::to_string(difference) + 
                                       ",File=stdin\n";
                logger.write(log_entry);
                out_of_order_count++;
            }
        }

        // Always update tracking for non-duplicates
        seen_sequences[pkt.unit].insert(pkt.seq);
        
        // Only update highest_seen_seq if this packet is newer
        if (highest_seen_seq.find(pkt.unit) == highest_seen_seq.end() || 
            pkt.seq > highest_seen_seq[pkt.unit]) {
            highest_seen_seq[pkt.unit] = pkt.seq;
        }
    }
    
    std::cout << "Lines read: " << line_count << ", Packets processed: " << processed_count << ", Skipped: " << skipped_count << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <file1> [file2] [file3] ... [fileN]\n";
        std::cerr << "  or: " << argv[0] << " --stdin  (to read from stdin)\n";
        std::cerr << "\nSDP Logging: Output will be split into up to 40 files of 100MB each\n";
        std::cerr << "Files will be named: seq_issues_0.log, seq_issues_1.log, etc.\n";
        return 1;
    }

    std::unordered_map<int, uint32_t> highest_seen_seq;
    std::unordered_map<int, std::unordered_set<uint32_t>> seen_sequences;
    std::unordered_map<int, std::string> unit_last_types; // Track last seen message type per unit
    
    // Counters for error types
    int duplicate_count = 0;
    int out_of_order_count = 0;

    try {
        SDPLogger logger("seq_issues");
        
        // Check if reading from stdin
        if (argc == 2 && std::string(argv[1]) == "--stdin") {
            process_stdin(highest_seen_seq, seen_sequences, unit_last_types, logger, 
                         duplicate_count, out_of_order_count);
        } else {
            // Process multiple files
            std::cout << "Processing " << (argc - 1) << " files..." << std::endl;
            
            for (int i = 1; i < argc; i++) {
                process_file(argv[i], highest_seen_seq, seen_sequences, unit_last_types, logger, 
                           duplicate_count, out_of_order_count);
            }
        }

        // Now find and report REAL sequence gaps (after all processing is done)
        std::vector<SequenceGap> gaps;
        find_sequence_gaps(seen_sequences, unit_last_types, gaps);
        
        // Sort gaps by unit and start sequence for better reporting
        std::sort(gaps.begin(), gaps.end(), [](const SequenceGap& a, const SequenceGap& b) {
            if (a.unit != b.unit) return a.unit < b.unit;
            return a.start_seq < b.start_seq;
        });

        // Write gap reports
        for (const auto& gap : gaps) {
            std::string log_entry = "SEQUENCE-GAP,Unit=" + std::to_string(gap.unit) + 
                                   ",MissingRange=" + std::to_string(gap.start_seq) + 
                                   "-" + std::to_string(gap.end_seq) + 
                                   ",Gap=" + std::to_string(gap.gap_size) + 
                                   ",Type=" + gap.last_seen_type + "\n";
            logger.write(log_entry);
        }

        // Enhanced summary
        std::string summary = "\n=== SUMMARY ===\n";
        summary += "Total Duplicates: " + std::to_string(duplicate_count) + "\n";
        summary += "Total Out-of-Order: " + std::to_string(out_of_order_count) + "\n";
        summary += "Total Sequence Gaps: " + std::to_string(gaps.size()) + "\n";
        summary += "Units processed: " + std::to_string(highest_seen_seq.size()) + "\n";
        summary += "Final log file: " + logger.get_current_filename() + "\n";
        summary += "Final file index: " + std::to_string(logger.get_current_file_index()) + "\n\n";
        
        summary += "=== PER-UNIT STATISTICS ===\n";
        for (const auto& pair : highest_seen_seq) {
            int unit = pair.first;
            uint32_t highest_seq = pair.second;
            size_t unique_sequences = seen_sequences[unit].size();
            
            // Calculate actual missing sequences based on real gaps
            uint32_t missing_sequences = 0;
            for (const auto& gap : gaps) {
                if (gap.unit == unit) {
                    missing_sequences += gap.gap_size;
                }
            }
            
            // Find the actual range of sequences
            auto& unit_seqs = seen_sequences[unit];
            uint32_t min_seq = *std::min_element(unit_seqs.begin(), unit_seqs.end());
            uint32_t max_seq = *std::max_element(unit_seqs.begin(), unit_seqs.end());
            
            summary += "Unit " + std::to_string(unit) + 
                      ": seq_range=" + std::to_string(min_seq) + "-" + std::to_string(max_seq) + 
                      ", unique_sequences=" + std::to_string(unique_sequences) + 
                      ", missing_sequences=" + std::to_string(missing_sequences) + "\n";
        }
        
        summary += "\n=== GAP DETAILS ===\n";
        if (gaps.empty()) {
            summary += "No sequence gaps found!\n";
        } else {
            for (const auto& gap : gaps) {
                summary += "Unit " + std::to_string(gap.unit) + 
                          ": Missing sequences " + std::to_string(gap.start_seq) + 
                          " to " + std::to_string(gap.end_seq) + 
                          " (gap size: " + std::to_string(gap.gap_size) + ")\n";
            }
        }
        summary += "\n";

        logger.write_summary(summary);
        
        std::cout << "\nAnalysis complete. Results written to rotated log files (seq_issues_*.log)" << std::endl;
        std::cout << "Total files created: up to 40 files of 100MB each" << std::endl;
        std::cout << "Current active file: " << logger.get_current_filename() << std::endl;
        std::cout << "\nSummary:" << std::endl;
        std::cout << "- Duplicates: " << duplicate_count << std::endl;
        std::cout << "- Out-of-order: " << out_of_order_count << std::endl;
        std::cout << "- Sequence gaps: " << gaps.size() << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}