import pandas as pd
import re
from collections import defaultdict

class EnhancedTelecomTextCleaner:
    """
    Enhanced text cleaner specifically designed for telecom/network infrastructure data
    Removes noise while preserving important technical information
    """
    
    def __init__(self):
        # Noise patterns to remove (order matters - most specific first)
        self.noise_patterns = [
            # HTML and XML tags
            (r'<[^>]+>', ''),
            
            # Multiple consecutive separators
            (r'^[-=_~*+#]{10,}.*$', ''),                    # Long separator lines
            (r'^[*]{5,}[^*]*[*]{5,}$', ''),                 # Asterisk blocks
            (r'^#+\s*$', ''),                               # Empty hash comment lines
            (r'^[^\w\d\s:./-]{5,}$', ''),                   # Pure symbol lines (5+ chars)
            
            # Multiple empty lines (replace with single empty line)
            (r'\n{4,}', '\n\n'),                           # 4+ newlines -> 2 newlines
            (r'\n\s*\n\s*\n', '\n\n'),                     # Multiple lines with spaces
            
            # Redundant spacing
            (r'[ \t]{3,}', ' '),                           # 3+ spaces/tabs -> single space
            (r'^\s+$', ''),                                 # Lines with only whitespace
            
            # Router/Switch command noise
            (r'^#$', ''),                                   # Single hash lines
            (r'^\s*quit\s*$', ''),                         # Quit commands
            (r'^\s*exit\s*$', ''),                         # Exit commands
            (r'^\s*end\s*$', ''),                          # End commands
            (r'^\s*return\s*$', ''),                       # Return commands
            
            # Debug/Log noise
            (r'^\s*\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}.*', ''),  # Timestamp lines
            (r'^\s*\[.*?\]\s*$', ''),                      # Bracketed debug info
            (r'^\s*DEBUG:.*$', ''),                        # Debug lines
            (r'^\s*INFO:.*$', ''),                         # Info lines
            (r'^\s*WARNING:.*$', ''),                      # Warning lines
            (r'^\s*ERROR:.*$', ''),                        # Error lines
        ]
        
        # Patterns to preserve (important telecom info)
        self.preserve_patterns = [
            r'#\d{8}-\d+.*ATIVAÇÃO',                       # Activation tickets
            r'PLANO\s+\d+',                                # Plan numbers
            r'BN\s*:?\s*\d+',                              # BN numbers
            r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',               # Service codes
            r'IP\s*CPE:\s*\d+\.\d+\.\d+\.\d+',            # CPE IP addresses
            r'BLOCO\s*IP:\s*\d+\.\d+\.\d+\.\d+/\d+',      # IP blocks
            r'AS\s*Cliente:\s*\d+',                        # ASN information
            r'VLAN\s*:?\s*\d+',                            # VLAN information
            r'OLT-[A-Z0-9-]+',                             # OLT equipment
            r'SLOT:\s*\d+',                                # Slot information
            r'PORT:\s*\d+',                                # Port information
            r'ONU:\s*\d+',                                 # ONU information
            r'SN:\s*[A-Za-z0-9]+',                         # Serial numbers
            r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # Domain patterns
            r'rede\s+cliente\s+v[46]:\s*[\d.:/]+',         # Network client info
            r'Rx\s+Optical\s+Power.*?=\s*[-\d.]+',         # Optical power readings
            r'GTW:\s*\d+\.\d+\.\d+\.\d+',                  # Gateway addresses
            r'interface\s+[A-Za-z0-9/]+',                  # Interface names
            r'MASC:\s*\d+\.\d+\.\d+\.\d+',                # Subnet masks
        ]
    
    def clean_text(self, text):
        """
        Advanced cleaning for telecom configuration text
        Removes noise while preserving technical information
        """
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        # Step 1: Split into lines for line-by-line processing
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            original_line = line
            line = line.strip()
            
            if not line:
                continue
            
            # Check if line contains important telecom info (preserve it)
            should_preserve = any(re.search(pattern, line, re.IGNORECASE) 
                                for pattern in self.preserve_patterns)
            
            if should_preserve:
                cleaned_lines.append(line)
                continue
            
            # Apply noise removal patterns
            is_noise = False
            for pattern, replacement in self.noise_patterns:
                if re.match(pattern, line, re.IGNORECASE | re.MULTILINE):
                    is_noise = True
                    break
            
            if is_noise:
                continue
            
            # Keep meaningful lines that aren't pure noise
            if self._is_meaningful_line(line):
                # Clean the line but keep it
                cleaned_line = self._clean_line(line)
                if cleaned_line.strip():
                    cleaned_lines.append(cleaned_line)
        
        # Step 2: Join and apply final cleanup
        cleaned_text = '\n'.join(cleaned_lines)
        
        # Apply global patterns
        for pattern, replacement in self.noise_patterns:
            if '\n' in pattern:  # Multi-line patterns
                cleaned_text = re.sub(pattern, replacement, cleaned_text, flags=re.MULTILINE)
        
        # Final cleanup
        cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)  # Max 2 consecutive newlines
        cleaned_text = cleaned_text.strip()
        
        return cleaned_text
    
    def _is_meaningful_line(self, line):
        """
        Determine if a line contains meaningful information
        """
        line_clean = line.strip()
        
        # Too short to be meaningful
        if len(line_clean) < 3:
            return False
        
        # Check for technical patterns that indicate meaningful content
        meaningful_patterns = [
            r'\d+\.\d+\.\d+\.\d+',          # IP addresses
            r'[A-Za-z0-9]+\.[A-Za-z0-9]',  # Domain-like strings
            r'[A-Z]{2,}\d+',                # Equipment codes
            r'\b\d+/\d+\b',                 # Port/slot references
            r'[A-Za-z]+:\s*[A-Za-z0-9]',   # Key-value pairs
            r'interface\s+\w+',             # Interface declarations
            r'vlan\s+\d+',                  # VLAN configurations
            r'description\s+\w+',           # Descriptions
            r'route-static',                # Routing commands
            r'ip\s+address',                # IP configurations
        ]
        
        return any(re.search(pattern, line_clean, re.IGNORECASE) 
                  for pattern in meaningful_patterns)
    
    def _clean_line(self, line):
        """
        Clean individual line while preserving structure
        """
        # Remove excessive whitespace
        line = re.sub(r'\s+', ' ', line)
        
        # Remove leading/trailing special characters that aren't part of content
        line = re.sub(r'^[^\w\d]+|[^\w\d]+$', '', line)
        
        return line.strip()
    
    def get_cleaning_stats(self, original_text, cleaned_text):
        """
        Get statistics about the cleaning process
        """
        if not isinstance(original_text, str) or not isinstance(cleaned_text, str):
            return {}
        
        original_lines = original_text.split('\n')
        cleaned_lines = cleaned_text.split('\n')
        
        return {
            'original_length': len(original_text),
            'cleaned_length': len(cleaned_text),
            'reduction_percent': round(100 * (1 - len(cleaned_text) / len(original_text)), 2) if len(original_text) > 0 else 0,
            'original_lines': len(original_lines),
            'cleaned_lines': len(cleaned_lines),
            'lines_removed': len(original_lines) - len(cleaned_lines),
        }

# For backward compatibility, also create the old class name
TelecomTextCleaner = EnhancedTelecomTextCleaner