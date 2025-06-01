import pandas as pd
import re
from collections import defaultdict
from core.product_groups import product_group_manager

class EnhancedTelecomTextCleaner:

    def __init__(self):
        self.product_group_manager = product_group_manager
        
        # Base noise patterns (common across all product groups)
        self.base_noise_patterns = [
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
        
        # Product group specific preservation patterns
        self.group_preserve_patterns = {
            "bandalarga_broadband_fiber_plans": [
                r'#\d{8}-\d+.*ATIVAÇÃO',                   # Activation tickets
                r'PLANO\s+\d+',                            # Plan numbers
                r'BN\s*:?\s*\d+',                          # BN numbers
                r'SN:\s*[A-Za-z0-9]+',                     # Serial numbers (critical for fiber)
                r'SSID[:\s]*[A-Za-z0-9_-]+',               # WiFi SSID (critical for residential)
                r'[Pp]assword[:\s]*[A-Za-z0-9@#$%^&*()_+-]+', # WiFi passwords
                r'VLAN\s*:?\s*\d+',                        # VLAN information
                r'login\s*pppoe[:\s]*[A-Za-z0-9@._-]+',    # PPPoE login
            ],
            
            "linkdeinternet_dedicated_internet_connectivity": [
                r'IP\s*CPE:\s*\d+\.\d+\.\d+\.\d+',          # CPE IP addresses (critical)
                r'BLOCO\s*IP:\s*\d+\.\d+\.\d+\.\d+/\d+',    # IP blocks (critical)
                r'AS\s*Cliente:\s*\d+',                      # ASN information (critical)
                r'VLAN\s*:?\s*\d+',                          # VLAN information (critical)
                r'GTW:\s*\d+\.\d+\.\d+\.\d+',                # Gateway addresses
                r'interface\s+[A-Za-z0-9/]+',                # Interface names (critical)
                r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',             # Technology/Service codes
                r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # POP descriptions
            ],
            
            "linkdeinternet_gpon_semi_dedicated_connections": [
                r'AS\s*Cliente:\s*\d+',                      # ASN (critical for GPON)
                r'VLAN\s*:?\s*\d+',                          # VLAN (critical)
                r'SN:\s*[A-Za-z0-9]+',                       # Serial numbers (critical for GPON)
                r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',             # Technology ID
                r'OLT-[A-Z0-9-]+',                           # OLT equipment (semi-dedicated specific)
                r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # POP descriptions
            ],
            
            "linkdeinternet_direct_l2l_links": [
                r'AS\s*Cliente:\s*\d+',                      # ASN (critical for L2L)
                r'interface\s+[A-Za-z0-9/]+',                # Interface names (critical for L2L)
                r'VLAN\s*:?\s*\d+',                          # VLAN (critical)
                r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',             # Technology codes
                r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # POP descriptions
                r'([A-Z]{4,}\d+/Frame\d+/Slot\d+/Port\d+)', # Equipment interfaces (L2L specific)
            ],
            
            "linkdeinternet_mpls_data_transport_services": [
                r'AS\s*Cliente:\s*\d+',                      # ASN (critical for MPLS)
                r'interface\s+[A-Za-z0-9/]+',                # Interface names (critical)
                r'VLAN\s*:?\s*\d+',                          # VLAN (critical)
                r'IP\s*CPE:\s*\d+\.\d+\.\d+\.\d+',          # IP management (critical for MPLS)
                r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',             # Technology codes
                r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # POP descriptions
                r'MPLS.*VPN',                                # MPLS specific configurations
            ],
            
            "linkdeinternet_lan_to_lan_infrastructure": [
                r'AS\s*Cliente:\s*\d+',                      # ASN (critical)
                r'interface\s+[A-Za-z0-9/]+',                # Interface names (critical for LAN-LAN)
                r'IP\s*CPE:\s*\d+\.\d+\.\d+\.\d+',          # IP management (critical)
                r'VLAN\s*:?\s*\d+',                          # VLAN (critical)
                r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',             # Technology codes
                r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # POP descriptions
                r'LAN.*(?:bridge|switch)',                   # LAN infrastructure specific
            ],
            
            "linkdeinternet_ip_transit_services": [
                r'AS\s*Cliente:\s*\d+',                      # ASN (critical for IP Transit)
                r'interface\s+[A-Za-z0-9/]+',                # Interface names (critical)
                r'GTW:\s*\d+\.\d+\.\d+\.\d+',                # Gateway (critical for transit)
                r'VLAN\s*:?\s*\d+',                          # VLAN (critical)
                r'BLOCO\s*IP:\s*\d+\.\d+\.\d+\.\d+/\d+',    # IP prefixes (critical for transit)
                r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',             # Technology codes
                r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # POP descriptions
                r'BGP.*peer',                                # BGP configurations (transit specific)
            ],
            
            "linkdeinternet_traffic_exchange_ptt": [
                r'AS\s*Cliente:\s*\d+',                      # ASN (critical for PTT)
                r'interface\s+[A-Za-z0-9/]+',                # Interface names (critical)
                r'VLAN\s*:?\s*\d+',                          # VLAN (critical)
                r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',             # Technology codes
                r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # POP descriptions
                r'PTT.*IX',                                  # PTT/IX specific configurations
            ],
            
            "linkdeinternet_enterprise_gpon_lan": [
                r'AS\s*Cliente:\s*\d+',                      # ASN (critical)
                r'SN:\s*[A-Za-z0-9]+',                       # Serial numbers (critical for GPON)
                r'IP\s*CPE:\s*\d+\.\d+\.\d+\.\d+',          # IP management (critical)
                r'VLAN\s*:?\s*\d+',                          # VLAN (critical)
                r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+',             # Technology codes
                r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+', # POP descriptions
                r'OLT-[A-Z0-9-]+',                           # OLT equipment (GPON specific)
                r'ONU[:\s]*\d+',                             # ONU information (GPON specific)
            ]
        }
    
    def clean_text(self, text, product_group=None):
        """
        Advanced cleaning for telecom configuration text
        Removes noise while preserving technical information based on product group
        """
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        # Determine which preservation patterns to use
        preserve_patterns = self._get_preserve_patterns(product_group)
        
        # Step 1: Split into lines for line-by-line processing
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            original_line = line
            line = line.strip()
            
            if not line:
                continue
            
            # Check if line contains important info for this product group
            should_preserve = any(re.search(pattern, line, re.IGNORECASE) 
                                for pattern in preserve_patterns)
            
            if should_preserve:
                cleaned_lines.append(line)
                continue
            
            # Apply noise removal patterns
            is_noise = False
            for pattern, replacement in self.base_noise_patterns:
                if re.match(pattern, line, re.IGNORECASE | re.MULTILINE):
                    is_noise = True
                    break
            
            if is_noise:
                continue
            
            # Keep meaningful lines that aren't pure noise
            if self._is_meaningful_line(line, product_group):
                # Clean the line but keep it
                cleaned_line = self._clean_line(line)
                if cleaned_line.strip():
                    cleaned_lines.append(cleaned_line)
        
        # Step 2: Join and apply final cleanup
        cleaned_text = '\n'.join(cleaned_lines)
        
        # Apply global patterns
        for pattern, replacement in self.base_noise_patterns:
            if '\n' in pattern:  # Multi-line patterns
                cleaned_text = re.sub(pattern, replacement, cleaned_text, flags=re.MULTILINE)
        
        # Final cleanup
        cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)  # Max 2 consecutive newlines
        cleaned_text = cleaned_text.strip()
        
        return cleaned_text
    
    def clean_dataframe_by_groups(self, df, obs_column='obs', product_group_column='product_group'):
        """
        Clean text data in a DataFrame grouped by product classification
        """
        if obs_column not in df.columns:
            raise ValueError(f"Column '{obs_column}' not found in DataFrame")
        
        if product_group_column not in df.columns:
            print(f"⚠️ Column '{product_group_column}' not found. Using generic cleaning.")
            # Fallback to generic cleaning
            df[obs_column + '_cleaned'] = df[obs_column].apply(
                lambda x: self.clean_text(x, product_group=None)
            )
            return df
        
        # Clean by product group
        def clean_by_group(group):
            product_group = group[product_group_column].iloc[0] if len(group) > 0 else None
            group[obs_column + '_cleaned'] = group[obs_column].apply(
                lambda x: self.clean_text(x, product_group=product_group)
            )
            return group
        
        # Group by product group and apply cleaning
        cleaned_df = df.groupby(product_group_column, group_keys=False).apply(clean_by_group)
        
        return cleaned_df
    
    def _get_preserve_patterns(self, product_group):
        """Get preservation patterns for a specific product group"""
        if not product_group or product_group not in self.group_preserve_patterns:
            # Return combined patterns from all groups for maximum preservation
            all_patterns = []
            for patterns in self.group_preserve_patterns.values():
                all_patterns.extend(patterns)
            return list(set(all_patterns))  # Remove duplicates
        
        return self.group_preserve_patterns[product_group]
    
    def _is_meaningful_line(self, line, product_group=None):
        """
        Determine if a line contains meaningful information for the product group
        """
        line_clean = line.strip()
        
        # Too short to be meaningful
        if len(line_clean) < 3:
            return False
        
        # Get product group specific patterns
        mandatory_fields = self.product_group_manager.get_mandatory_fields(product_group) if product_group else []
        
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
        
        # Add product group specific meaningful patterns
        if product_group and mandatory_fields:
            # If line contains references to mandatory fields, it's likely meaningful
            for field in mandatory_fields:
                if field.replace('_', '').lower() in line_clean.lower():
                    return True
        
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
    
    def get_group_cleaning_summary(self, df, obs_column='obs', product_group_column='product_group'):
        """
        Get cleaning statistics by product group
        """
        if product_group_column not in df.columns:
            return {}
        
        summary = {}
        
        for group_name in df[product_group_column].unique():
            if pd.isna(group_name):
                continue
                
            group_data = df[df[product_group_column] == group_name]
            
            total_original_chars = 0
            total_cleaned_chars = 0
            
            for _, row in group_data.iterrows():
                original = str(row[obs_column]) if pd.notna(row[obs_column]) else ""
                cleaned = str(row[obs_column + '_cleaned']) if obs_column + '_cleaned' in row and pd.notna(row[obs_column + '_cleaned']) else ""
                
                total_original_chars += len(original)
                total_cleaned_chars += len(cleaned)
            
            summary[group_name] = {
                'group_display_name': self.product_group_manager.get_group_display_name(group_name),
                'total_records': len(group_data),
                'original_chars': total_original_chars,
                'cleaned_chars': total_cleaned_chars,
                'reduction_percent': round(100 * (1 - total_cleaned_chars / total_original_chars), 2) if total_original_chars > 0 else 0,
                'avg_original_length': round(total_original_chars / len(group_data), 2) if len(group_data) > 0 else 0,
                'avg_cleaned_length': round(total_cleaned_chars / len(group_data), 2) if len(group_data) > 0 else 0
            }
        
        return summary

# For backward compatibility
TelecomTextCleaner = EnhancedTelecomTextCleaner