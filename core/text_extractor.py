import re
import pandas as pd
from datetime import datetime

class EnhancedTelecomTextExtractor:
    """
    Enhanced text extractor for telecom/network infrastructure data
    Extracts structured information from cleaned configuration text
    """
    
    def __init__(self):
        # Enhanced patterns based on actual data analysis
        self.field_patterns = {
            # Ticket and Service Information
            'ticket_number': [
                r'#(\d{8}-\d+)',
                r'ticket[:\s]*(\d{8}-\d+)',
            ],
            
            'activation_type': [
                r'(ATIVAÇÃO\s+INICIAL)',
                r'(ATIVAÇÃO\s+SECUNDÁRIA)',
                r'(MIGRAÇÃO)',
                r'(ALTERAÇÃO)',
            ],
            
            'plan_number': [
                r'PLANO\s+(\d+)',
                r'BN\s*:?\s*(\d+)',
            ],
            
            # Network Infrastructure
            'service_code': [
                r'([A-Z]{3,}\d*/[A-Z]{2,}/\d+/\d+)',
                r'(RIOS|SDI\d|FLA\d)/[A-Z]+/\d+/\d+',
                r'([A-Z]{3,}/[A-Z]{2,}/\d+/\d+-[A-Z-]+)',
            ],
            
            'pop_location': [
                r'br\.([a-z]{2}\.[a-z]{2,}\.[\w.]+\.pe\.\d+)',
                r'OLT-([A-Z]{2}-[A-Z-]+\d+)',
                r'BR-([A-Z]{2}-[A-Z-]+-[A-Z-]+)',
            ],
            
            # IP Address Management
            'ip_cpe': [
                r'IP\s*CPE[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'IP[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            'gateway_ip': [
                r'GTW[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'gateway[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            'ip_block': [
                r'BLOCO\s*IP[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'rede\s*cliente\s*v4[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
            ],
            
            'subnet_mask': [
                r'MASC[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'netmask[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            # IPv6 Information
            'ipv6_block': [
                r'rede\s*cliente\s*v6[:\s]*([0-9a-fA-F:]+::/\d{1,3})',
                r'ipv6[:\s]*([0-9a-fA-F:]+::/\d{1,3})',
            ],
            
            # VLAN Configuration
            'vlan_id': [
                r'VLAN\s*:?\s*(\d+)',
                r'vlan\s*(\d+)',
                r'Vlan\s*(\d+)',
            ],
            
            # Equipment Information
            'olt_name': [
                r'(OLT-[A-Z0-9-]+)',
                r'(OLT[A-Z0-9-]+)',
            ],
            
            'slot_number': [
                r'SLOT[:\s]*(\d+)',
                r'Slot(\d+)',
                r'slot[:\s]*(\d+)',
            ],
            
            'port_number': [
                r'PORT[:\s]*(\d+)',
                r'Port(\d+)',
                r'port[:\s]*(\d+)',
            ],
            
            'onu_id': [
                r'ONU[:\s]*(\d+)',
                r'OnuID(\d+)',
                r'onu[:\s]*(\d+)',
            ],
            
            'serial_number': [
                r'SN[:\s]*([A-Za-z0-9]+)',
                r'serial\s*number[:\s=]*([A-Za-z0-9]+)',
                r'# serial number = ([A-Za-z0-9]+)',
            ],
            
            # ASN and BGP Information
            'asn_client': [
                r'AS\s*Cliente[:\s]*(\d+)',
                r'as-number[:\s]*(\d+)',
                r'remote-as[:\s]*(\d+)',
            ],
            
            'peer_ip': [
                r'peer\s*wirelink\s*v4[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'neighbor[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            # Optical Network Information
            'optical_power': [
                r'Rx\s*Optical\s*Power.*?=\s*([-\d.]+)',
                r'optical.*?power.*?([-\d.]+\s*dBm)',
            ],
            
            # Network Equipment Models
            'equipment_model': [
                r'# model = ([\w\d-]+)',
                r'model[:\s]*=\s*([\w\d-]+)',
                r'([A-Z]{4,}\d+/Frame\d+/Slot\d+/Port\d+)',
            ],
            
            'interface_name': [
                r'interface\s*([\w\d/-]+)',
                r'interface\s*vlan\s*(\d+)',
                r'ethernet\s*([0-9/]+)',
            ],
            
            # Service Descriptions
            'service_description': [
                r'description\s*(.*?)(?:\n|$)',
                r'name\s*(.*?)(?:\n|$)',
                r'comment[:\s=]*(.*?)(?:\n|$)',
            ],
            
            # WiFi Information (if present)
            'wifi_ssid': [
                r'SSID[:\s]*([A-Za-z0-9_-]+)',
                r'ssid[:\s]*([A-Za-z0-9_-]+)',
            ],
            
            'wifi_password': [
                r'password[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
                r'passcode[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
            ],
            
            # MAC Addresses
            'mac_address': [
                r'([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})',
                r'([0-9A-Fa-f]{12})',
            ],
        }
    
    def extract_all_fields(self, text):
        """
        Extract all available fields from telecom configuration text
        """
        result = {field: None for field in self.field_patterns.keys()}
        
        if pd.isna(text) or not isinstance(text, str):
            return result
        
        # Process each field type
        for field, patterns in self.field_patterns.items():
            best_match = None
            best_confidence = 0
            
            for pattern in patterns:
                try:
                    matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
                    if matches:
                        for match in matches:
                            if isinstance(match, tuple):
                                # Take the first non-empty group
                                match = next((m for m in match if m), '')
                            
                            if match and str(match).strip():
                                clean_match = str(match).strip()
                                confidence = self._calculate_confidence(pattern, clean_match, field)
                                
                                if confidence > best_confidence:
                                    best_match = clean_match
                                    best_confidence = confidence
                                    
                except re.error as e:
                    print(f"Regex error in field '{field}' with pattern '{pattern}': {e}")
                    continue
            
            if best_match and best_confidence > 0.3:
                result[field] = self._clean_field_value(best_match, field)
        
        return result
    
    def _calculate_confidence(self, pattern, match, field):
        """
        Calculate confidence score for field extraction
        """
        confidence = 0.5  # Base confidence
        
        # Field-specific validation and confidence boosts
        if field == 'vlan_id' and match.isdigit():
            vlan_num = int(match)
            if 1 <= vlan_num <= 4094:
                confidence += 0.3
        
        elif field in ['ip_cpe', 'gateway_ip', 'ip_block', 'peer_ip']:
            # Validate IP format
            ip_part = match.split('/')[0]
            try:
                parts = ip_part.split('.')
                if len(parts) == 4 and all(0 <= int(p) <= 255 for p in parts):
                    confidence += 0.3
            except (ValueError, AttributeError):
                confidence -= 0.2
        
        elif field == 'asn_client' and match.isdigit():
            try:
                asn_num = int(match)
                if 1 <= asn_num <= 4294967295:
                    confidence += 0.3
            except ValueError:
                confidence -= 0.2
        
        elif field == 'serial_number' and len(match) >= 6:
            confidence += 0.2
        
        elif field == 'optical_power':
            try:
                power = float(re.findall(r'[-\d.]+', match)[0])
                if -50 <= power <= 10:  # Reasonable optical power range
                    confidence += 0.3
            except (ValueError, IndexError):
                confidence -= 0.1
        
        # Pattern specificity bonus
        if len(pattern) > 25:  # More specific patterns get bonus
            confidence += 0.1
        
        return min(1.0, max(0, confidence))
    
    def _clean_field_value(self, value, field):
        """
        Clean and normalize extracted field values
        """
        if not value:
            return None
        
        value = str(value).strip()
        
        # Field-specific cleaning
        if field in ['vlan_id', 'slot_number', 'port_number', 'onu_id', 'plan_number']:
            # Extract numeric value
            num_match = re.search(r'\b(\d+)\b', value)
            return int(num_match.group(1)) if num_match else value
        
        elif field == 'asn_client':
            # Extract ASN number
            asn_match = re.search(r'\b(\d+)\b', value)
            return int(asn_match.group(1)) if asn_match else value
        
        elif field in ['ip_cpe', 'gateway_ip', 'subnet_mask', 'peer_ip']:
            # Clean IP addresses
            ip_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?:/\d{1,2})?\b', value)
            return ip_match.group() if ip_match else value
        
        elif field == 'ip_block':
            # Clean IP blocks (with CIDR)
            block_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}/\d{1,2}\b', value)
            return block_match.group() if block_match else value
        
        elif field == 'ipv6_block':
            # Clean IPv6 blocks
            ipv6_match = re.search(r'[0-9a-fA-F:]+::/\d{1,3}', value)
            return ipv6_match.group() if ipv6_match else value
        
        elif field == 'mac_address':
            # Normalize MAC address format
            mac_clean = re.sub(r'[^0-9A-Fa-f]', '', value)
            if len(mac_clean) == 12:
                return ':'.join(mac_clean[i:i+2] for i in range(0, 12, 2)).upper()
            return value
        
        elif field == 'optical_power':
            # Extract numeric value with unit
            power_match = re.search(r'([-\d.]+)', value)
            if power_match:
                return f"{power_match.group(1)} dBm"
            return value
        
        elif field == 'service_description':
            # Clean service descriptions
            return re.sub(r'\s+', ' ', value)[:200]  # Limit length and clean spaces
        
        elif field in ['wifi_ssid', 'wifi_password']:
            # Remove quotes and clean WiFi credentials
            return re.sub(r'^["\']|["\']$', '', value)
        
        return value
    
    def get_field_list(self):
        """Return list of all extractable fields"""
        return list(self.field_patterns.keys())
    
    def process_text(self, text):
        """Backward compatibility method"""
        return self.extract_all_fields(text)
    
    def get_extraction_stats(self, text):
        """Get detailed extraction statistics"""
        if not isinstance(text, str):
            return {}
        
        stats = {
            'text_length': len(text),
            'lines_count': len(text.split('\n')),
            'patterns_found': {},
            'field_coverage': {}
        }
        
        # Analyze pattern coverage
        for field, patterns in self.field_patterns.items():
            total_matches = 0
            for pattern in patterns:
                try:
                    matches = len(re.findall(pattern, text, re.IGNORECASE))
                    total_matches += matches
                except re.error:
                    continue
            
            stats['patterns_found'][field] = total_matches
            stats['field_coverage'][field] = 'high' if total_matches > 0 else 'none'
        
        return stats

# For backward compatibility
TelecomTextExtractor = EnhancedTelecomTextExtractor