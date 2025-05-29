import re
import pandas as pd

class TelecomTextExtractor:
    
    def __init__(self):
        # Enhanced patterns based on actual data analysis
        self.field_patterns = {
            # Network Infrastructure
            'technology_id': [
                r'PLANO\s*(\d+)',
                r'BN[:\s]*(\d+)',
                r'(IPCOR|L2LAN|TRANSIT)',
                r'(ATIVAÇÃO\s+INICIAL)',
            ],
            
            'provider_id': [
                r'([A-Z]{3,}\d*/[A-Z]{2,}/\d+/\d+)',
                r'(RIOS|SDI\d|FLA\d)/[A-Z]+/\d+',
            ],
            
            'pop': [
                r'br\.([a-z]{2}\.[a-z]{2,}\.[\w.]+)',
                r'BR-([A-Z]{2}-[A-Z]{2,}-[\w-]+)',
                r'OLT-([A-Z]{2}-[\w-]+)',
            ],
            
            # IP Addresses (Enhanced)
            'ip_management': [
                r'IP\s*CPE[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'IP[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'ip\s*address\s*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            'gateway': [
                r'GTW[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'gateway\s*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'route-static.*?([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            'ip_block': [
                r'BLOCO\s*IP[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'rede\s*cliente\s*v4[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'network[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
            ],
            
            # IPv6 Addresses
            'ipv6_block': [
                r'rede\s*cliente\s*v6[:\s]*([0-9a-fA-F:]+::/\d{1,3})',
                r'ipv6\s*address\s*([0-9a-fA-F:]+::/\d{1,3})',
                r'([0-9a-fA-F]{4}:[0-9a-fA-F:]+::/\d{1,3})',
            ],
            
            # VLAN Information
            'vlan': [
                r'VLAN[:\s]*(\d+)',
                r'vlan\s*(\d+)',
                r'vlan-id\s*(\d+)',
                r'source-vlan\s*(\d+)',
            ],
            
            # Equipment Information
            'slot': [
                r'SLOT[:\s]*(\d+)',
                r'Slot(\d+)',
                r'slot\s*(\d+)',
            ],
            
            'port': [
                r'PORT[:\s]*(\d+)',
                r'Port(\d+)',
                r'port\s*(\d+)',
            ],
            
            'onu_id': [
                r'ONU[:\s]*(\d+)',
                r'OnuID(\d+)',
                r'onu\s*(\d+)',
            ],
            
            'serial': [
                r'SN[:\s]*([A-Za-z0-9]+)',
                r'serial\s*number[:\s=]*([A-Za-z0-9]+)',
                r'# serial number = ([A-Za-z0-9]+)',
            ],
            
            # ASN Information
            'asn': [
                r'AS\s*Cliente[:\s]*(\d+)',
                r'as-number\s*(\d+)',
                r'remote-as\s*(\d+)',
                r'peer-as\s*(\d+)',
            ],
            
            # Network Equipment
            'cpe': [
                r'# model = ([\w\d]+)',
                r'model[:\s]*=\s*([\w\d]+)',
                r'CPE[:\s]*([\w\d-]+)',
            ],
            
            'interface_1': [
                r'interface\s*([\w\d/-]+)',
                r'ethernet\s*([0-9/]+)',
                r'ether(\d+)',
            ],
            
            'optical_power': [
                r'Rx\s*Optical\s*Power.*?=\s*([-\d.]+)',
                r'optical.*?power.*?([-\d.]+\s*dBm)',
            ],
            
            # Service Information
            'service_description': [
                r'description\s*(.*?)(?:\n|$)',
                r'name\s*(.*?)(?:\n|$)',
                r'comment[:\s=]*(.*?)(?:\n|$)',
            ],
            
            # BGP/Routing
            'peer_ip': [
                r'peer\s*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'neighbor\s*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            'peer_ipv6': [
                r'peer\s*([0-9a-fA-F:]+::[0-9a-fA-F:]+)',
                r'neighbor\s*([0-9a-fA-F:]+::[0-9a-fA-F:]+)',
            ],
        }
    
    def process_text(self, text):
        """Extract all fields from telecom configuration text"""
        result = {field: None for field in self.field_patterns.keys()}
        
        if pd.isna(text) or not isinstance(text, str):
            return result
        
        # Extract each field type
        for field, patterns in self.field_patterns.items():
            best_match = None
            best_confidence = 0
            
            for pattern in patterns:
                try:
                    matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
                    if matches:
                        # Take the first valid match
                        for match in matches:
                            if isinstance(match, tuple):
                                match = match[0] if match[0] else (match[1] if len(match) > 1 else '')
                            
                            if match and str(match).strip():
                                clean_match = str(match).strip()
                                
                                # Calculate confidence based on pattern specificity
                                confidence = self._calculate_confidence(pattern, clean_match, field)
                                
                                if confidence > best_confidence:
                                    best_match = clean_match
                                    best_confidence = confidence
                                    
                except re.error:
                    continue
            
            if best_match and best_confidence > 0.3:
                result[field] = self._clean_field_value(best_match, field)
        
        return result
    
    def _calculate_confidence(self, pattern, match, field):
        """Calculate confidence score for extraction"""
        confidence = 0.5  # Base confidence
        
        # Field-specific confidence boosts
        if field == 'vlan' and match.isdigit():
            vlan_num = int(match)
            if 1 <= vlan_num <= 4094:
                confidence += 0.3
        
        elif field in ['ip_management', 'gateway', 'ip_block']:
            # Validate IP format
            ip_part = match.split('/')[0]
            parts = ip_part.split('.')
            if len(parts) == 4 and all(0 <= int(p) <= 255 for p in parts if p.isdigit()):
                confidence += 0.3
        
        elif field == 'serial' and len(match) >= 6:
            confidence += 0.2
        
        elif field == 'asn' and match.isdigit():
            asn_num = int(match)
            if 1 <= asn_num <= 4294967295:
                confidence += 0.3
        
        # Pattern specificity bonus
        if len(pattern) > 20:  # More specific patterns
            confidence += 0.1
        
        return min(1.0, confidence)
    
    def _clean_field_value(self, value, field):
        """Clean extracted field values"""
        if not value:
            return None
        
        value = str(value).strip()
        
        # Field-specific cleaning
        if field == 'vlan':
            # Extract just the number
            vlan_match = re.search(r'\b(\d+)\b', value)
            return int(vlan_match.group(1)) if vlan_match else value
        
        elif field == 'asn':
            # Extract just the number
            asn_match = re.search(r'\b(\d+)\b', value)
            return int(asn_match.group(1)) if asn_match else value
        
        elif field in ['slot', 'port', 'onu_id']:
            # Extract numeric value
            num_match = re.search(r'\b(\d+)\b', value)
            return int(num_match.group(1)) if num_match else value
        
        elif field in ['ip_management', 'gateway', 'ip_block', 'peer_ip']:
            # Clean IP addresses
            ip_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?:/\d{1,2})?\b', value)
            return ip_match.group() if ip_match else value
        
        elif field in ['ipv6_block', 'peer_ipv6']:
            # Clean IPv6 addresses
            ipv6_match = re.search(r'[0-9a-fA-F:]+::[0-9a-fA-F:/]*', value)
            return ipv6_match.group() if ipv6_match else value
        
        elif field == 'service_description':
            # Clean service descriptions
            return re.sub(r'\s+', ' ', value)[:100]  # Limit length
        
        return value
    
    def get_field_list(self):
        """Return list of all extractable fields"""
        return list(self.field_patterns.keys())
    
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

