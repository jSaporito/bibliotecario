import pandas as pd
import re

class TextCleaner:
    """Class for cleaning unstructured text data - Enhanced with original logic"""
    
    def __init__(self):
        # Characters that Excel cannot handle
        self.problematic_chars = [
            '\x00', '\x01', '\x02', '\x03', '\x04', '\x05', '\x06', '\x07', 
            '\x08', '\x0b', '\x0c', '\x0e', '\x0f', '\x10', '\x11', '\x12', 
            '\x13', '\x14', '\x15', '\x16', '\x17', '\x18', '\x19', '\x1a', 
            '\x1b', '\x1c', '\x1d', '\x1e', '\x1f'
        ]
    
    def clean_text(self, text):
        """Advanced cleaning function using original comprehensive logic"""
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            
            if not line:
                continue
            
            if self._is_separator_line_enhanced(line):
                continue
            
            if line.startswith('=>') and len(line.split()) <= 3:
                device_line = line.replace('=>', '').strip()
                if device_line:
                    cleaned_lines.append(f"DEVICE: {device_line}")
                continue
            
            # Remove useless comments (enhanced)
            if self._is_useless_comment_enhanced(line):
                continue
            
            # Clean model/serial lines (from original)
            if self._is_model_serial_line(line):
                cleaned_line = self._clean_model_serial_line(line)
                if cleaned_line:
                    cleaned_lines.append(cleaned_line)
                continue
            
            # Clean whitespace
            line = re.sub(r'\s+', ' ', line)
            
            # Remove pure noise (enhanced)
            if self._is_noise_line_enhanced(line):
                continue
            
            # Keep meaningful content (enhanced)
            if self._is_meaningful_content_enhanced(line):
                cleaned_lines.append(line)
        
        cleaned_text = '\n'.join(cleaned_lines)
        cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)
        cleaned_text = cleaned_text.strip()
        
        return cleaned_text
    
    def _is_separator_line_enhanced(self, line):
        """Enhanced separator line detection from original code"""
        patterns = [
            r'^[-=_~*+#]{3,}$',
            r'^[-=_~*+#\s]{3,}$',
            r'^[-]{10,}$',
            r'^[=]{10,}$',
            r'^[x]{5,}$',
            r'^[#]{5,}$',
            r'^[*]{5,}$',
            r'^[=>]{1,10}$'
        ]
        
        for pattern in patterns:
            if re.match(pattern, line, re.IGNORECASE):
                return True
        return False
    
    def _is_useless_comment_enhanced(self, line):
        """Enhanced useless comment detection from original code"""
        # Short hash comments
        if line.startswith('#') and len(line) <= 5:
            return True
        
        # Date/time comments
        if re.match(r'^# \w{3}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', line):
            return True
        
        # Software ID lines
        if 'software id =' in line.lower() and len(line) < 50:
            return True
        
        return False
    
    def _is_model_serial_line(self, line):
        """Check if line contains model or serial information"""
        return line.startswith('# model =') or line.startswith('# serial number =')
    
    def _clean_model_serial_line(self, line):
        """Clean model/serial lines and extract important info"""
        if '=' in line:
            key_value = line.split('=', 1)
            if len(key_value) == 2:
                key = key_value[0].replace('#', '').strip()
                value = key_value[1].strip()
                return f"{key}: {value}"
        return None
    
    def _is_noise_line_enhanced(self, line):
        """Enhanced noise line detection from original code"""
        return re.match(r'^[^\w\d\s:./-]{3,}$', line)
    
    def _is_meaningful_content_enhanced(self, line):
        """Enhanced meaningful content detection from original code"""
        return len(line) > 2 and not re.match(r'^[^\w\d]{3,}$', line)
    
    def sanitize_for_excel(self, text):
        """Remove characters that Excel cannot handle"""
        if not isinstance(text, str):
            return text
        
        # Remove control characters that Excel can't handle
        sanitized = ''.join(char for char in text if ord(char) >= 32 or char in '\t\n\r')
        
        # Remove specific problematic characters
        for char in self.problematic_chars:
            sanitized = sanitized.replace(char, '')
        
        return sanitized
    
    def get_cleaning_stats(self, original_text, cleaned_text):
        """Get statistics about the cleaning process"""
        if not isinstance(original_text, str) or not isinstance(cleaned_text, str):
            return {}
        
        original_len = len(original_text)
        cleaned_len = len(cleaned_text)
        
        reduction = ((original_len - cleaned_len) / original_len) * 100 if original_len > 0 else 0
        
        return {
            'original_length': original_len,
            'cleaned_length': cleaned_len,
            'characters_removed': original_len - cleaned_len,
            'reduction_percentage': reduction,
            'original_lines': len(original_text.split('\n')),
            'cleaned_lines': len(cleaned_text.split('\n'))
        }
    
    def clean_extracted_data(self, data):
        """Clean and standardize extracted data"""
        cleaned = {}
        
        for field, value in data.items():
            if not value or str(value).strip() == '':
                cleaned[field] = None
                continue
            
            value = str(value).strip()
            value = self.sanitize_for_excel(value)
            
            # Field-specific cleaning (enhanced from original)
            if field == 'mac':
                cleaned[field] = self._clean_mac_address(value)
            elif field == 'vlan':
                cleaned[field] = self._clean_vlan(value)
            elif field == 'asn':
                cleaned[field] = self._clean_asn(value)
            elif field in ['ip_management', 'ip_telephony', 'ip_block', 'gateway']:
                cleaned[field] = self._clean_ip_address(value)
            else:
                cleaned[field] = value
        
        return cleaned
    
    def _clean_mac_address(self, value):
        """Clean and standardize MAC address format"""
        mac_clean = re.sub(r'[^\w]', '', value)
        if len(mac_clean) == 12 and re.match(r'^[0-9A-Fa-f]+$', mac_clean):
            mac_formatted = ':'.join([mac_clean[i:i+2] for i in range(0, 12, 2)])
            return mac_formatted.upper()
        return value
    
    def _clean_vlan(self, value):
        """Extract numeric VLAN value"""
        vlan_num = re.search(r'\d+', value)
        if vlan_num:
            return int(vlan_num.group())
        return value
    
    def _clean_asn(self, value):
        """Extract numeric ASN value"""
        asn_num = re.search(r'\d+', value)
        if asn_num:
            return int(asn_num.group())
        return value
    
    def _clean_ip_address(self, value):
        """Clean IP address format"""
        ip_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?:/[0-9]{1,2})?\b', value)
        if ip_match:
            return ip_match.group()
        return value