import pandas as pd
import re
from collections import defaultdict

class TelecomTextCleaner:
    """Specialized text cleaner for telecom/network infrastructure data"""
    
    def __init__(self):
        # Patterns to remove (noise/separators)
        self.noise_patterns = [
            r'^[-=_~*+#]{5,}$',                    # Long separators
            r'^\*{10,}.*\*{10,}$',                 # Asterisk blocks
            r'^#{1,3}\s*$',                        # Empty hash comments
            r'^\s*$',                              # Empty lines
            r'^[^\w\d\s:./-]{3,}$',                # Pure symbol lines
        ]
        
        # Patterns to preserve (important info)
        self.preserve_patterns = [
            r'#\d{8}-\d+.*ATIVAÇÃO',               # Activation tickets
            r'PLANO\s+\d+',                        # Plan numbers
            r'BN\s*\d+',                           # BN numbers
            r'[A-Z]{2,}/[A-Z]{2,}/\d+/\d+-',      # Service codes
        ]
    
    def clean_text(self, text):
        """Advanced cleaning for telecom configuration text"""
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            
            if not line:
                continue
            
            # Check if line should be preserved (contains important info)
            should_preserve = any(re.search(pattern, line, re.IGNORECASE) 
                                for pattern in self.preserve_patterns)
            
            if should_preserve:
                cleaned_lines.append(line)
                continue
            
            # Check if line is noise and should be removed
            is_noise = any(re.match(pattern, line, re.IGNORECASE) 
                          for pattern in self.noise_patterns)
            
            if is_noise:
                continue
            
            # Clean whitespace and keep meaningful content
            if len(line) > 2 and not re.match(r'^[^\w\d]{3,}$', line):
                # Normalize whitespace
                line = re.sub(r'\s+', ' ', line)
                cleaned_lines.append(line)
        
        # Join and clean up multiple empty lines
        cleaned_text = '\n'.join(cleaned_lines)
        cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)
        
        return cleaned_text.strip()

