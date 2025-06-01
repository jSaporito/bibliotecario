"""
Group-Based Text Cleaner - Refactored
Cleans text based on product group specific rules and preserves critical patterns
"""

import pandas as pd
import re
from collections import defaultdict

class GroupBasedTextCleaner:
    """
    Enhanced text cleaner that applies group-specific cleaning rules
    Preserves mandatory field patterns while removing noise
    """
    
    def __init__(self, product_group_manager):
        self.group_manager = product_group_manager
        
        # Base noise patterns (applied to all groups)
        self.base_noise_patterns = [
            # Command line noise
            (r'^\s*quit\s*$', ''),
            (r'^\s*exit\s*$', ''),
            (r'^\s*end\s*$', ''),
            (r'^\s*return\s*$', ''),
            (r'^configure terminal.*$', ''),
            
            # Separator lines
            (r'^[-=_~*+#]{10,}.*$', ''),
            (r'^[*]{5,}[^*]*[*]{5,}$', ''),
            (r'^#+\s*$', ''),
            
            # Debug/log lines
            (r'^\s*DEBUG:.*$', ''),
            (r'^\s*INFO:.*$', ''),
            (r'^\s*WARNING:.*$', ''),
            (r'^\s*ERROR:.*$', ''),
            (r'^\s*\[.*?\]\s*$', ''),
            
            # Whitespace cleanup
            (r'\n{4,}', '\n\n'),
            (r'[ \t]{3,}', ' '),
            (r'^\s+$', ''),
        ]
        
        # Group-specific cleaning statistics
        self.cleaning_stats = defaultdict(lambda: {
            'total_processed': 0,
            'total_chars_removed': 0,
            'patterns_removed': defaultdict(int),
            'patterns_preserved': defaultdict(int)
        })
    
    def clean_text_by_group(self, text, product_group=None):
        """
        Clean text using group-specific rules
        """
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        original_text = text
        original_length = len(text)
        
        # Get group-specific cleaning rules
        if product_group and self.group_manager.is_valid_group(product_group):
            cleaning_rules = self.group_manager.get_cleaning_rules(product_group)
            preserve_patterns = cleaning_rules.get('preserve_patterns', [])
            remove_patterns = cleaning_rules.get('remove_patterns', [])
        else:
            preserve_patterns = []
            remove_patterns = []
        
        # Step 1: Identify and mark preservation areas
        preserved_segments = self._identify_preservation_segments(text, preserve_patterns, product_group)
        
        # Step 2: Apply group-specific removal patterns
        text = self._apply_removal_patterns(text, remove_patterns, preserved_segments, product_group)
        
        # Step 3: Apply base noise removal (avoiding preserved areas)
        text = self._apply_base_cleaning(text, preserved_segments, product_group)
        
        # Step 4: Final cleanup
        text = self._final_cleanup(text)
        
        # Update statistics
        self._update_cleaning_stats(product_group, original_text, text, original_length)
        
        return text
    
    def clean_dataframe_by_groups(self, df, obs_column='obs', product_group_column='product_group'):
        """
        Clean DataFrame with group-aware processing
        """
        if obs_column not in df.columns:
            raise ValueError(f"Column '{obs_column}' not found in DataFrame")
        
        if product_group_column not in df.columns:
            print(f"⚠️ Column '{product_group_column}' not found. Using generic cleaning.")
            # Fallback to generic cleaning
            df[obs_column + '_cleaned'] = df[obs_column].apply(
                lambda x: self.clean_text_by_group(x, product_group=None)
            )
            return df
        
        # Process by product group for optimized cleaning
        def clean_group(group):
            product_group = group[product_group_column].iloc[0] if len(group) > 0 else None
            
            print(f"🧹 Cleaning group: {self.group_manager.get_group_display_name(product_group)} ({len(group)} records)")
            
            # Apply group-specific cleaning
            group[obs_column + '_cleaned'] = group[obs_column].apply(
                lambda x: self.clean_text_by_group(x, product_group=product_group)
            )
            
            return group
        
        # Group by product group and apply cleaning
        cleaned_df = df.groupby(product_group_column, group_keys=False).apply(clean_group)
        
        return cleaned_df
    
    def _identify_preservation_segments(self, text, preserve_patterns, product_group):
        """
        Identify text segments that should be preserved based on mandatory field patterns
        """
        preserved_segments = []
        
        # Add mandatory field patterns for preservation
        mandatory_fields = self.group_manager.get_mandatory_fields(product_group) if product_group else []
        
        # Convert mandatory fields to regex patterns for preservation
        mandatory_patterns = []
        if mandatory_fields:
            field_patterns = {
                'serial_code': [r'SN[:\s]*[A-Za-z0-9]+'],
                'wifi_ssid': [r'SSID[:\s]*[A-Za-z0-9_-]+'],
                'wifi_passcode': [r'password[:\s]*[A-Za-z0-9@#$%^&*()_=+-]+'],
                'vlan': [r'VLAN\s*:?\s*\d+'],
                'ip_management': [r'IP\s*CPE[:\s]*([0-9]{1,3}\.){3}[0-9]{1,3}'],
                'client_type': [r'(RESIDENCIAL|EMPRESARIAL|CORPORATIVO)'],
                'technology_id': [r'(GPON|EPON|ETHERNET|MPLS|P2P)'],
                'asn': [r'AS\s*Cliente[:\s]*\d+'],
                'interface_1': [r'interface\s*([\w\d/\-]+)'],
                'pop_description': [r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+']
            }
            
            for field in mandatory_fields:
                if field in field_patterns:
                    mandatory_patterns.extend(field_patterns[field])
        
        # Combine with group-specific preserve patterns
        all_preserve_patterns = preserve_patterns + mandatory_patterns
        
        # Find all preservation segments
        for pattern in all_preserve_patterns:
            try:
                for match in re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE):
                    preserved_segments.append({
                        'start': match.start(),
                        'end': match.end(),
                        'text': match.group(),
                        'pattern': pattern
                    })
                    
                    # Update preservation stats
                    if product_group:
                        self.cleaning_stats[product_group]['patterns_preserved'][pattern] += 1
                        
            except re.error as e:
                print(f"⚠️ Invalid regex pattern '{pattern}': {e}")
                continue
        
        # Sort by position and merge overlapping segments
        preserved_segments.sort(key=lambda x: x['start'])
        merged_segments = []
        
        for segment in preserved_segments:
            if merged_segments and segment['start'] <= merged_segments[-1]['end']:
                # Merge overlapping segments
                merged_segments[-1]['end'] = max(merged_segments[-1]['end'], segment['end'])
                merged_segments[-1]['text'] = text[merged_segments[-1]['start']:merged_segments[-1]['end']]
            else:
                merged_segments.append(segment)
        
        return merged_segments
    
    def _apply_removal_patterns(self, text, remove_patterns, preserved_segments, product_group):
        """
        Apply group-specific removal patterns while avoiding preserved segments
        """
        for pattern, replacement in [(p, '') for p in remove_patterns]:
            try:
                # Find matches that don't overlap with preserved segments
                matches = list(re.finditer(pattern, text, re.IGNORECASE | re.MULTILINE))
                
                # Filter out matches that overlap with preserved segments
                safe_matches = []
                for match in matches:
                    is_safe = True
                    for preserved in preserved_segments:
                        if (match.start() < preserved['end'] and match.end() > preserved['start']):
                            is_safe = False
                            break
                    if is_safe:
                        safe_matches.append(match)
                
                # Apply replacements in reverse order to maintain positions
                for match in reversed(safe_matches):
                    text = text[:match.start()] + replacement + text[match.end():]
                    
                    # Update removal stats
                    if product_group and safe_matches:
                        self.cleaning_stats[product_group]['patterns_removed'][pattern] += len(safe_matches)
                        
            except re.error as e:
                print(f"⚠️ Invalid removal pattern '{pattern}': {e}")
                continue
        
        return text
    
    def _apply_base_cleaning(self, text, preserved_segments, product_group):
        """
        Apply base noise removal patterns while preserving critical segments
        """
        for pattern, replacement in self.base_noise_patterns:
            try:
                # For line-based patterns, process line by line
                if pattern.startswith('^') or pattern.endswith(''):
                    lines = text.split('\n')
                    cleaned_lines = []
                    
                    for line_idx, line in enumerate(lines):
                        # Check if this line overlaps with any preserved segment
                        line_start = sum(len(l) + 1 for l in lines[:line_idx])  # +1 for \n
                        line_end = line_start + len(line)
                        
                        is_preserved = False
                        for preserved in preserved_segments:
                            if (line_start < preserved['end'] and line_end > preserved['start']):
                                is_preserved = True
                                break
                        
                        if not is_preserved and re.match(pattern, line, re.IGNORECASE):
                            # Apply replacement
                            cleaned_lines.append(re.sub(pattern, replacement, line))
                        else:
                            cleaned_lines.append(line)
                    
                    text = '\n'.join(cleaned_lines)
                else:
                    # For non-line patterns, apply globally but avoid preserved areas
                    text = re.sub(pattern, replacement, text, flags=re.IGNORECASE | re.MULTILINE)
                    
            except re.error as e:
                print(f"⚠️ Invalid base pattern '{pattern}': {e}")
                continue
        
        return text
    
    def _final_cleanup(self, text):
        """
        Final text cleanup and normalization
        """
        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]{2,}', ' ', text)
        
        # Remove empty lines at start/end
        text = text.strip()
        
        # Normalize line endings
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\r', '\n', text)
        
        return text
    
    def _update_cleaning_stats(self, product_group, original_text, cleaned_text, original_length):
        """
        Update cleaning statistics for analytics
        """
        if not product_group:
            product_group = 'generic'
        
        cleaned_length = len(cleaned_text)
        chars_removed = original_length - cleaned_length
        
        stats = self.cleaning_stats[product_group]
        stats['total_processed'] += 1
        stats['total_chars_removed'] += chars_removed
        stats['avg_reduction_percent'] = (stats['total_chars_removed'] / 
                                        (stats['total_processed'] * original_length)) * 100 if original_length > 0 else 0
    
    def get_cleaning_stats(self, original_text, cleaned_text):
        """
        Get detailed cleaning statistics for a single text
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
            'chars_removed': len(original_text) - len(cleaned_text)
        }
    
def get_group_cleaning_summary(self, df, obs_column='obs', product_group_column='product_group'):
    """
    Get comprehensive cleaning statistics by product group
    """
    # Add this null check at the very beginning
    if df is None:
        return {
            'total_groups': 0,
            'records_cleaned': 0,
            'cleaning_efficiency': 0.0,
            'group_details': {}
        }
    
    if product_group_column not in df.columns:
        return {}
        
        for group_name in df[product_group_column].unique():
            if pd.isna(group_name):
                continue
                
            group_data = df[df[product_group_column] == group_name]
            group_display_name = self.group_manager.get_group_display_name(group_name)
            
            # Calculate cleaning impact
            total_original_chars = 0
            total_cleaned_chars = 0
            total_patterns_removed = 0
            total_patterns_preserved = 0
            
            for _, row in group_data.iterrows():
                original = str(row[obs_column]) if pd.notna(row[obs_column]) else ""
                cleaned = str(row[obs_column + '_cleaned']) if obs_column + '_cleaned' in row and pd.notna(row[obs_column + '_cleaned']) else ""
                
                total_original_chars += len(original)
                total_cleaned_chars += len(cleaned)
            
            # Get stats from cleaning process
            if group_name in self.cleaning_stats:
                stats = self.cleaning_stats[group_name]
                total_patterns_removed = sum(stats['patterns_removed'].values())
                total_patterns_preserved = sum(stats['patterns_preserved'].values())
            
            summary[group_name] = {
                'group_display_name': group_display_name,
                'category': self.group_manager.get_group_category(group_name),
                'priority_level': self.group_manager.get_group_priority_level(group_name),
                'total_records': len(group_data),
                'original_chars': total_original_chars,
                'cleaned_chars': total_cleaned_chars,
                'reduction_percent': round(100 * (1 - total_cleaned_chars / total_original_chars), 2) if total_original_chars > 0 else 0,
                'avg_original_length': round(total_original_chars / len(group_data), 2) if len(group_data) > 0 else 0,
                'avg_cleaned_length': round(total_cleaned_chars / len(group_data), 2) if len(group_data) > 0 else 0,
                'patterns_removed': total_patterns_removed,
                'patterns_preserved': total_patterns_preserved,
                'mandatory_fields': self.group_manager.get_mandatory_fields(group_name),
                'cleaning_efficiency': self._calculate_cleaning_efficiency(group_name, total_original_chars, total_cleaned_chars)
            }
        
        return summary
    
    def _calculate_cleaning_efficiency(self, group_name, original_chars, cleaned_chars):
        """
        Calculate cleaning efficiency score based on reduction and preservation
        """
        if original_chars == 0:
            return 0
        
        reduction_score = min(100, (original_chars - cleaned_chars) / original_chars * 100)
        
        # Bonus for preserving mandatory field patterns
        if group_name in self.cleaning_stats:
            stats = self.cleaning_stats[group_name]
            preservation_bonus = min(20, sum(stats['patterns_preserved'].values()) * 2)
        else:
            preservation_bonus = 0
        
        return round(reduction_score + preservation_bonus, 2)
    
    def analyze_cleaning_impact_by_group(self, df, obs_column='obs', product_group_column='product_group'):
        """
        Analyze cleaning impact with group-specific insights
        """
        if product_group_column not in df.columns:
            return {'error': 'Product group column not found'}
        
        analysis = {
            'overall_stats': {},
            'group_analysis': {},
            'recommendations': []
        }
        
        # Overall statistics
        total_records = len(df)
        total_original_size = df[obs_column].astype(str).str.len().sum()
        total_cleaned_size = df[obs_column + '_cleaned'].astype(str).str.len().sum() if obs_column + '_cleaned' in df.columns else 0
        
        analysis['overall_stats'] = {
            'total_records': total_records,
            'total_original_mb': round(total_original_size / (1024 * 1024), 2),
            'total_cleaned_mb': round(total_cleaned_size / (1024 * 1024), 2),
            'overall_reduction_percent': round(100 * (1 - total_cleaned_size / total_original_size), 2) if total_original_size > 0 else 0,
            'space_saved_mb': round((total_original_size - total_cleaned_size) / (1024 * 1024), 2)
        }
        
        # Group-specific analysis
        analysis['group_analysis'] = self.get_group_cleaning_summary(df, obs_column, product_group_column)
        
        # Generate recommendations
        for group_name, group_stats in analysis['group_analysis'].items():
            if group_stats['reduction_percent'] < 10:
                analysis['recommendations'].append({
                    'group': group_stats['group_display_name'],
                    'type': 'optimization',
                    'message': f"Low cleaning impact ({group_stats['reduction_percent']}%). Consider reviewing cleaning rules for {group_stats['group_display_name']}."
                })
            
            if group_stats['patterns_preserved'] > group_stats['patterns_removed']:
                analysis['recommendations'].append({
                    'group': group_stats['group_display_name'],
                    'type': 'success',
                    'message': f"Excellent preservation of mandatory patterns in {group_stats['group_display_name']}."
                })
        
        return analysis
    
    def preview_cleaning_impact(self, text_sample, product_group=None):
        """
        Preview cleaning impact on a text sample
        """
        if not text_sample:
            return {'error': 'No text provided'}
        
        original_text = str(text_sample)
        cleaned_text = self.clean_text_by_group(original_text, product_group)
        
        # Get detailed stats
        stats = self.get_cleaning_stats(original_text, cleaned_text)
        
        # Identify what was preserved
        if product_group:
            cleaning_rules = self.group_manager.get_cleaning_rules(product_group)
            preserve_patterns = cleaning_rules.get('preserve_patterns', [])
            preserved_segments = self._identify_preservation_segments(original_text, preserve_patterns, product_group)
            
            preserved_items = [seg['text'] for seg in preserved_segments]
        else:
            preserved_items = []
        
        return {
            'product_group': self.group_manager.get_group_display_name(product_group) if product_group else 'Generic',
            'original_text': original_text[:500] + '...' if len(original_text) > 500 else original_text,
            'cleaned_text': cleaned_text[:500] + '...' if len(cleaned_text) > 500 else cleaned_text,
            'stats': stats,
            'preserved_patterns': preserved_items[:10],  # Show first 10 preserved items
            'mandatory_fields': self.group_manager.get_mandatory_fields(product_group) if product_group else [],
            'cleaning_rules_applied': len(self.group_manager.get_cleaning_rules(product_group)) if product_group else 0
        }


# Backward compatibility
EnhancedTelecomTextCleaner = GroupBasedTextCleaner