"""
Group-Based Text Extractor - Refactored
Extracts fields based on product group mandatory field requirements
"""

import re
import pandas as pd
from collections import defaultdict
from datetime import datetime

class GroupBasedTextExtractor:
    """
    Enhanced text extractor focused on product group mandatory fields
    Prioritizes extraction based on business criticality per group
    """
    
    def __init__(self, product_group_manager):
        self.group_manager = product_group_manager
        
        # Enhanced field patterns with improved accuracy
        self.field_patterns = {
            # Service and Ticket Information
            'ticket_number': [
                r'#(\d{8}-\d+)',
                r'ticket[:\s]*(\d{8}-\d+)',
                r'chamado[:\s]*(\d{8}-\d+)'
            ],
            
            # Client Classification (Critical for business logic)
            'client_type': [
                r'(?:cliente|tipo)[:\s]*(RESIDENCIAL|EMPRESARIAL|CORPORATIVO)',
                r'\b(RESIDENCIAL|EMPRESARIAL|CORPORATIVO)\b',
                r'(?:type|category)[:\s]*(residential|business|corporate)'
            ],
            
            # Technology Identification (Critical for service classification)
            'technology_id': [
                r'\b(GPON|EPON|P2P|ETHERNET|MPLS)\b',
                r'(?:tecnologia|tech)[:\s]*([A-Z0-9]{3,})',
                r'([A-Z]{3,}\d*/[A-Z]{2,}/\d+/\d+)',
                r'(?:service|serviço)[:\s]*([A-Z]{3,})'
            ],
            
            # Network Infrastructure (Critical for connectivity)
            'ip_management': [
                r'IP\s*CPE[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'IP\s*(?:management|gerencia)[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'endereco[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})'
            ],
            
            'ip_block': [
                r'BLOCO\s*IP[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'rede\s*cliente\s*v4[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'subnet[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})'
            ],
            
            'gateway': [
                r'GTW[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'gateway[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'gw[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})'
            ],
            
            'prefixes': [
                r'prefixo[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'prefix[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'anuncio[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})'
            ],
            
            # VLAN Configuration (Critical for network segmentation)
            'vlan': [
                r'VLAN\s*:?\s*(\d+)',
                r'vlan[:\s]*(\d+)',
                r'Vlan[:\s]*(\d+)',
                r'V-LAN[:\s]*(\d+)'
            ],
            
            # Equipment Information (Critical for support)
            'cpe': [
                r'CPE[:\s]*([A-Za-z0-9-]+)',
                r'equipamento[:\s]*([A-Za-z0-9-]+)',
                r'(OLT-[A-Z0-9-]+)',
                r'device[:\s]*([A-Za-z0-9-]+)'
            ],
            
            'serial_code': [
                r'SN[:\s]*([A-Za-z0-9]+)',
                r'serial\s*number[:\s=]*([A-Za-z0-9]+)',
                r'# serial number = ([A-Za-z0-9]+)',
                r'serie[:\s]*([A-Za-z0-9]+)',
                r'S/N[:\s]*([A-Za-z0-9]+)'
            ],
            
            'model_onu': [
                r'modelo\s*onu[:\s]*([A-Za-z0-9-]+)',
                r'onu\s*model[:\s]*([A-Za-z0-9-]+)',
                r'ONU[:\s]*([A-Za-z0-9-]+)',
                r'model[:\s]*([A-Za-z0-9-]+)'
            ],
            
            # Interface and Connectivity (Critical for L2L and enterprise)
            'interface_1': [
                r'interface\s*([\w\d/-]+)',
                r'porta[:\s]*([\w\d/-]+)',
                r'port[:\s]*([\w\d/-]+)',
                r'ethernet\s*([0-9/]+)',
                r'([A-Z]{4,}\d+/Frame\d+/Slot\d+/Port\d+)'
            ],
            
            # ASN and Provider Information (Critical for BGP/transit)
            'asn': [
                r'AS\s*Cliente[:\s]*(\d+)',
                r'ASN[:\s]*(\d+)',
                r'as-number[:\s]*(\d+)',
                r'remote-as[:\s]*(\d+)',
                r'provider[:\s]*AS[:\s]*(\d+)'
            ],
            
            'provider_id': [
                r'AS\s*Cliente[:\s]*(\d+)',
                r'provider[:\s]*(\d+)',
                r'fornecedor[:\s]*(\d+)'
            ],
            
            # Service and POP Information (Critical for network topology)
            'service_code': [
                r'([A-Z]{3,}\d*/[A-Z]{2,}/\d+/\d+)',
                r'(RIOS|SDI\d|FLA\d)/[A-Z]+/\d+/\d+',
                r'([A-Z]{3,}/[A-Z]{2,}/\d+/\d+-[A-Z-]+)',
                r'service[:\s]*([A-Z0-9-]+)'
            ],
            
            'pop_description': [
                r'(br\.[a-z]{2}\.[a-z]{2,}\.[\w.]+\.pe\.\d+)',
                r'OLT-([A-Z]{2}-[A-Z-]+\d+)',
                r'BR-([A-Z]{2}-[A-Z-]+-[A-Z-]+)',
                r'POP[:\s]*([A-Z]{2,}[-\w]*)'
            ],
            
            # WiFi Information (Critical for residential broadband)
            'wifi_ssid': [
                r'SSID[:\s]*([A-Za-z0-9_-]+)',
                r'ssid[:\s]*([A-Za-z0-9_-]+)',
                r'rede[:\s]*([A-Za-z0-9_-]+)',
                r'wifi[:\s]*name[:\s]*([A-Za-z0-9_-]+)'
            ],
            
            'wifi_passcode': [
                r'password[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
                r'passcode[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
                r'senha[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
                r'wifi[:\s]*pass[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)'
            ],
            
            'login_pppoe': [
                r'login\s*pppoe[:\s]*([A-Za-z0-9@._-]+)',
                r'usuario\s*pppoe[:\s]*([A-Za-z0-9@._-]+)',
                r'pppoe[:\s]*([A-Za-z0-9@._-]+)',
                r'user[:\s]*([A-Za-z0-9@._-]+)'
            ],
            
            # Physical Infrastructure
            'slot_number': [
                r'SLOT[:\s]*(\d+)',
                r'Slot(\d+)',
                r'slot[:\s]*(\d+)'
            ],
            
            'port_number': [
                r'PORT[:\s]*(\d+)',
                r'Port(\d+)',
                r'port[:\s]*(\d+)',
                r'porta[:\s]*(\d+)'
            ],
            
            'onu_id': [
                r'ONU[:\s]*(\d+)',
                r'OnuID(\d+)',
                r'onu[:\s]*(\d+)'
            ],
            
            # Optical Network Information (Critical for fiber services)
            'optical_power': [
                r'Rx\s*Optical\s*Power.*?=\s*([-\d.]+)',
                r'optical.*?power.*?([-\d.]+\s*dBm)',
                r'potencia[:\s]*([-\d.]+)',
                r'power[:\s]*([-\d.]+\s*dBm)'
            ],
            
            # MAC Addresses
            'mac_address': [
                r'([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})',
                r'([0-9A-Fa-f]{12})',
                r'MAC[:\s]*([0-9A-Fa-f:.-]{12,17})'
            ],
            
            # IPv6 Information
            'ipv6_block': [
                r'rede\s*cliente\s*v6[:\s]*([0-9a-fA-F:]+::/\d{1,3})',
                r'ipv6[:\s]*([0-9a-fA-F:]+::/\d{1,3})'
            ]
        }
        
        # Extraction statistics per group
        self.extraction_stats = defaultdict(lambda: {
            'total_processed': 0,
            'successful_extractions': defaultdict(int),
            'failed_extractions': defaultdict(int),
            'mandatory_coverage': defaultdict(float)
        })
    
    def extract_mandatory_fields_only(self, text, product_group):
        """
        Extract ONLY mandatory fields for a specific product group
        Optimized for speed and accuracy on critical fields
        """
        if not product_group or not self.group_manager.is_valid_group(product_group):
            return {}
        
        mandatory_fields = self.group_manager.get_mandatory_fields(product_group)
        extraction_priorities = self.group_manager.get_extraction_priority(product_group)
        
        results = {}
        
        # Process mandatory fields by priority
        sorted_fields = sorted(mandatory_fields, 
                             key=lambda f: extraction_priorities.get(f, 5), 
                             reverse=True)
        
        for field in sorted_fields:
            if field in self.field_patterns:
                extracted_value = self._extract_field_with_validation(
                    text, field, priority=extraction_priorities.get(field, 5)
                )
                results[field] = extracted_value
                
                # Update stats
                if extracted_value:
                    self.extraction_stats[product_group]['successful_extractions'][field] += 1
                else:
                    self.extraction_stats[product_group]['failed_extractions'][field] += 1
            else:
                results[field] = None
        
        self.extraction_stats[product_group]['total_processed'] += 1
        return results
    
    def extract_all_fields_by_group(self, text, product_group=None):
        """
        Extract all fields but prioritize mandatory fields for the group
        """
        results = {}
        
        if product_group and self.group_manager.is_valid_group(product_group):
            # First extract mandatory fields with high priority
            mandatory_fields = self.group_manager.get_mandatory_fields(product_group)
            optional_fields = self.group_manager.get_optional_fields(product_group)
            extraction_priorities = self.group_manager.get_extraction_priority(product_group)
            
            # Process mandatory fields first
            for field in mandatory_fields:
                if field in self.field_patterns:
                    results[field] = self._extract_field_with_validation(
                        text, field, priority=extraction_priorities.get(field, 8)
                    )
            
            # Then process optional fields
            for field in optional_fields:
                if field in self.field_patterns and field not in results:
                    results[field] = self._extract_field_with_validation(
                        text, field, priority=extraction_priorities.get(field, 3)
                    )
            
            # Fill in any missing fields from the full pattern list
            for field in self.field_patterns:
                if field not in results:
                    results[field] = self._extract_field_with_validation(
                        text, field, priority=1
                    )
        else:
            # Generic extraction for unknown/invalid groups
            for field in self.field_patterns:
                results[field] = self._extract_field_with_validation(text, field, priority=5)
        
        return results
    
    def extract_dataframe_by_groups(self, df, obs_column='obs', product_group_column='product_group'):
        """
        Extract fields from DataFrame with group-optimized processing
        """
        if obs_column not in df.columns:
            raise ValueError(f"Column '{obs_column}' not found in DataFrame")
        
        if product_group_column not in df.columns:
            print(f"⚠️ Column '{product_group_column}' not found. Using generic extraction.")
            return self._extract_dataframe_generic(df, obs_column)
        
        # Process by product group for optimized extraction
        def extract_by_group(group):
            product_group = group[product_group_column].iloc[0] if len(group) > 0 else None
            group_name = self.group_manager.get_group_display_name(product_group)
            
            print(f"🔍 Extracting fields for group: {group_name} ({len(group)} records)")
            
            if product_group and self.group_manager.is_valid_group(product_group):
                # Group-specific extraction focusing on mandatory fields
                mandatory_fields = self.group_manager.get_mandatory_fields(product_group)
                all_group_fields = self.group_manager.get_all_fields(product_group)
                
                print(f"   📋 Mandatory fields: {len(mandatory_fields)}, Total fields: {len(all_group_fields)}")
                
                # Extract for each record in the group
                for idx, row in group.iterrows():
                    text = row[obs_column]
                    
                    if pd.isna(text) or not isinstance(text, str):
                        # Set all fields to None for empty text
                        for field in all_group_fields:
                            extracted_field = f"extracted_{field}"
                            group.loc[idx, extracted_field] = None
                        continue
                    
                    # Extract mandatory fields first (high priority)
                    mandatory_extractions = self.extract_mandatory_fields_only(text, product_group)
                    
                    # Set mandatory field values
                    for field, value in mandatory_extractions.items():
                        extracted_field = f"extracted_{field}"
                        group.loc[idx, extracted_field] = value
                    
                    # Extract optional fields (lower priority)
                    optional_fields = self.group_manager.get_optional_fields(product_group)
                    for field in optional_fields:
                        if field in self.field_patterns:
                            extracted_field = f"extracted_{field}"
                            if extracted_field not in group.columns or pd.isna(group.loc[idx, extracted_field]):
                                value = self._extract_field_with_validation(text, field, priority=3)
                                group.loc[idx, extracted_field] = value
                
                # Calculate group extraction summary
                self._calculate_group_extraction_summary(group, product_group, mandatory_fields)
                
            else:
                # Generic extraction for invalid groups
                group = self._extract_group_generic(group, obs_column)
            
            return group
        
        # Group by product group and apply extraction
        processed_df = df.groupby(product_group_column, group_keys=False).apply(extract_by_group)
        
        return processed_df
    
    def _extract_field_with_validation(self, text, field, priority=5):
        """
        Extract a specific field with validation and confidence scoring
        """
        if field not in self.field_patterns:
            return None
        
        patterns = self.field_patterns[field]
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
                            confidence = self._calculate_field_confidence(
                                pattern, clean_match, field, priority
                            )
                            
                            if confidence > best_confidence:
                                best_match = clean_match
                                best_confidence = confidence
                                
            except re.error as e:
                print(f"⚠️ Regex error in field '{field}' with pattern '{pattern}': {e}")
                continue
        
        if best_match and best_confidence > 0.4:  # Confidence threshold
            return self._clean_and_validate_field_value(best_match, field)
        
        return None
    
    def _calculate_field_confidence(self, pattern, match, field, priority=5):
        """
        Calculate confidence score for field extraction with business priority
        """
        confidence = 0.5  # Base confidence
        
        # Priority boost for mandatory fields
        confidence += (priority / 10) * 0.3
        
        # Field-specific validation and confidence boosts
        if field == 'vlan' and match.isdigit():
            vlan_num = int(match)
            if 1 <= vlan_num <= 4094:
                confidence += 0.4
        
        elif field in ['ip_management', 'gateway', 'ip_block']:
            # Validate IP format
            ip_part = match.split('/')[0]
            try:
                parts = ip_part.split('.')
                if len(parts) == 4 and all(0 <= int(p) <= 255 for p in parts):
                    confidence += 0.4
                    # Bonus for private IP ranges (more likely to be correct)
                    first_octet = int(parts[0])
                    if first_octet in [10, 172, 192]:
                        confidence += 0.1
            except (ValueError, AttributeError):
                confidence -= 0.3
        
        elif field in ['asn', 'provider_id'] and match.isdigit():
            try:
                asn_num = int(match)
                if 1 <= asn_num <= 4294967295:
                    confidence += 0.3
                    # Bonus for typical ISP ASN ranges
                    if 64512 <= asn_num <= 65534 or asn_num < 64512:
                        confidence += 0.1
            except ValueError:
                confidence -= 0.3
        
        elif field == 'serial_code':
            # Validate serial number format
            if len(match) >= 6 and any(c.isdigit() for c in match) and any(c.isalpha() for c in match):
                confidence += 0.3
        
        elif field == 'client_type':
            valid_types = ['RESIDENCIAL', 'EMPRESARIAL', 'CORPORATIVO']
            if match.upper() in valid_types:
                confidence += 0.4
        
        elif field == 'technology_id':
            valid_techs = ['GPON', 'EPON', 'ETHERNET', 'MPLS', 'P2P']
            if any(tech in match.upper() for tech in valid_techs):
                confidence += 0.4
        
        elif field in ['wifi_ssid', 'wifi_passcode'] and len(match) >= 4:
            confidence += 0.3
            # Bonus for reasonable WiFi names/passwords
            if 8 <= len(match) <= 32:
                confidence += 0.1
        
        elif field == 'optical_power':
            try:
                power = float(re.findall(r'[-\d.]+', match)[0])
                if -50 <= power <= 10:  # Reasonable optical power range
                    confidence += 0.4
            except (ValueError, IndexError):
                confidence -= 0.2
        
        elif field == 'mac_address':
            # Validate MAC address format
            if re.match(r'^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}', match):
                confidence += 0.4
        
        # Pattern specificity bonus
        if len(pattern) > 30:  # More specific patterns get bonus
            confidence += 0.1
        
        # Context bonus - if field appears with expected keywords
        context_keywords = {
            'serial_code': ['serial', 'SN', 'S/N'],
            'vlan': ['VLAN', 'vlan'],
            'ip_management': ['IP', 'CPE', 'management'],
            'wifi_ssid': ['SSID', 'ssid', 'network'],
            'client_type': ['cliente', 'type', 'category']
        }
        
        if field in context_keywords:
            for keyword in context_keywords[field]:
                if keyword.lower() in pattern.lower():
                    confidence += 0.05
        
        return min(1.0, max(0, confidence))
    
    def _clean_and_validate_field_value(self, value, field):
        """
        Clean and normalize extracted field values with business logic
        """
        if not value:
            return None
        
        value = str(value).strip()
        
        # Field-specific cleaning and validation
        if field in ['vlan', 'slot_number', 'port_number', 'onu_id']:
            # Extract numeric value and validate range
            num_match = re.search(r'\b(\d+)\b', value)
            if num_match:
                num = int(num_match.group(1))
                if field == 'vlan' and 1 <= num <= 4094:
                    return num
                elif field in ['slot_number', 'port_number'] and 0 <= num <= 1000:
                    return num
                elif field == 'onu_id' and 0 <= num <= 255:
                    return num
            return None
        
        elif field in ['asn', 'provider_id']:
            # Extract and validate ASN
            asn_match = re.search(r'\b(\d+)\b', value)
            if asn_match:
                asn = int(asn_match.group(1))
                if 1 <= asn <= 4294967295:
                    return asn
            return None
        
        elif field in ['ip_management', 'gateway']:
            # Validate and clean IP addresses
            ip_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', value)
            if ip_match:
                ip = ip_match.group()
                try:
                    parts = [int(p) for p in ip.split('.')]
                    if all(0 <= p <= 255 for p in parts):
                        return ip
                except ValueError:
                    pass
            return None
        
        elif field in ['ip_block', 'prefixes']:
            # Validate IP blocks with CIDR
            block_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}/\d{1,2}\b', value)
            if block_match:
                block = block_match.group()
                try:
                    ip_part, cidr = block.split('/')
                    parts = [int(p) for p in ip_part.split('.')]
                    cidr_num = int(cidr)
                    if all(0 <= p <= 255 for p in parts) and 0 <= cidr_num <= 32:
                        return block
                except ValueError:
                    pass
            return None
        
        elif field == 'mac_address':
            # Normalize MAC address format
            mac_clean = re.sub(r'[^0-9A-Fa-f]', '', value)
            if len(mac_clean) == 12:
                return ':'.join(mac_clean[i:i+2] for i in range(0, 12, 2)).upper()
            return value if re.match(r'^([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}', value) else None
        
        elif field == 'client_type':
            # Normalize client type
            value_upper = value.upper()
            valid_types = ['RESIDENCIAL', 'EMPRESARIAL', 'CORPORATIVO']
            for valid_type in valid_types:
                if valid_type in value_upper:
                    return valid_type
            return value.upper() if value.upper() in valid_types else None
        
        elif field == 'technology_id':
            # Normalize technology identifier
            value_upper = value.upper()
            known_techs = ['GPON', 'EPON', 'P2P', 'ETHERNET', 'MPLS']
            for tech in known_techs:
                if tech in value_upper:
                    return tech
            return value_upper
        
        elif field in ['wifi_ssid', 'wifi_passcode', 'login_pppoe']:
            # Clean credentials (remove quotes, trim)
            cleaned = re.sub(r'^["\']|["\']', '', value)
            return cleaned if 4 <= len(cleaned) <= 63 else None
        
        elif field in ['cpe', 'model_onu', 'service_code']:
            # Clean equipment identifiers
            return re.sub(r'[^\w\d-.]', '', value).upper()
        
        elif field == 'serial_code':
            # Clean serial numbers
            cleaned = re.sub(r'[^\w\d]', '', value).upper()
            return cleaned if len(cleaned) >= 6 else None
        
        elif field == 'optical_power':
            # Extract numeric value with unit
            power_match = re.search(r'([-\d.]+)', value)
            if power_match:
                try:
                    power_val = float(power_match.group(1))
                    if -50 <= power_val <= 10:
                        return f"{power_val} dBm"
                except ValueError:
                    pass
            return None
        
        elif field == 'interface_1':
            # Clean interface names
            cleaned = re.sub(r'[^\w\d/-]', '', value)
            return cleaned if len(cleaned) >= 2 else None
        
        return value
    
    def _extract_dataframe_generic(self, df, obs_column):
        """
        Generic extraction for DataFrames without product groups
        """
        extraction_results = []
        field_names = list(self.field_patterns.keys())
        
        for idx, row in df.iterrows():
            text = row[obs_column]
            if pd.isna(text) or not isinstance(text, str):
                result = {field: None for field in field_names}
            else:
                result = self.extract_all_fields_by_group(text, product_group=None)
            
            extraction_results.append(result)
        
        # Add extracted fields as new columns
        for field in field_names:
            df[f'extracted_{field}'] = [result.get(field) for result in extraction_results]
        
        return df
    
    def _extract_group_generic(self, group, obs_column):
        """
        Generic extraction for a group without specific product group
        """
        field_names = list(self.field_patterns.keys())
        
        for idx, row in group.iterrows():
            text = row[obs_column]
            if pd.isna(text) or not isinstance(text, str):
                for field in field_names:
                    group.loc[idx, f'extracted_{field}'] = None
            else:
                extractions = self.extract_all_fields_by_group(text, product_group=None)
                for field, value in extractions.items():
                    group.loc[idx, f'extracted_{field}'] = value
        
        return group
    
    def _calculate_group_extraction_summary(self, group_data, product_group, mandatory_fields):
        """
        Calculate extraction summary statistics for a product group
        """
        total_records = len(group_data)
        successful_mandatory = 0
        
        for field in mandatory_fields:
            extracted_field = f"extracted_{field}"
            if extracted_field in group_data.columns:
                filled_count = group_data[extracted_field].notna().sum()
                success_rate = (filled_count / total_records) * 100 if total_records > 0 else 0
                
                self.extraction_stats[product_group]['mandatory_coverage'][field] = success_rate
                if success_rate > 50:  # Consider >50% as successful
                    successful_mandatory += 1
        
        # Overall mandatory field coverage
        overall_coverage = (successful_mandatory / len(mandatory_fields)) * 100 if mandatory_fields else 0
        self.extraction_stats[product_group]['overall_mandatory_coverage'] = overall_coverage
        
        print(f"   ✅ Mandatory field coverage: {overall_coverage:.1f}% ({successful_mandatory}/{len(mandatory_fields)} fields)")
    
    def get_field_list(self):
        """Return list of all extractable fields"""
        return list(self.field_patterns.keys())
    
    def get_extraction_stats_by_group(self, df, obs_column='obs', product_group_column='product_group'):
        """
        Get comprehensive extraction statistics by product group
        """
        if product_group_column not in df.columns:
            return {'error': 'Product group column not found'}
        
        stats = {}
        
        for group_name in df[product_group_column].unique():
            if pd.isna(group_name) or not self.group_manager.is_valid_group(group_name):
                continue
                
            group_data = df[df[product_group_column] == group_name]
            mandatory_fields = self.group_manager.get_mandatory_fields(group_name)
            all_fields = self.group_manager.get_all_fields(group_name)
            
            group_stats = {
                'group_display_name': self.group_manager.get_group_display_name(group_name),
                'category': self.group_manager.get_group_category(group_name),
                'priority_level': self.group_manager.get_group_priority_level(group_name),
                'total_records': len(group_data),
                'mandatory_fields': mandatory_fields,
                'mandatory_extraction_rates': {},
                'optional_extraction_rates': {},
                'overall_mandatory_coverage': 0,
                'extraction_quality_score': 0
            }
            
            # Calculate mandatory field extraction rates
            mandatory_success = 0
            for field in mandatory_fields:
                extracted_field = f'extracted_{field}'
                
                if extracted_field in group_data.columns:
                    successful_extractions = group_data[extracted_field].notna().sum()
                    extraction_rate = (successful_extractions / len(group_data)) * 100
                    group_stats['mandatory_extraction_rates'][field] = {
                        'extraction_rate': round(extraction_rate, 2),
                        'successful_count': int(successful_extractions),
                        'total_count': len(group_data),
                        'status': 'excellent' if extraction_rate >= 80 else
                                 'good' if extraction_rate >= 60 else
                                 'needs_improvement' if extraction_rate >= 30 else 'critical'
                    }
                    
                    if extraction_rate >= 50:
                        mandatory_success += 1
                else:
                    group_stats['mandatory_extraction_rates'][field] = {
                        'extraction_rate': 0.0,
                        'successful_count': 0,
                        'total_count': len(group_data),
                        'status': 'missing'
                    }
            
            # Calculate optional field extraction rates
            optional_fields = self.group_manager.get_optional_fields(group_name)
            for field in optional_fields:
                extracted_field = f'extracted_{field}'
                
                if extracted_field in group_data.columns:
                    successful_extractions = group_data[extracted_field].notna().sum()
                    extraction_rate = (successful_extractions / len(group_data)) * 100
                    group_stats['optional_extraction_rates'][field] = {
                        'extraction_rate': round(extraction_rate, 2),
                        'successful_count': int(successful_extractions)
                    }
            
            # Calculate overall metrics
            group_stats['overall_mandatory_coverage'] = round(
                (mandatory_success / len(mandatory_fields)) * 100, 2
            ) if mandatory_fields else 0
            
            # Calculate extraction quality score
            total_rate = sum([stats['extraction_rate'] for stats in group_stats['mandatory_extraction_rates'].values()])
            avg_rate = total_rate / len(mandatory_fields) if mandatory_fields else 0
            
            # Quality score considers both coverage and accuracy
            coverage_score = (mandatory_success / len(mandatory_fields)) * 50 if mandatory_fields else 0
            accuracy_score = min(50, avg_rate / 2)
            group_stats['extraction_quality_score'] = round(coverage_score + accuracy_score, 2)
            
            stats[group_name] = group_stats
        
        return stats
    
    def analyze_extraction_patterns(self, text_samples, product_group=None):
        """
        Analyze extraction patterns to optimize field detection
        """
        if not text_samples:
            return {'error': 'No text samples provided'}
        
        analysis = {
            'product_group': self.group_manager.get_group_display_name(product_group) if product_group else 'Generic',
            'total_samples': len(text_samples),
            'field_detection_rates': {},
            'pattern_effectiveness': {},
            'recommendations': []
        }
        
        mandatory_fields = self.group_manager.get_mandatory_fields(product_group) if product_group else []
        
        # Test each field pattern against samples
        for field in self.field_patterns:
            detection_count = 0
            pattern_hits = defaultdict(int)
            
            for sample in text_samples[:100]:  # Limit to 100 samples for analysis
                if not isinstance(sample, str):
                    continue
                
                extracted = self._extract_field_with_validation(sample, field, priority=5)
                if extracted:
                    detection_count += 1
                
                # Track which patterns are hitting
                for pattern in self.field_patterns[field]:
                    try:
                        if re.search(pattern, sample, re.IGNORECASE):
                            pattern_hits[pattern] += 1
                    except re.error:
                        continue
            
            detection_rate = (detection_count / len(text_samples)) * 100
            analysis['field_detection_rates'][field] = {
                'detection_rate': round(detection_rate, 2),
                'is_mandatory': field in mandatory_fields,
                'pattern_count': len(self.field_patterns[field]),
                'most_effective_patterns': sorted(pattern_hits.items(), key=lambda x: x[1], reverse=True)[:3]
            }
        
        # Generate recommendations
        for field, stats in analysis['field_detection_rates'].items():
            if field in mandatory_fields and stats['detection_rate'] < 50:
                analysis['recommendations'].append({
                    'type': 'critical',
                    'field': field,
                    'message': f"Mandatory field '{field}' has low detection rate ({stats['detection_rate']:.1f}%). Consider improving patterns."
                })
            elif stats['detection_rate'] == 0 and field in mandatory_fields:
                analysis['recommendations'].append({
                    'type': 'urgent',
                    'field': field,
                    'message': f"Mandatory field '{field}' has zero detection. Patterns may need complete revision."
                })
        
        return analysis


# Backward compatibility
EnhancedTelecomTextExtractor = GroupBasedTextExtractor