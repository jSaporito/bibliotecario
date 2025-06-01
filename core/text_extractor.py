import re
import pandas as pd
from datetime import datetime
from core.product_groups import product_group_manager

class EnhancedTelecomTextExtractor:
    """
    Enhanced text extractor for telecom/network infrastructure data
    Now supports product group-specific extraction with mandatory field focus
    """
    
    def __init__(self):
        self.product_group_manager = product_group_manager
        
        # Enhanced patterns mapped to business field names
        self.field_patterns = {
            # Service and Ticket Information
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
            
            # Client and Service Classification
            'client_type': [
                r'cliente[:\s]*(residencial|empresarial|corporativo)',
                r'tipo[:\s]*(residencial|empresarial|corporativo)',
                r'(RESIDENCIAL|EMPRESARIAL|CORPORATIVO)',
            ],
            
            'technology_id': [
                r'tecnologia[:\s]*([A-Z0-9]{3,})',
                r'tech[:\s]*([A-Z0-9]{3,})',
                r'([A-Z]{3,}\d*/[A-Z]{2,}/\d+/\d+)',
                r'(GPON|EPON|P2P|ETHERNET|MPLS)',
            ],
            
            'provider_id': [
                r'provedor[:\s]*(\d+)',
                r'provider[:\s]*(\d+)',
                r'AS\s*Cliente[:\s]*(\d+)',
            ],
            
            # Network Infrastructure
            'service_code': [
                r'([A-Z]{3,}\d*/[A-Z]{2,}/\d+/\d+)',
                r'(RIOS|SDI\d|FLA\d)/[A-Z]+/\d+/\d+',
                r'([A-Z]{3,}/[A-Z]{2,}/\d+/\d+-[A-Z-]+)',
            ],
            
            'pop_description': [
                r'br\.([a-z]{2}\.[a-z]{2,}\.[\w.]+\.pe\.\d+)',
                r'OLT-([A-Z]{2}-[A-Z-]+\d+)',
                r'BR-([A-Z]{2}-[A-Z-]+-[A-Z-]+)',
                r'POP[:\s]*([A-Z]{2,}[-\w]*)',
            ],
            
            # IP Address Management (Critical for most groups)
            'ip_management': [
                r'IP\s*CPE[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'IP[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'endereco[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            'gateway': [
                r'GTW[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'gateway[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
                r'gw[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            ],
            
            'ip_block': [
                r'BLOCO\s*IP[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'rede\s*cliente\s*v4[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'subnet[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
            ],
            
            'prefixes': [
                r'prefixo[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'prefix[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
                r'anuncio[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d{1,2})',
            ],
            
            # VLAN Configuration (Critical for most groups)
            'vlan': [
                r'VLAN\s*:?\s*(\d+)',
                r'vlan[:\s]*(\d+)',
                r'Vlan[:\s]*(\d+)',
            ],
            
            # Equipment Information
            'cpe': [
                r'CPE[:\s]*([A-Za-z0-9-]+)',
                r'equipamento[:\s]*([A-Za-z0-9-]+)',
                r'modelo[:\s]*([A-Za-z0-9-]+)',
                r'(OLT-[A-Z0-9-]+)',
            ],
            
            'serial_code': [
                r'SN[:\s]*([A-Za-z0-9]+)',
                r'serial\s*number[:\s=]*([A-Za-z0-9]+)',
                r'# serial number = ([A-Za-z0-9]+)',
                r'serie[:\s]*([A-Za-z0-9]+)',
            ],
            
            'model_onu': [
                r'modelo\s*onu[:\s]*([A-Za-z0-9-]+)',
                r'onu\s*model[:\s]*([A-Za-z0-9-]+)',
                r'ONU[:\s]*([A-Za-z0-9-]+)',
            ],
            
            # Interface and Connectivity
            'interface_1': [
                r'interface\s*([\w\d/-]+)',
                r'porta[:\s]*([\w\d/-]+)',
                r'port[:\s]*([\w\d/-]+)',
                r'ethernet\s*([0-9/]+)',
            ],
            
            # ASN and BGP Information (Critical for internet services)
            'asn': [
                r'AS\s*Cliente[:\s]*(\d+)',
                r'ASN[:\s]*(\d+)',
                r'as-number[:\s]*(\d+)',
                r'remote-as[:\s]*(\d+)',
            ],
            
            # WiFi Information (Critical for broadband)
            'wifi_ssid': [
                r'SSID[:\s]*([A-Za-z0-9_-]+)',
                r'ssid[:\s]*([A-Za-z0-9_-]+)',
                r'rede[:\s]*([A-Za-z0-9_-]+)',
            ],
            
            'wifi_passcode': [
                r'password[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
                r'passcode[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
                r'senha[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
            ],
            
            'login_pppoe': [
                r'login\s*pppoe[:\s]*([A-Za-z0-9@._-]+)',
                r'usuario\s*pppoe[:\s]*([A-Za-z0-9@._-]+)',
                r'pppoe[:\s]*([A-Za-z0-9@._-]+)',
            ],
            
            # Additional network fields
            'slot_number': [
                r'SLOT[:\s]*(\d+)',
                r'Slot(\d+)',
                r'slot[:\s]*(\d+)',
            ],
            
            'port_number': [
                r'PORT[:\s]*(\d+)',
                r'Port(\d+)',
                r'port[:\s]*(\d+)',
                r'porta[:\s]*(\d+)',
            ],
            
            'onu_id': [
                r'ONU[:\s]*(\d+)',
                r'OnuID(\d+)',
                r'onu[:\s]*(\d+)',
            ],
            
            # Optical Network Information
            'optical_power': [
                r'Rx\s*Optical\s*Power.*?=\s*([-\d.]+)',
                r'optical.*?power.*?([-\d.]+\s*dBm)',
                r'potencia[:\s]*([-\d.]+)',
            ],
            
            # MAC Addresses
            'mac_address': [
                r'([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})',
                r'([0-9A-Fa-f]{12})',
                r'MAC[:\s]*([0-9A-Fa-f:.-]{12,17})',
            ],
            
            # IPv6 Information
            'ipv6_block': [
                r'rede\s*cliente\s*v6[:\s]*([0-9a-fA-F:]+::/\d{1,3})',
                r'ipv6[:\s]*([0-9a-fA-F:]+::/\d{1,3})',
            ],
        }
        
        # Priority weights for mandatory fields by product group
        self.field_priorities = {
            "bandalarga_broadband_fiber_plans": {
                'serial_code': 10,
                'model_onu': 9,
                'wifi_ssid': 8,
                'wifi_passcode': 8,
                'vlan': 7,
                'login_pppoe': 6,
            },
            "linkdeinternet_dedicated_internet_connectivity": {
                'client_type': 10,
                'technology_id': 9,
                'cpe': 8,
                'ip_management': 8,
                'vlan': 7,
                'ip_block': 7,
                'pop_description': 6,
                'interface_1': 6,
            },
            "linkdeinternet_gpon_semi_dedicated_connections": {
                'client_type': 10,
                'technology_id': 9,
                'provider_id': 8,
                'cpe': 8,
                'vlan': 7,
                'serial_code': 7,
                'pop_description': 6,
            },
            "linkdeinternet_direct_l2l_links": {
                'client_type': 10,
                'technology_id': 9,
                'provider_id': 8,
                'pop_description': 8,
                'cpe': 7,
                'interface_1': 7,
                'vlan': 6,
            },
            "linkdeinternet_mpls_data_transport_services": {
                'client_type': 10,
                'technology_id': 9,
                'provider_id': 8,
                'pop_description': 8,
                'interface_1': 7,
                'vlan': 7,
                'cpe': 6,
                'ip_management': 6,
            },
            "linkdeinternet_lan_to_lan_infrastructure": {
                'client_type': 10,
                'technology_id': 9,
                'provider_id': 8,
                'pop_description': 8,
                'interface_1': 7,
                'cpe': 7,
                'ip_management': 6,
                'vlan': 6,
            },
            "linkdeinternet_ip_transit_services": {
                'client_type': 10,
                'technology_id': 9,
                'provider_id': 8,
                'pop_description': 8,
                'interface_1': 7,
                'gateway': 7,
                'asn': 7,
                'vlan': 6,
                'prefixes': 6,
            },
            "linkdeinternet_traffic_exchange_ptt": {
                'client_type': 10,
                'technology_id': 9,
                'provider_id': 8,
                'pop_description': 8,
                'interface_1': 7,
                'vlan': 6,
            },
            "linkdeinternet_enterprise_gpon_lan": {
                'client_type': 10,
                'technology_id': 9,
                'provider_id': 8,
                'pop_description': 8,
                'serial_code': 7,
                'cpe': 7,
                'ip_management': 6,
                'vlan': 6,
            }
        }
    
    def extract_all_fields(self, text, product_group=None):
        """
        Extract all available fields from telecom configuration text
        Prioritizes mandatory fields for the specific product group
        """
        result = {field: None for field in self.field_patterns.keys()}
        
        if pd.isna(text) or not isinstance(text, str):
            return result
        
        # Get mandatory fields for this product group
        mandatory_fields = self.product_group_manager.get_mandatory_fields(product_group) if product_group else []
        field_mapping = self.product_group_manager.get_extracted_field_mapping(product_group) if product_group else {}
        
        # Get field priorities for this group
        priorities = self.field_priorities.get(product_group, {})
        
        # Process mandatory fields first with higher priority
        for business_field in mandatory_fields:
            extracted_field = self._map_business_to_extracted_field(business_field, field_mapping)
            if extracted_field and extracted_field in self.field_patterns:
                best_match = self._extract_field_with_priority(
                    text, extracted_field, priority=priorities.get(business_field, 5)
                )
                if best_match:
                    result[extracted_field] = best_match
        
        # Process remaining fields
        for field, patterns in self.field_patterns.items():
            if result[field] is not None:  # Already extracted as mandatory
                continue
                
            best_match = self._extract_field_with_priority(text, field, priority=1)
            if best_match:
                result[field] = best_match
        
        return result
    
    def extract_mandatory_fields_only(self, text, product_group):
        """
        Extract only mandatory fields for a specific product group
        Returns faster results with focused extraction
        """
        if not product_group:
            return self.extract_all_fields(text)
        
        mandatory_fields = self.product_group_manager.get_mandatory_fields(product_group)
        field_mapping = self.product_group_manager.get_extracted_field_mapping(product_group)
        
        result = {}
        
        for business_field in mandatory_fields:
            extracted_field = self._map_business_to_extracted_field(business_field, field_mapping)
            if extracted_field and extracted_field in self.field_patterns:
                best_match = self._extract_field_with_priority(
                    text, extracted_field, priority=10  # High priority for mandatory
                )
                result[business_field] = best_match
            else:
                result[business_field] = None
        
        return result
    
    def extract_dataframe_by_groups(self, df, obs_column='obs', product_group_column='product_group'):
        """
        Extract fields from DataFrame grouped by product classification
        More efficient processing with group-specific extraction
        """
        if obs_column not in df.columns:
            raise ValueError(f"Column '{obs_column}' not found in DataFrame")
        
        if product_group_column not in df.columns:
            print(f"⚠️ Column '{product_group_column}' not found. Using generic extraction.")
            # Fallback to generic extraction
            all_extractions = []
            for _, row in df.iterrows():
                extraction = self.extract_all_fields(row[obs_column])
                all_extractions.append(extraction)
            
            # Add extracted columns
            for field in self.field_patterns.keys():
                df[f'extracted_{field}'] = [ext[field] for ext in all_extractions]
            
            return df
        
        # Process by product group for efficiency
        def extract_by_group(group):
            product_group = group[product_group_column].iloc[0] if len(group) > 0 else None
            
            # Get mandatory fields for this group
            mandatory_fields = self.product_group_manager.get_mandatory_fields(product_group)
            
            # Extract fields for each row in the group
            for idx, row in group.iterrows():
                text = row[obs_column]
                
                if mandatory_fields:
                    # Focus on mandatory fields first
                    mandatory_extractions = self.extract_mandatory_fields_only(text, product_group)
                    
                    # Also extract all fields for completeness
                    all_extractions = self.extract_all_fields(text, product_group)
                    
                    # Merge results, prioritizing mandatory field extractions
                    for business_field, value in mandatory_extractions.items():
                        extracted_field = self._get_extracted_field_name(business_field, product_group)
                        if extracted_field:
                            group.loc[idx, f'extracted_{extracted_field}'] = value
                    
                    # Add other extracted fields
                    for field, value in all_extractions.items():
                        if f'extracted_{field}' not in group.columns:
                            group.loc[idx, f'extracted_{field}'] = value
                else:
                    # Generic extraction for unknown groups
                    extractions = self.extract_all_fields(text, product_group)
                    for field, value in extractions.items():
                        group.loc[idx, f'extracted_{field}'] = value
            
            return group
        
        # Group by product group and apply extraction
        processed_df = df.groupby(product_group_column, group_keys=False).apply(extract_by_group)
        
        return processed_df
    
    def _map_business_to_extracted_field(self, business_field, field_mapping):
        """Map business field name to extracted field name"""
        if business_field in field_mapping:
            # Remove 'extracted_' prefix if present
            mapped_field = field_mapping[business_field]
            return mapped_field.replace('extracted_', '') if mapped_field.startswith('extracted_') else mapped_field
        
        # Direct mapping if field exists in patterns
        if business_field in self.field_patterns:
            return business_field
        
        # Try to find similar field
        for field in self.field_patterns.keys():
            if business_field.replace('_', '') in field.replace('_', ''):
                return field
        
        return None
    
    def _get_extracted_field_name(self, business_field, product_group):
        """Get the extracted field name for a business field"""
        field_mapping = self.product_group_manager.get_extracted_field_mapping(product_group) if product_group else {}
        
        if business_field in field_mapping:
            mapped_field = field_mapping[business_field]
            return mapped_field.replace('extracted_', '') if mapped_field.startswith('extracted_') else mapped_field
        
        return business_field
    
    def _extract_field_with_priority(self, text, field, priority=5):
        """Extract a specific field with priority-based confidence"""
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
                            confidence = self._calculate_confidence(pattern, clean_match, field, priority)
                            
                            if confidence > best_confidence:
                                best_match = clean_match
                                best_confidence = confidence
                                
            except re.error as e:
                print(f"Regex error in field '{field}' with pattern '{pattern}': {e}")
                continue
        
        if best_match and best_confidence > 0.3:
            return self._clean_field_value(best_match, field)
        
        return None
    
    def _calculate_confidence(self, pattern, match, field, priority=5):
        """
        Calculate confidence score for field extraction with priority weighting
        """
        confidence = 0.5  # Base confidence
        
        # Priority boost (mandatory fields get higher confidence)
        confidence += (priority / 10) * 0.2
        
        # Field-specific validation and confidence boosts
        if field == 'vlan' and match.isdigit():
            vlan_num = int(match)
            if 1 <= vlan_num <= 4094:
                confidence += 0.3
        
        elif field in ['ip_management', 'gateway', 'ip_block']:
            # Validate IP format
            ip_part = match.split('/')[0]
            try:
                parts = ip_part.split('.')
                if len(parts) == 4 and all(0 <= int(p) <= 255 for p in parts):
                    confidence += 0.3
            except (ValueError, AttributeError):
                confidence -= 0.2
        
        elif field in ['asn', 'provider_id'] and match.isdigit():
            try:
                asn_num = int(match)
                if 1 <= asn_num <= 4294967295:
                    confidence += 0.3
            except ValueError:
                confidence -= 0.2
        
        elif field == 'serial_code' and len(match) >= 6:
            confidence += 0.2
        
        elif field == 'optical_power':
            try:
                power = float(re.findall(r'[-\d.]+', match)[0])
                if -50 <= power <= 10:  # Reasonable optical power range
                    confidence += 0.3
            except (ValueError, IndexError):
                confidence -= 0.1
        
        elif field in ['wifi_ssid', 'wifi_passcode'] and len(match) >= 4:
            confidence += 0.2
        
        elif field == 'technology_id' and any(tech in match.upper() for tech in ['GPON', 'EPON', 'ETHERNET', 'MPLS']):
            confidence += 0.3
        
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
        if field in ['vlan', 'slot_number', 'port_number', 'onu_id', 'plan_number']:
            # Extract numeric value
            num_match = re.search(r'\b(\d+)\b', value)
            return int(num_match.group(1)) if num_match else value
        
        elif field in ['asn', 'provider_id']:
            # Extract ASN number
            asn_match = re.search(r'\b(\d+)\b', value)
            return int(asn_match.group(1)) if asn_match else value
        
        elif field in ['ip_management', 'gateway']:
            # Clean IP addresses
            ip_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?:/\d{1,2})?\b', value)
            return ip_match.group() if ip_match else value
        
        elif field in ['ip_block', 'prefixes']:
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
        
        elif field in ['wifi_ssid', 'wifi_passcode', 'login_pppoe']:
            # Remove quotes and clean credentials
            return re.sub(r'^["\']|["\']
        , '', value)
        
        elif field == 'client_type':
            # Normalize client type
            value_lower = value.lower()
            if 'residencial' in value_lower:
                return 'RESIDENCIAL'
            elif 'empresarial' in value_lower:
                return 'EMPRESARIAL'
            elif 'corporativo' in value_lower:
                return 'CORPORATIVO'
            return value.upper()
        
        elif field in ['cpe', 'model_onu', 'technology_id']:
            # Clean equipment and technology identifiers
            return re.sub(r'[^\w\d-.]', '', value).upper()
        
        return value
    
    def get_field_list(self):
        """Return list of all extractable fields"""
        return list(self.field_patterns.keys())
    
    def get_mandatory_field_mapping(self, product_group):
        """Get mapping of mandatory fields for a product group"""
        if not product_group:
            return {}
        
        mandatory_fields = self.product_group_manager.get_mandatory_fields(product_group)
        field_mapping = self.product_group_manager.get_extracted_field_mapping(product_group)
        
        result = {}
        for business_field in mandatory_fields:
            extracted_field = self._map_business_to_extracted_field(business_field, field_mapping)
            if extracted_field:
                result[business_field] = extracted_field
        
        return result
    
    def get_extraction_stats_by_group(self, df, obs_column='obs', product_group_column='product_group'):
        """Get detailed extraction statistics by product group"""
        if product_group_column not in df.columns:
            return {}
        
        stats = {}
        
        for group_name in df[product_group_column].unique():
            if pd.isna(group_name):
                continue
                
            group_data = df[df[product_group_column] == group_name]
            mandatory_fields = self.product_group_manager.get_mandatory_fields(group_name)
            
            group_stats = {
                'group_display_name': self.product_group_manager.get_group_display_name(group_name),
                'total_records': len(group_data),
                'mandatory_fields': mandatory_fields,
                'mandatory_extraction_rates': {},
                'overall_extraction_rate': 0,
                'completeness_score': 0
            }
            
            # Calculate mandatory field extraction rates
            total_mandatory_extractions = 0
            for business_field in mandatory_fields:
                extracted_field = self._get_extracted_field_name(business_field, group_name)
                column_name = f'extracted_{extracted_field}'
                
                if column_name in group_data.columns:
                    successful_extractions = group_data[column_name].notna().sum()
                    extraction_rate = (successful_extractions / len(group_data)) * 100
                    group_stats['mandatory_extraction_rates'][business_field] = {
                        'extraction_rate': round(extraction_rate, 2),
                        'successful_count': int(successful_extractions),
                        'total_count': len(group_data)
                    }
                    total_mandatory_extractions += successful_extractions
                else:
                    group_stats['mandatory_extraction_rates'][business_field] = {
                        'extraction_rate': 0.0,
                        'successful_count': 0,
                        'total_count': len(group_data)
                    }
            
            # Calculate overall metrics
            if mandatory_fields:
                group_stats['overall_extraction_rate'] = round(
                    (total_mandatory_extractions / (len(group_data) * len(mandatory_fields))) * 100, 2
                )
                group_stats['completeness_score'] = round(
                    sum([stats['extraction_rate'] for stats in group_stats['mandatory_extraction_rates'].values()]) / len(mandatory_fields), 2
                )
            
            stats[group_name] = group_stats
        
        return stats
    
    # Backward compatibility methods
    def process_text(self, text):
        """Backward compatibility method"""
        return self.extract_all_fields(text)
    
    def get_extraction_stats(self, text):
        """Get detailed extraction statistics for a single text"""
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