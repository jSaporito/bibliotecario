"""
Product Group Manager - Refactored for Group-Based Processing
Manages all product group configurations, mandatory fields, and processing rules
"""

class ProductGroupManager:
    """
    Enhanced Product Group Manager focused on group-based data processing
    """
    
    def __init__(self):
        # Enhanced product group definitions with processing specifics
        self.product_groups = {
            "bandalarga_broadband_fiber_plans": {
                "name": "Banda Larga - Planos de Fibra Residencial",
                "category": "residential_broadband",
                "priority_level": "high",
                "mandatory_fields": [
                    "serial_code",      # Critical for fiber ONUs
                    "model_onu",        # Equipment identification
                    "wifi_ssid",        # Customer access
                    "wifi_passcode",    # Security
                    "vlan",             # Network segmentation
                    "login_pppoe"       # Authentication
                ],
                "optional_fields": [
                    "optical_power", "slot_number", "port_number", 
                    "ip_management", "mac_address"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'SN:\s*[A-Za-z0-9]+',
                        r'SSID[:\s]*[A-Za-z0-9_-]+',
                        r'password[:\s]*[A-Za-z0-9@#$%^&*()_+-=]+',
                        r'VLAN\s*:?\s*\d+',
                        r'login\s*pppoe[:\s]*[A-Za-z0-9@._-]+'
                    ],
                    "remove_patterns": [
                        r'OLT-[A-Z0-9-]+\s*quit',
                        r'configure terminal',
                        r'^#\s*$'
                    ]
                },
                "extraction_priority": {
                    "serial_code": 10,
                    "wifi_ssid": 9,
                    "wifi_passcode": 9,
                    "vlan": 8,
                    "model_onu": 7,
                    "login_pppoe": 6
                }
            },
            
            "linkdeinternet_dedicated_internet_connectivity": {
                "name": "Link de Internet - Conectividade Dedicada",
                "category": "business_internet",
                "priority_level": "critical",
                "mandatory_fields": [
                    "client_type",       # Business classification
                    "technology_id",     # Service technology
                    "cpe",              # Customer equipment
                    "ip_management",     # Dedicated IP
                    "vlan",             # Network isolation
                    "ip_block",         # IP block allocation
                    "pop_description",   # Point of presence
                    "interface_1"       # Physical interface
                ],
                "optional_fields": [
                    "gateway", "asn", "prefixes", "mac_address",
                    "serial_code", "service_code"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'IP\s*CPE[:\s]*([0-9]{1,3}\.){3}[0-9]{1,3}',
                        r'BLOCO\s*IP[:\s]*([0-9]{1,3}\.){3}[0-9]{1,3}/\d+',
                        r'(RESIDENCIAL|EMPRESARIAL|CORPORATIVO)',
                        r'VLAN\s*:?\s*\d+',
                        r'interface\s*([\w\d/-]+)',
                        r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+'
                    ]
                },
                "extraction_priority": {
                    "client_type": 10,
                    "technology_id": 9,
                    "ip_management": 9,
                    "ip_block": 8,
                    "vlan": 8,
                    "cpe": 7,
                    "interface_1": 7,
                    "pop_description": 6
                }
            },
            
            "linkdeinternet_gpon_semi_dedicated_connections": {
                "name": "Link de Internet - GPON Semi-Dedicado",
                "category": "semi_dedicated",
                "priority_level": "high",
                "mandatory_fields": [
                    "client_type",
                    "technology_id",     # Must be GPON
                    "provider_id",       # ASN
                    "cpe",              # OLT equipment
                    "vlan",
                    "serial_code",       # ONU serial
                    "pop_description"
                ],
                "optional_fields": [
                    "model_onu", "optical_power", "slot_number", 
                    "port_number", "onu_id", "service_code"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'AS\s*Cliente[:\s]*\d+',
                        r'GPON',
                        r'OLT-[A-Z0-9-]+',
                        r'SN:\s*[A-Za-z0-9]+',
                        r'VLAN\s*:?\s*\d+'
                    ]
                },
                "extraction_priority": {
                    "client_type": 10,
                    "technology_id": 9,
                    "provider_id": 8,
                    "serial_code": 8,
                    "cpe": 7,
                    "vlan": 7,
                    "pop_description": 6
                }
            },
            
            "linkdeinternet_direct_l2l_links": {
                "name": "Link de Internet - Links Diretos L2L",
                "category": "point_to_point",
                "priority_level": "critical",
                "mandatory_fields": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "cpe",
                    "interface_1",
                    "vlan"
                ],
                "optional_fields": [
                    "service_code", "ip_management", "gateway",
                    "serial_code", "mac_address"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'interface\s*([\w\d/-]+)',
                        r'([A-Z]{4,}\d+/Frame\d+/Slot\d+/Port\d+)',
                        r'L2L|P2P'
                    ]
                },
                "extraction_priority": {
                    "client_type": 10,
                    "technology_id": 9,
                    "interface_1": 9,
                    "provider_id": 8,
                    "pop_description": 8,
                    "cpe": 7,
                    "vlan": 6
                }
            },
            
            "linkdeinternet_mpls_data_transport_services": {
                "name": "Link de Internet - Serviços de Transporte MPLS",
                "category": "mpls_transport",
                "priority_level": "critical", 
                "mandatory_fields": [
                    "client_type",
                    "technology_id",     # Must include MPLS
                    "provider_id",
                    "pop_description",
                    "interface_1",
                    "vlan",
                    "cpe",
                    "ip_management"
                ],
                "optional_fields": [
                    "service_code", "gateway", "asn", 
                    "prefixes", "ip_block"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'MPLS.*VPN',
                        r'interface\s*([\w\d/-]+)',
                        r'IP\s*CPE[:\s]*([0-9]{1,3}\.){3}[0-9]{1,3}'
                    ]
                },
                "extraction_priority": {
                    "client_type": 10,
                    "technology_id": 9,
                    "provider_id": 8,
                    "interface_1": 8,
                    "ip_management": 7,
                    "vlan": 7,
                    "pop_description": 6,
                    "cpe": 6
                }
            },
            
            "linkdeinternet_lan_to_lan_infrastructure": {
                "name": "Link de Internet - Infraestrutura LAN-to-LAN",
                "category": "lan_infrastructure",
                "priority_level": "high",
                "mandatory_fields": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "interface_1",
                    "cpe",
                    "ip_management",
                    "vlan"
                ],
                "optional_fields": [
                    "service_code", "gateway", "mac_address",
                    "serial_code", "asn"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'LAN.*(?:bridge|switch)',
                        r'interface\s*([\w\d/-]+)'
                    ]
                },
                "extraction_priority": {
                    "client_type": 10,
                    "technology_id": 9,
                    "provider_id": 8,
                    "interface_1": 8,
                    "ip_management": 7,
                    "cpe": 7,
                    "vlan": 6,
                    "pop_description": 6
                }
            },
            
            "linkdeinternet_ip_transit_services": {
                "name": "Link de Internet - Serviços de Trânsito IP",
                "category": "ip_transit",
                "priority_level": "critical",
                "mandatory_fields": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "interface_1",
                    "gateway",           # Critical for transit
                    "asn",              # BGP required
                    "vlan",
                    "prefixes"          # IP announcements
                ],
                "optional_fields": [
                    "service_code", "ip_management", "ip_block",
                    "cpe", "serial_code"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'BGP.*peer',
                        r'GTW:\s*\d+\.\d+\.\d+\.\d+',
                        r'AS\s*Cliente[:\s]*\d+',
                        r'prefixo[:\s]*([0-9]{1,3}\.){3}[0-9]{1,3}/\d+'
                    ]
                },
                "extraction_priority": {
                    "client_type": 10,
                    "technology_id": 9,
                    "asn": 9,
                    "gateway": 8,
                    "provider_id": 8,
                    "interface_1": 7,
                    "prefixes": 7,
                    "vlan": 6,
                    "pop_description": 6
                }
            },
            
            "linkdeinternet_traffic_exchange_ptt": {
                "name": "Link de Internet - Intercâmbio de Tráfego PTT",
                "category": "traffic_exchange",
                "priority_level": "high",
                "mandatory_fields": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "interface_1",
                    "vlan"
                ],
                "optional_fields": [
                    "service_code", "asn", "gateway",
                    "ip_management", "cpe"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'PTT.*IX',
                        r'interface\s*([\w\d/-]+)'
                    ]
                },
                "extraction_priority": {
                    "client_type": 10,
                    "technology_id": 9,
                    "provider_id": 8,
                    "interface_1": 7,
                    "pop_description": 7,
                    "vlan": 6
                }
            },
            
            "linkdeinternet_enterprise_gpon_lan": {
                "name": "Link de Internet - GPON Empresarial LAN",
                "category": "enterprise_gpon",
                "priority_level": "high",
                "mandatory_fields": [
                    "client_type",       # Must be EMPRESARIAL/CORPORATIVO
                    "technology_id",     # GPON + LAN
                    "provider_id",
                    "pop_description",
                    "serial_code",       # ONU serial for GPON
                    "cpe",              # OLT/ONU equipment
                    "ip_management",     # Enterprise needs dedicated IP
                    "vlan"
                ],
                "optional_fields": [
                    "service_code", "model_onu", "optical_power",
                    "gateway", "asn", "slot_number", "port_number"
                ],
                "cleaning_rules": {
                    "preserve_patterns": [
                        r'(EMPRESARIAL|CORPORATIVO)',
                        r'GPON',
                        r'OLT-[A-Z0-9-]+',
                        r'ONU[:\s]*\d+',
                        r'SN:\s*[A-Za-z0-9]+'
                    ]
                },
                "extraction_priority": {
                    "client_type": 10,
                    "technology_id": 9,
                    "provider_id": 8,
                    "serial_code": 8,
                    "ip_management": 7,
                    "cpe": 7,
                    "vlan": 6,
                    "pop_description": 6
                }
            }
        }
        
        # Field mapping for extraction
        self.extracted_field_mapping = {
            # Standard mappings for all groups
            "serial_code": "extracted_serial_code",
            "model_onu": "extracted_model_onu", 
            "wifi_ssid": "extracted_wifi_ssid",
            "wifi_passcode": "extracted_wifi_passcode",
            "vlan": "extracted_vlan",
            "login_pppoe": "extracted_login_pppoe",
            "client_type": "extracted_client_type",
            "technology_id": "extracted_technology_id",
            "cpe": "extracted_cpe",
            "ip_management": "extracted_ip_management",
            "ip_block": "extracted_ip_block",
            "pop_description": "extracted_pop_description",
            "interface_1": "extracted_interface_1",
            "provider_id": "extracted_asn",  # ASN maps to provider_id
            "gateway": "extracted_gateway",
            "asn": "extracted_asn",
            "prefixes": "extracted_prefixes",
            "service_code": "extracted_service_code",
            "optical_power": "extracted_optical_power",
            "slot_number": "extracted_slot_number",
            "port_number": "extracted_port_number",
            "onu_id": "extracted_onu_id",
            "mac_address": "extracted_mac_address"
        }
    
    def get_export_columns_for_group(self, group_key, dataframe):
        """
        Get the specific columns that should be exported for a product group
        Returns: ID + hosting_type + product_group + mandatory extracted fields with data
        """
        export_columns = []
        
        # ALWAYS include ID columns if they exist (case insensitive search)
        id_column_names = ['id', 'ID', 'Id', 'identifier', 'Identifier', 'IDENTIFIER']
        for col_name in id_column_names:
            if col_name in dataframe.columns:
                export_columns.append(col_name)
                break  # Only add the first ID column found
        
        # ALWAYS include hosting_type columns if they exist (case insensitive search)
        hosting_type_names = ['hosting_type', 'hosting_Type', 'Hosting_Type', 'HOSTING_TYPE', 
                            'hostingtype', 'hostingType', 'HostingType', 'HOSTINGTYPE']
        for col_name in hosting_type_names:
            if col_name in dataframe.columns:
                export_columns.append(col_name)
                break  # Only add the first hosting_type column found
        
        # ALWAYS include product_group if it exists
        if 'product_group' in dataframe.columns:
            export_columns.append('product_group')
        
        # Get mandatory fields for this specific group
        if self.is_valid_group(group_key):
            mandatory_fields = self.get_mandatory_fields(group_key)
            group_data = dataframe[dataframe['product_group'] == group_key] if 'product_group' in dataframe.columns else dataframe
            
            print(f"📋 Group {group_key} mandatory fields: {mandatory_fields}")
            
            # Add only extracted mandatory fields that actually have data in this group
            for field in mandatory_fields:
                extracted_field = f'extracted_{field}'
                if extracted_field in dataframe.columns:
                    # Check if this field has meaningful data in this group
                    field_data = group_data[extracted_field].dropna()
                    # Remove empty strings and 'nan' values
                    meaningful_data = field_data[field_data.astype(str).str.strip() != '']
                    meaningful_data = meaningful_data[meaningful_data.astype(str).str.lower() != 'nan']
                    
                    if len(meaningful_data) > 0:
                        export_columns.append(extracted_field)
                        print(f"   ✅ Including {extracted_field}: {len(meaningful_data)} records with data")
                    else:
                        print(f"   ❌ Skipping {extracted_field}: no meaningful data")
        
        # Remove duplicates while preserving order
        unique_columns = []
        for col in export_columns:
            if col not in unique_columns:
                unique_columns.append(col)
        
        print(f"📤 Final export columns for {group_key}: {unique_columns}")
        return unique_columns

    def get_all_mandatory_export_columns(self, dataframe):
        """
        Get all columns that should be exported across all groups
        Returns: ID + hosting_type + product_group + all mandatory fields with data
        """
        all_export_columns = set()
        
        # Always include priority columns
        priority_columns = ['id', 'ID', 'Id', 'identifier', 'Identifier', 
                        'hosting_type', 'hosting_Type', 'Hosting_Type', 'HOSTING_TYPE',
                        'hostingtype', 'hostingType']
        
        for col in priority_columns:
            if col in dataframe.columns:
                all_export_columns.add(col)
        
        # Always include product_group
        if 'product_group' in dataframe.columns:
            all_export_columns.add('product_group')
        
        # Get mandatory fields for each group that exists in the data
        if 'product_group' in dataframe.columns:
            for group_key in dataframe['product_group'].dropna().unique():
                if self.is_valid_group(group_key):
                    group_columns = self.get_export_columns_for_group(group_key, dataframe)
                    all_export_columns.update(group_columns)
        else:
            # If no product groups, include all extracted fields that have data
            for col in dataframe.columns:
                if col.startswith('extracted_') and dataframe[col].notna().any():
                    all_export_columns.add(col)
        
        # Return as ordered list, preserving the columns that exist in the dataframe
        final_columns = [col for col in dataframe.columns if col in all_export_columns]
        print(f"📋 All mandatory export columns: {final_columns}")
        return final_columns

    def validate_mandatory_field_coverage(self, dataframe, group_column='product_group'):
        """
        Validate that mandatory fields have adequate coverage for export
        """
        validation_results = {}
        
        if group_column not in dataframe.columns:
            return {'error': 'No product group column found'}
        
        for group_key in dataframe[group_column].dropna().unique():
            if not self.is_valid_group(group_key):
                continue
                
            group_data = dataframe[dataframe[group_column] == group_key]
            mandatory_fields = self.get_mandatory_fields(group_key)
            group_info = self.get_group_info(group_key)
            
            field_coverage = {}
            total_coverage = 0
            fields_with_data = 0
            
            for field in mandatory_fields:
                extracted_field = f'extracted_{field}'
                
                if extracted_field in group_data.columns:
                    # Count meaningful data (not null, not empty, not 'nan')
                    non_null = group_data[extracted_field].notna().sum()
                    non_empty = group_data[extracted_field].astype(str).str.strip().ne('').sum()
                    meaningful = group_data[extracted_field].astype(str).str.lower().ne('nan').sum()
                    
                    actual_data_count = min(non_null, non_empty, meaningful)
                    coverage_rate = (actual_data_count / len(group_data)) * 100 if len(group_data) > 0 else 0
                    
                    field_coverage[field] = {
                        'coverage_rate': round(coverage_rate, 2),
                        'records_with_data': int(actual_data_count),
                        'total_records': len(group_data),
                        'will_be_exported': coverage_rate > 0
                    }
                    
                    total_coverage += coverage_rate
                    if coverage_rate > 0:
                        fields_with_data += 1
                else:
                    field_coverage[field] = {
                        'coverage_rate': 0.0,
                        'records_with_data': 0,
                        'total_records': len(group_data),
                        'will_be_exported': False,
                        'missing_field': True
                    }
            
            avg_coverage = total_coverage / len(mandatory_fields) if mandatory_fields else 0
            
            validation_results[group_key] = {
                'group_name': group_info['name'] if group_info else group_key,
                'total_records': len(group_data),
                'mandatory_fields_count': len(mandatory_fields),
                'fields_with_data': fields_with_data,
                'fields_to_export': fields_with_data,
                'average_coverage': round(avg_coverage, 2),
                'field_details': field_coverage,
                'export_ready': fields_with_data > 0,
                'quality_assessment': (
                    'excellent' if avg_coverage >= 80 else
                    'good' if avg_coverage >= 60 else
                    'fair' if avg_coverage >= 30 else
                    'poor'
                )
            }
        
        return validation_results

    # Add to the end of the ProductGroupManager class:
    def create_export_summary(self, dataframe, group_column='product_group'):
        """
        Create a summary of what will be exported
        """
        summary = {
            'export_type': 'mandatory_fields_only',
            'total_records': len(dataframe),
            'export_timestamp': datetime.now().isoformat(),
            'groups_processed': 0,
            'total_fields_exported': 0,
            'priority_columns_found': [],
            'group_summaries': {}
        }
        
        # Check for priority columns
        priority_columns = ['id', 'ID', 'Id', 'hosting_type', 'hosting_Type', 'product_group']
        for col in priority_columns:
            if col in dataframe.columns:
                summary['priority_columns_found'].append(col)
        
        # Analyze by group
        if group_column in dataframe.columns:
            for group_key in dataframe[group_column].dropna().unique():
                if self.is_valid_group(group_key):
                    export_columns = self.get_export_columns_for_group(group_key, dataframe)
                    group_info = self.get_group_info(group_key)
                    
                    summary['group_summaries'][group_key] = {
                        'name': group_info['name'] if group_info else group_key,
                        'records': len(dataframe[dataframe[group_column] == group_key]),
                        'columns_to_export': len(export_columns),
                        'exported_columns': export_columns
                    }
                    summary['groups_processed'] += 1
                    summary['total_fields_exported'] += len([col for col in export_columns if col.startswith('extracted_')])
        
        return summary
    def get_all_groups(self):
        """Get all available product group keys"""
        return list(self.product_groups.keys())
    
    def get_group_info(self, group_key):
        """Get complete group information"""
        return self.product_groups.get(group_key)
    
    def get_group_display_name(self, group_key):
        """Get human-readable group name"""
        group_info = self.product_groups.get(group_key)
        return group_info['name'] if group_info else group_key
    
    def get_mandatory_fields(self, group_key):
        """Get mandatory fields for a product group"""
        group_info = self.product_groups.get(group_key)
        return group_info['mandatory_fields'] if group_info else []
    
    def get_optional_fields(self, group_key):
        """Get optional fields for a product group"""
        group_info = self.product_groups.get(group_key)
        return group_info['optional_fields'] if group_info else []
    
    def get_all_fields(self, group_key):
        """Get all fields (mandatory + optional) for a product group"""
        mandatory = self.get_mandatory_fields(group_key)
        optional = self.get_optional_fields(group_key)
        return mandatory + optional
    
    def get_cleaning_rules(self, group_key):
        """Get cleaning rules specific to a product group"""
        group_info = self.product_groups.get(group_key)
        return group_info.get('cleaning_rules', {}) if group_info else {}
    
    def get_extraction_priority(self, group_key):
        """Get field extraction priorities for a product group"""
        group_info = self.product_groups.get(group_key)
        return group_info.get('extraction_priority', {}) if group_info else {}
    
    def get_extracted_field_mapping(self, group_key):
        """Get mapping from business fields to extracted field names"""
        return self.extracted_field_mapping
    
    def get_group_category(self, group_key):
        """Get the category of a product group"""
        group_info = self.product_groups.get(group_key)
        return group_info.get('category') if group_info else 'unknown'
    
    def get_group_priority_level(self, group_key):
        """Get the priority level of a product group"""
        group_info = self.product_groups.get(group_key)
        return group_info.get('priority_level', 'medium') if group_info else 'medium'
    
    def is_valid_group(self, group_key):
        """Check if a group key is valid"""
        return group_key in self.product_groups
    
    def get_groups_by_category(self, category):
        """Get all groups that belong to a specific category"""
        return [
            group_key for group_key, group_info in self.product_groups.items()
            if group_info.get('category') == category
        ]
    
    def get_groups_by_priority(self, priority_level):
        """Get all groups with a specific priority level"""
        return [
            group_key for group_key, group_info in self.product_groups.items()
            if group_info.get('priority_level') == priority_level
        ]
    
    def validate_group_data(self, df, product_group_column='product_group'):
        """Validate product group data in DataFrame"""
        if product_group_column not in df.columns:
            return {
                'valid': False,
                'error': f"Column '{product_group_column}' not found",
                'suggestions': ['Add product_group column', 'Use generic processing']
            }
        
        # Check for valid groups
        unique_groups = df[product_group_column].dropna().unique()
        valid_groups = [g for g in unique_groups if self.is_valid_group(g)]
        invalid_groups = [g for g in unique_groups if not self.is_valid_group(g)]
        
        return {
            'valid': len(invalid_groups) == 0,
            'total_groups': len(unique_groups),
            'valid_groups': valid_groups,
            'invalid_groups': invalid_groups,
            'coverage': len(valid_groups) / len(unique_groups) if unique_groups.size > 0 else 0
        }
    
    def analyze_group_completeness(self, df, product_group_column='product_group'):
        """Analyze mandatory field completeness by product group"""
        results = {}
        
        if product_group_column not in df.columns:
            return {'error': 'Product group column not found'}
        
        for group_key in df[product_group_column].dropna().unique():
            if not self.is_valid_group(group_key):
                continue
                
            group_data = df[df[product_group_column] == group_key]
            mandatory_fields = self.get_mandatory_fields(group_key)
            
            completeness_stats = {}
            total_filled = 0
            total_possible = len(mandatory_fields) * len(group_data)
            
            for field in mandatory_fields:
                # Map to extracted field name
                extracted_field = self.extracted_field_mapping.get(field, f"extracted_{field}")
                
                if extracted_field in group_data.columns:
                    filled_records = group_data[extracted_field].notna().sum()
                    completeness_rate = (filled_records / len(group_data)) * 100 if len(group_data) > 0 else 0
                    total_filled += filled_records
                    
                    completeness_stats[field] = {
                        'completeness_rate': round(completeness_rate, 2),
                        'filled_records': int(filled_records),
                        'total_records': len(group_data),
                        'extracted_field': extracted_field
                    }
                else:
                    completeness_stats[field] = {
                        'completeness_rate': 0.0,
                        'filled_records': 0,
                        'total_records': len(group_data),
                        'extracted_field': extracted_field,
                        'missing': True
                    }
            
            overall_completeness = (total_filled / total_possible) * 100 if total_possible > 0 else 0
            
            results[group_key] = {
                'name': self.get_group_display_name(group_key),
                'category': self.get_group_category(group_key),
                'priority_level': self.get_group_priority_level(group_key),
                'total_records': len(group_data),
                'mandatory_fields': mandatory_fields,
                'completeness_stats': completeness_stats,
                'overall_completeness': round(overall_completeness, 2),
                'total_filled': total_filled,
                'total_possible': total_possible
            }
        
        return results


# Create global instance
product_group_manager = ProductGroupManager()