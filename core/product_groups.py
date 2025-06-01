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