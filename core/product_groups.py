"""
Product Groups Configuration for Bibliotecario
Defines mandatory fields for each product group and provides group-based processing
"""

class ProductGroupManager:
    """
    Manages product groups and their mandatory fields
    """
    
    def __init__(self):
        self.grupos = {
            "bandalarga_broadband_fiber_plans": {
                "name": "Banda Larga - Planos de Fibra",
                "campos_obrigatorios": [
                    "serial_code",
                    "model_onu", 
                    "wifi_ssid",
                    "wifi_passcode",
                    "vlan",
                    "login_pppoe"
                ],
                "extracted_fields_mapping": {
                    "serial_code": "extracted_serial_number",
                    "model_onu": "extracted_equipment_model", 
                    "wifi_ssid": "extracted_wifi_ssid",
                    "wifi_passcode": "extracted_wifi_password",
                    "vlan": "extracted_vlan_id",
                    "login_pppoe": "extracted_service_description"
                }
            },
            "linkdeinternet_dedicated_internet_connectivity": {
                "name": "Link de Internet - Conectividade Dedicada",
                "campos_obrigatorios": [
                    "client_type",
                    "technology_id",
                    "cpe",
                    "ip_management",
                    "vlan",
                    "ip_block",
                    "pop_description",
                    "interface_1"
                ],
                "extracted_fields_mapping": {
                    "client_type": "extracted_service_description",
                    "technology_id": "extracted_service_code",
                    "cpe": "extracted_equipment_model",
                    "ip_management": "extracted_ip_cpe",
                    "vlan": "extracted_vlan_id",
                    "ip_block": "extracted_ip_block",
                    "pop_description": "extracted_pop_location",
                    "interface_1": "extracted_interface_name"
                }
            },
            "linkdeinternet_gpon_semi_dedicated_connections": {
                "name": "Link de Internet - GPON Semi-Dedicado",
                "campos_obrigatorios": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "cpe",
                    "vlan",
                    "serial_code",
                    "pop_description"
                ],
                "extracted_fields_mapping": {
                    "client_type": "extracted_service_description",
                    "technology_id": "extracted_service_code",
                    "provider_id": "extracted_asn_client",
                    "cpe": "extracted_equipment_model",
                    "vlan": "extracted_vlan_id",
                    "serial_code": "extracted_serial_number",
                    "pop_description": "extracted_pop_location"
                }
            },
            "linkdeinternet_direct_l2l_links": {
                "name": "Link de Internet - Links L2L Diretos",
                "campos_obrigatorios": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "cpe",
                    "interface_1",
                    "vlan"
                ],
                "extracted_fields_mapping": {
                    "client_type": "extracted_service_description",
                    "technology_id": "extracted_service_code",
                    "provider_id": "extracted_asn_client",
                    "pop_description": "extracted_pop_location",
                    "cpe": "extracted_equipment_model",
                    "interface_1": "extracted_interface_name",
                    "vlan": "extracted_vlan_id"
                }
            },
            "linkdeinternet_mpls_data_transport_services": {
                "name": "Link de Internet - Serviços MPLS",
                "campos_obrigatorios": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "interface_1",
                    "vlan",
                    "cpe",
                    "ip_management"
                ],
                "extracted_fields_mapping": {
                    "client_type": "extracted_service_description",
                    "technology_id": "extracted_service_code",
                    "provider_id": "extracted_asn_client",
                    "pop_description": "extracted_pop_location",
                    "interface_1": "extracted_interface_name",
                    "vlan": "extracted_vlan_id",
                    "cpe": "extracted_equipment_model",
                    "ip_management": "extracted_ip_cpe"
                }
            },
            "linkdeinternet_lan_to_lan_infrastructure": {
                "name": "Link de Internet - Infraestrutura LAN-LAN",
                "campos_obrigatorios": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "interface_1",
                    "cpe",
                    "ip_management",
                    "vlan"
                ],
                "extracted_fields_mapping": {
                    "client_type": "extracted_service_description",
                    "technology_id": "extracted_service_code",
                    "provider_id": "extracted_asn_client",
                    "pop_description": "extracted_pop_location",
                    "interface_1": "extracted_interface_name",
                    "cpe": "extracted_equipment_model",
                    "ip_management": "extracted_ip_cpe",
                    "vlan": "extracted_vlan_id"
                }
            },
            "linkdeinternet_ip_transit_services": {
                "name": "Link de Internet - Serviços IP Transit",
                "campos_obrigatorios": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "interface_1",
                    "gateway",
                    "asn",
                    "vlan",
                    "prefixes"
                ],
                "extracted_fields_mapping": {
                    "client_type": "extracted_service_description",
                    "technology_id": "extracted_service_code",
                    "provider_id": "extracted_asn_client",
                    "pop_description": "extracted_pop_location",
                    "interface_1": "extracted_interface_name",
                    "gateway": "extracted_gateway_ip",
                    "asn": "extracted_asn_client",
                    "vlan": "extracted_vlan_id",
                    "prefixes": "extracted_ip_block"
                }
            },
            "linkdeinternet_traffic_exchange_ptt": {
                "name": "Link de Internet - Troca de Tráfego PTT",
                "campos_obrigatorios": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "interface_1",
                    "vlan"
                ],
                "extracted_fields_mapping": {
                    "client_type": "extracted_service_description",
                    "technology_id": "extracted_service_code",
                    "provider_id": "extracted_asn_client",
                    "pop_description": "extracted_pop_location",
                    "interface_1": "extracted_interface_name",
                    "vlan": "extracted_vlan_id"
                }
            },
            "linkdeinternet_enterprise_gpon_lan": {
                "name": "Link de Internet - GPON Empresarial LAN",
                "campos_obrigatorios": [
                    "client_type",
                    "technology_id",
                    "provider_id",
                    "pop_description",
                    "serial_code",
                    "cpe",
                    "ip_management",
                    "vlan"
                ],
                "extracted_fields_mapping": {
                    "client_type": "extracted_service_description",
                    "technology_id": "extracted_service_code",
                    "provider_id": "extracted_asn_client",
                    "pop_description": "extracted_pop_location",
                    "serial_code": "extracted_serial_number",
                    "cpe": "extracted_equipment_model",
                    "ip_management": "extracted_ip_cpe",
                    "vlan": "extracted_vlan_id"
                }
            }
        }
    
    def get_group_info(self, group_key):
        """Get information about a specific group"""
        return self.grupos.get(group_key, None)
    
    def get_mandatory_fields(self, group_key):
        """Get mandatory fields for a specific group"""
        group = self.grupos.get(group_key)
        if group:
            return group.get("campos_obrigatorios", [])
        return []
    
    def get_extracted_field_mapping(self, group_key):
        """Get mapping from business field to extracted field"""
        group = self.grupos.get(group_key)
        if group:
            return group.get("extracted_fields_mapping", {})
        return {}
    
    def get_all_groups(self):
        """Get all available groups"""
        return list(self.grupos.keys())
    
    def get_group_display_name(self, group_key):
        """Get display name for a group"""
        group = self.grupos.get(group_key)
        if group:
            return group.get("name", group_key)
        return group_key
    
    def identify_group_from_data(self, dataframe, group_column="product_group"):
        """
        Identify which group each row belongs to based on a column
        """
        if group_column not in dataframe.columns:
            print(f"⚠️  Column '{group_column}' not found. Available columns: {list(dataframe.columns)}")
            return None
        
        # Get unique groups in the data
        unique_groups = dataframe[group_column].unique()
        valid_groups = [g for g in unique_groups if g in self.grupos.keys()]
        
        print(f"📊 Found groups in data: {unique_groups}")
        print(f"✅ Valid groups: {valid_groups}")
        
        return valid_groups
    
    def analyze_group_completeness(self, dataframe, group_column="product_group"):
        """
        Analyze completeness for each product group
        """
        if group_column not in dataframe.columns:
            return {}
        
        group_analysis = {}
        
        for group_key in self.get_all_groups():
            # Filter data for this group
            group_data = dataframe[dataframe[group_column] == group_key]
            
            if len(group_data) == 0:
                continue
            
            # Get mandatory fields for this group
            mandatory_fields = self.get_mandatory_fields(group_key)
            field_mapping = self.get_extracted_field_mapping(group_key)
            
            # Calculate completeness for mandatory fields only
            completeness_stats = {}
            
            for business_field in mandatory_fields:
                extracted_field = field_mapping.get(business_field, f"extracted_{business_field}")
                
                if extracted_field in group_data.columns:
                    total_records = len(group_data)
                    filled_records = group_data[extracted_field].notna().sum()
                    completeness_rate = (filled_records / total_records * 100) if total_records > 0 else 0
                    
                    completeness_stats[business_field] = {
                        'extracted_field': extracted_field,
                        'total_records': total_records,
                        'filled_records': int(filled_records),
                        'completeness_rate': round(completeness_rate, 2),
                        'is_mandatory': True
                    }
                else:
                    completeness_stats[business_field] = {
                        'extracted_field': extracted_field,
                        'total_records': len(group_data),
                        'filled_records': 0,
                        'completeness_rate': 0.0,
                        'is_mandatory': True,
                        'field_missing': True
                    }
            
            group_analysis[group_key] = {
                'name': self.get_group_display_name(group_key),
                'total_records': len(group_data),
                'mandatory_fields': mandatory_fields,
                'completeness_stats': completeness_stats,
                'overall_completeness': round(
                    sum([stat['completeness_rate'] for stat in completeness_stats.values()]) / len(completeness_stats), 2
                ) if completeness_stats else 0
            }
        
        return group_analysis
    
    def filter_relevant_extractions(self, extraction_results, group_key):
        """
        Filter extraction results to show only relevant fields for the group
        """
        if group_key not in self.grupos:
            return extraction_results
        
        mandatory_fields = self.get_mandatory_fields(group_key)
        field_mapping = self.get_extracted_field_mapping(group_key)
        
        # Get list of relevant extracted field names
        relevant_extracted_fields = list(field_mapping.values())
        
        # Filter extraction results
        filtered_results = {}
        
        for field_name, field_data in extraction_results.items():
            # Check if this extracted field is relevant for the group
            if any(extracted_field.replace('extracted_', '') in field_name for extracted_field in relevant_extracted_fields):
                filtered_results[field_name] = field_data
            # Or if it directly matches a mandatory field
            elif field_name.replace('extracted_', '') in mandatory_fields:
                filtered_results[field_name] = field_data
        
        return filtered_results

# Global instance
product_group_manager = ProductGroupManager()