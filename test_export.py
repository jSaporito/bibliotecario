#!/usr/bin/env python3
"""
Simple test script to verify product group export functionality
Run this from your project root directory
"""

import pandas as pd
import os
import sys

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def create_sample_data():
    """Create sample data with product groups"""
    data = []
    
    # Sample data for Banda Larga (residential fiber)
    for i in range(10):
        data.append({
            'id': f'bb_{i}',
            'product_group': 'bandalarga_broadband_fiber_plans',
            'obs': f'Cliente residencial SN: BB{i:03d} SSID: WiFi_{i} password: pass{i} VLAN: {100+i}',
            'extracted_serial_code': f'BB{i:03d}',
            'extracted_wifi_ssid': f'WiFi_{i}',
            'extracted_wifi_passcode': f'pass{i}',
            'extracted_vlan': 100+i,
            'extracted_model_onu': 'ZTE-F670L',
            'extracted_ip_management': None,  # Not used for residential
            'extracted_gateway': None
        })
    
    # Sample data for Link Dedicado (business internet)
    for i in range(8):
        data.append({
            'id': f'ld_{i}',
            'product_group': 'linkdeinternet_dedicated_internet_connectivity',
            'obs': f'Empresarial IP: 192.168.{i+1}.1 GTW: 192.168.{i+1}.254 VLAN: {200+i}',
            'extracted_client_type': 'EMPRESARIAL',
            'extracted_technology_id': 'ETHERNET',
            'extracted_ip_management': f'192.168.{i+1}.1',
            'extracted_gateway': f'192.168.{i+1}.254',
            'extracted_vlan': 200+i,
            'extracted_cpe': f'CISCO-{i}',
            'extracted_serial_code': None,  # Not used for business
            'extracted_wifi_ssid': None
        })
    
    return pd.DataFrame(data)

def test_export():
    """Test the export functionality"""
    try:
        from core.export_handler import EnhancedExportHandler
        from core.product_groups import product_group_manager
        
        print("🧪 Testing Product Group Export")
        print("=" * 40)
        
        # Create test data
        print("📊 Creating sample data...")
        df = create_sample_data()
        print(f"   Created {len(df)} records")
        print(f"   Groups: {df['product_group'].nunique()}")
        
        # Show distribution
        for group, count in df['product_group'].value_counts().items():
            group_info = product_group_manager.get_group_info(group)
            name = group_info['name'] if group_info else group
            print(f"   - {name}: {count} records")
        
        # Test export
        print("\n📁 Testing export...")
        output_dir = 'test_output'
        os.makedirs(output_dir, exist_ok=True)
        
        exporter = EnhancedExportHandler()
        result = exporter.export_data(
            dataframe=df,
            output_dir=output_dir,
            filename_base='test',
            formats=['csv', 'excel'],
            product_group_manager=product_group_manager
        )
        
        # Show results
        print(f"\n✅ Export success: {result['success']}")
        print(f"📋 Files created: {len(result['files_created'])}")
        
        if result['errors']:
            print(f"⚠️ Errors: {result['errors']}")
        
        # List files
        for file_info in result['files_created']:
            if 'product_group' in file_info:
                print(f"   📄 {file_info['group_name']}: {file_info['filename']}")
            else:
                print(f"   📄 General: {file_info['filename']}")
        
        print(f"\n🎉 Test completed! Check '{output_dir}' directory.")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_export()
    if success:
        print("\n✅ All tests passed!")
    else:
        print("\n❌ Tests failed!")