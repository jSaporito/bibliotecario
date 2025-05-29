import pandas as pd
import os
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class ExportHandler:
    """Simple fix - just use the exact path given, don't create nested folders"""
    
    def __init__(self):
        self.max_cell_length = 32000
    
    def export_data(self, dataframe, output_dir, filename_base, formats=['csv', 'excel']):
        """Fixed export - uses exact output_dir without modification"""
        result = {
            'success': False,
            'files_created': [],
            'errors': []
        }
        
        try:
            # 🔧 ROOT CAUSE FIX: Use output_dir EXACTLY as given
            # Don't join it with anything else, don't modify it
            export_folder = output_dir  # This is already the full path from Flask config
            
            # Just ensure it exists
            os.makedirs(export_folder, exist_ok=True)
            
            print(f"✅ Exporting to: {export_folder}")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Export CSV
            if 'csv' in formats or 'both' in formats:
                csv_filename = f"bibliotecario_export_{timestamp}.csv"
                csv_path = os.path.join(export_folder, csv_filename)  # Direct join, no nesting
                
                dataframe.to_csv(csv_path, index=False, encoding='utf-8-sig')
                
                if os.path.exists(csv_path):
                    result['files_created'].append({
                        'format': 'csv',
                        'path': csv_path,
                        'size': os.path.getsize(csv_path)
                    })
                    print(f"✅ CSV: {csv_path}")
            
            # Export Excel
            if 'excel' in formats or 'both' in formats:
                excel_filename = f"bibliotecario_export_{timestamp}.xlsx"
                excel_path = os.path.join(export_folder, excel_filename)  # Direct join, no nesting
                
                # Clean data for Excel
                clean_df = dataframe.copy()
                for col in clean_df.select_dtypes(include=['object']).columns:
                    clean_df[col] = clean_df[col].astype(str)
                    clean_df[col] = clean_df[col].apply(lambda x: x[:32000] if len(str(x)) > 32000 else x)
                
                clean_df.to_excel(excel_path, index=False, engine='openpyxl')
                
                if os.path.exists(excel_path):
                    result['files_created'].append({
                        'format': 'excel',
                        'path': excel_path,
                        'size': os.path.getsize(excel_path)
                    })
                    print(f"✅ Excel: {excel_path}")
            
            result['success'] = len(result['files_created']) > 0
            
        except Exception as e:
            print(f"❌ Export error: {str(e)}")
            result['errors'].append(str(e))
        
        return result
    
    def create_download_info(self, export_results):
        """Create download info"""
        if not export_results['success']:
            return None
        
        return {
            'files': [{
                'format': f['format'].upper(),
                'path': f['path'],
                'filename': os.path.basename(f['path']),
                'size_mb': round(f['size'] / (1024 * 1024), 2)
            } for f in export_results['files_created']],
            'total_files': len(export_results['files_created'])
        }