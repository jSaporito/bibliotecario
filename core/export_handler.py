import pandas as pd
import os
import re
from datetime import datetime

class ExportHandler:
    """Simple data export handler with fixed file paths"""
    
    def clean_for_excel(self, text):
        """Clean text to be safe for Excel export"""
        if pd.isna(text) or text is None:
            return text
        
        text_str = str(text)
        
        # Remove control characters that Excel can't handle
        cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', text_str)
        
        # Replace problematic characters
        cleaned = cleaned.replace('\x1f', '_')  # Unit separator
        cleaned = cleaned.replace('\x1e', '_')  # Record separator  
        cleaned = cleaned.replace('\x1d', '_')  # Group separator
        
        # Limit cell content length (Excel has limits)
        if len(cleaned) > 32000:  # Excel cell limit is ~32,767 characters
            cleaned = cleaned[:32000] + "... [TRUNCATED]"
        
        return cleaned
    
    def export_data(self, dataframe, output_dir, filename_base, formats=['csv', 'excel']):
        """Export dataframe to specified formats with fixed file naming"""
        result = {
            'success': False,
            'files_created': [],
            'errors': []
        }
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # ✅ Create timestamp once and use consistently
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            print(f"Export timestamp: {timestamp}")
            print(f"Output directory: {output_dir}")
            print(f"Filename base: {filename_base}")
            
            # Export CSV
            if 'csv' in formats or 'both' in formats:
                try:
                    # ✅ Simple, clean filename
                    csv_filename = f"bibliotecario_export_{timestamp}.csv"
                    csv_path = os.path.join(output_dir, csv_filename)
                    
                    print(f"Creating CSV: {csv_path}")
                    dataframe.to_csv(csv_path, index=False, encoding='utf-8')
                    
                    if os.path.exists(csv_path):
                        result['files_created'].append({
                            'format': 'csv',
                            'path': csv_path,
                            'size': os.path.getsize(csv_path)
                        })
                        print(f"✓ CSV export successful: {csv_path}")
                    else:
                        print(f"✗ CSV file not created: {csv_path}")
                        
                except Exception as e:
                    error_msg = f"CSV export failed: {str(e)}"
                    result['errors'].append(error_msg)
                    print(f"✗ {error_msg}")
            
            # Export Excel with character cleaning
            if 'excel' in formats or 'both' in formats:
                try:
                    # ✅ Simple, clean filename
                    excel_filename = f"bibliotecario_export_{timestamp}.xlsx"
                    excel_path = os.path.join(output_dir, excel_filename)
                    
                    print(f"Creating Excel: {excel_path}")
                    
                    # Clean the dataframe for Excel
                    print("Cleaning data for Excel export...")
                    clean_df = dataframe.copy()
                    
                    # Clean all text columns
                    for column in clean_df.columns:
                        if clean_df[column].dtype == 'object':  # Text columns
                            clean_df[column] = clean_df[column].apply(self.clean_for_excel)
                    
                    # Export to Excel
                    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                        clean_df.to_excel(writer, sheet_name='Processed_Data', index=False)
                        
                        # Add a simple info sheet
                        info_data = {
                            'Info': ['Total Rows', 'Total Columns', 'Export Date'],
                            'Value': [len(clean_df), len(clean_df.columns), datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
                        }
                        info_df = pd.DataFrame(info_data)
                        info_df.to_excel(writer, sheet_name='Export_Info', index=False)
                    
                    if os.path.exists(excel_path):
                        result['files_created'].append({
                            'format': 'excel', 
                            'path': excel_path,
                            'size': os.path.getsize(excel_path)
                        })
                        print(f"✓ Excel export successful: {excel_path}")
                    else:
                        print(f"✗ Excel file not created: {excel_path}")
                        
                except Exception as e:
                    error_msg = f"Excel export failed: {str(e)}"
                    result['errors'].append(error_msg)
                    print(f"✗ {error_msg}")
                    
                    # ✅ Try CSV as fallback with clean filename
                    if 'csv' not in formats:
                        try:
                            fallback_filename = f"bibliotecario_fallback_{timestamp}.csv"
                            fallback_path = os.path.join(output_dir, fallback_filename)
                            
                            print(f"Creating fallback CSV: {fallback_path}")
                            dataframe.to_csv(fallback_path, index=False, encoding='utf-8')
                            
                            if os.path.exists(fallback_path):
                                result['files_created'].append({
                                    'format': 'csv',
                                    'path': fallback_path,
                                    'size': os.path.getsize(fallback_path)
                                })
                                print(f"✓ Fallback CSV created: {fallback_path}")
                            
                        except Exception as csv_e:
                            print(f"✗ Even fallback CSV failed: {str(csv_e)}")
            
            result['success'] = len(result['files_created']) > 0
            
            if result['success']:
                print(f"✓ Export completed: {len(result['files_created'])} file(s) created")
                for file_info in result['files_created']:
                    print(f"  - {file_info['format'].upper()}: {file_info['path']} ({file_info['size']} bytes)")
            else:
                print("✗ Export failed: No files were created")
                
        except Exception as e:
            error_msg = f"General export error: {str(e)}"
            result['errors'].append(error_msg)
            print(f"✗ Export handler error: {error_msg}")
        
        return result
    
    def create_download_info(self, export_results):
        """Create download information with file existence check"""
        if not export_results['success']:
            return None
        
        # ✅ Verify files exist before creating download info
        valid_files = []
        for file_info in export_results['files_created']:
            if os.path.exists(file_info['path']):
                valid_files.append({
                    'format': file_info['format'].upper(),
                    'path': file_info['path'],
                    'filename': os.path.basename(file_info['path']),
                    'size_mb': round(file_info['size'] / (1024 * 1024), 2)
                })
            else:
                print(f"⚠️  File does not exist: {file_info['path']}")
        
        return {
            'files': valid_files,
            'total_files': len(valid_files)
        }