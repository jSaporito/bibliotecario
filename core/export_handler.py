import pandas as pd
import json
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class EnhancedExportHandler:
    """Enhanced export handler - fixed and simplified for Flask integration"""
    
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
            # Use output_dir EXACTLY as given - don't modify it
            export_folder = output_dir
            
            # Just ensure it exists
            os.makedirs(export_folder, exist_ok=True)
            
            print(f"✅ Exporting to: {export_folder}")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Handle 'both' format for backward compatibility
            if 'both' in formats:
                formats = ['csv', 'excel']
            elif 'all' in formats:
                formats = ['csv', 'excel', 'json']
            
            # Export CSV
            if 'csv' in formats:
                csv_filename = f"bibliotecario_export_{timestamp}.csv"
                csv_path = os.path.join(export_folder, csv_filename)
                
                dataframe.to_csv(csv_path, index=False, encoding='utf-8-sig')
                
                if os.path.exists(csv_path):
                    result['files_created'].append({
                        'format': 'csv',
                        'path': csv_path,
                        'filename': csv_filename,
                        'size': os.path.getsize(csv_path)
                    })
                    print(f"✅ CSV: {csv_path}")
            
            # Export Excel
            if 'excel' in formats:
                excel_filename = f"bibliotecario_export_{timestamp}.xlsx"
                excel_path = os.path.join(export_folder, excel_filename)
                
                # Clean data for Excel
                clean_df = dataframe.copy()
                for col in clean_df.select_dtypes(include=['object']).columns:
                    clean_df[col] = clean_df[col].astype(str)
                    clean_df[col] = clean_df[col].apply(lambda x: x[:32000] if len(str(x)) > 32000 else x)
                
                # Create Excel with multiple sheets
                with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                    # Main data sheet
                    clean_df.to_excel(writer, sheet_name='Processed_Data', index=False)
                    
                    # Summary sheet
                    summary_data = self._create_summary_data(dataframe)
                    if summary_data:
                        summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
                        summary_df.to_excel(writer, sheet_name='Summary', index=False)
                    
                    # Extracted fields sheet (if available)
                    extracted_cols = [col for col in clean_df.columns if col.startswith('extracted_')]
                    if extracted_cols and len(extracted_cols) > 0:
                        # Create a summary of extracted fields
                        field_summary = []
                        for col in extracted_cols:
                            field_name = col.replace('extracted_', '')
                            non_null_count = clean_df[col].notna().sum()
                            total_count = len(clean_df)
                            fill_rate = (non_null_count / total_count) * 100 if total_count > 0 else 0
                            
                            field_summary.append([
                                field_name.replace('_', ' ').title(),
                                non_null_count,
                                total_count,
                                f"{fill_rate:.1f}%"
                            ])
                        
                        if field_summary:
                            field_df = pd.DataFrame(field_summary, 
                                                  columns=['Field', 'Extracted', 'Total', 'Fill Rate'])
                            field_df.to_excel(writer, sheet_name='Extraction_Summary', index=False)
                
                if os.path.exists(excel_path):
                    result['files_created'].append({
                        'format': 'excel',
                        'path': excel_path,
                        'filename': excel_filename,
                        'size': os.path.getsize(excel_path)
                    })
                    print(f"✅ Excel: {excel_path}")
            
            # Export JSON
            if 'json' in formats:
                json_filename = f"bibliotecario_export_{timestamp}.json"
                json_path = os.path.join(export_folder, json_filename)
                
                # Create JSON structure
                json_data = {
                    'metadata': {
                        'export_timestamp': datetime.now().isoformat(),
                        'total_records': len(dataframe),
                        'columns': list(dataframe.columns),
                        'processing_info': {
                            'records_processed': len(dataframe),
                            'extracted_fields': len([col for col in dataframe.columns if col.startswith('extracted_')]),
                            'cleaned_text_available': any('cleaned' in col for col in dataframe.columns)
                        }
                    },
                    'data': []
                }
                
                # Convert DataFrame to records (limit to avoid huge files)
                sample_size = min(1000, len(dataframe))  # Limit JSON to first 1000 records
                for _, row in dataframe.head(sample_size).iterrows():
                    record = {}
                    for col, value in row.items():
                        if pd.isna(value):
                            record[col] = None
                        elif isinstance(value, (int, float, str, bool)):
                            record[col] = value
                        else:
                            record[col] = str(value)
                    json_data['data'].append(record)
                
                # Add summary
                json_data['summary'] = {
                    'total_records_in_file': len(dataframe),
                    'records_in_json': sample_size,
                    'note': f'JSON limited to first {sample_size} records for file size management'
                }
                
                # Write JSON
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)
                
                if os.path.exists(json_path):
                    result['files_created'].append({
                        'format': 'json',
                        'path': json_path,
                        'filename': json_filename,
                        'size': os.path.getsize(json_path)
                    })
                    print(f"✅ JSON: {json_path}")
            
            result['success'] = len(result['files_created']) > 0
            
        except Exception as e:
            print(f"❌ Export error: {str(e)}")
            result['errors'].append(str(e))
        
        return result
    
    def _create_summary_data(self, df):
        """Create summary data for Excel"""
        try:
            summary = [
                ['Processing Timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['Total Records', len(df)],
                ['Total Columns', len(df.columns)],
                ['', ''],  # Empty row
            ]
            
            # Add column info
            summary.append(['Column Information', ''])
            original_cols = [col for col in df.columns if not col.startswith('extracted_') and not col.endswith('_cleaned')]
            extracted_cols = [col for col in df.columns if col.startswith('extracted_')]
            cleaned_cols = [col for col in df.columns if col.endswith('_cleaned')]
            
            summary.extend([
                ['Original Columns', len(original_cols)],
                ['Extracted Fields', len(extracted_cols)],
                ['Cleaned Text Columns', len(cleaned_cols)],
            ])
            
            return summary
            
        except Exception as e:
            print(f"Warning: Could not create summary data: {e}")
            return None
    
    def create_download_info(self, export_results):
        """Create download information for the web interface"""
        if not export_results['success']:
            return None
        
        download_info = {
            'files': [],
            'total_files': len(export_results['files_created']),
            'timestamp': datetime.now().isoformat()
        }
        
        for file_info in export_results['files_created']:
            download_info['files'].append({
                'format': file_info['format'].upper(),
                'path': file_info['path'],
                'filename': file_info['filename'],
                'size_mb': round(file_info['size'] / (1024 * 1024), 2)
            })
        
        return download_info

# Backward compatibility
ExportHandler = EnhancedExportHandler