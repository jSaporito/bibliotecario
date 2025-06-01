import pandas as pd
import json
import os
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class EnhancedExportHandler:
    """Enhanced export handler with product group separation"""
    
    def __init__(self):
        self.max_cell_length = 32000
    
    def  export_data(self, dataframe, output_dir, filename_base, formats=['csv', 'excel'], product_group_manager=None):
        """Enhanced export with product group separation - CONSOLIDATED EXCEL ONLY"""
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
            
            # Check if we have product groups
            has_product_groups = 'product_group' in dataframe.columns and product_group_manager is not None
            
            # Export CSV (single file only, no group separation)
            if 'csv' in formats:
                self._export_csv_single(dataframe, export_folder, timestamp, result)
            
            # Export Excel (consolidated with product group sheets)
            if 'excel' in formats:
                if has_product_groups:
                    self._export_excel_by_groups(dataframe, export_folder, timestamp, product_group_manager, result)
                else:
                    self._export_excel_single(dataframe, export_folder, timestamp, result)
            
            # Export JSON (single file only, no group separation)
            if 'json' in formats:
                self._export_json_single(dataframe, export_folder, timestamp, result)
            
            result['success'] = len(result['files_created']) > 0
            
        except Exception as e:
            print(f"❌ Export error: {str(e)}")
            result['errors'].append(str(e))
        
        return result
    
    def _export_csv_by_groups(self, dataframe, export_folder, timestamp, product_group_manager, result):
        """Export CSV files separated by product group"""
        try:
            if 'product_group' not in dataframe.columns:
                return
            
            for group_key in dataframe['product_group'].unique():
                if pd.isna(group_key):
                    continue
                    
                group_data = dataframe[dataframe['product_group'] == group_key].copy()
                group_info = product_group_manager.get_group_info(group_key)
                group_name = group_info['name'] if group_info else group_key
                
                # Get relevant columns for this group
                relevant_columns = self._get_relevant_columns_for_group(group_data, group_key, product_group_manager)
                group_export = group_data[relevant_columns]
                
                # Clean group name for filename
                safe_group_name = self._sanitize_filename(group_name)
                csv_filename = f"bibliotecario_{safe_group_name}_{timestamp}.csv"
                csv_path = os.path.join(export_folder, csv_filename)
                
                group_export.to_csv(csv_path, index=False, encoding='utf-8-sig')
                
                if os.path.exists(csv_path):
                    result['files_created'].append({
                        'format': 'csv',
                        'path': csv_path,
                        'filename': csv_filename,
                        'size': os.path.getsize(csv_path),
                        'product_group': group_key,
                        'group_name': group_name,
                        'records': len(group_export)
                    })
                    print(f"✅ CSV para {group_name}: {csv_path}")
                    
        except Exception as e:
            result['errors'].append(f"Erro na exportação CSV por grupos: {str(e)}")
    
    def _export_csv_single(self, dataframe, export_folder, timestamp, result):
        """Export single CSV file"""
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
    
    def _export_excel_by_groups(self, dataframe, export_folder, timestamp, product_group_manager, result):
        """Export Excel file with separate sheets for each product group"""
        try:
            excel_filename = f"bibliotecario_by_groups_{timestamp}.xlsx"
            excel_path = os.path.join(export_folder, excel_filename)
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Summary sheet first
                self._create_groups_summary_sheet(dataframe, product_group_manager, writer)
                
                # Create sheet for each product group
                for group_key in dataframe['product_group'].unique():
                    if pd.isna(group_key):
                        continue
                        
                    group_data = dataframe[dataframe['product_group'] == group_key].copy()
                    group_info = product_group_manager.get_group_info(group_key)
                    group_name = group_info['name'] if group_info else group_key
                    
                    # Get relevant columns for this group
                    relevant_columns = self._get_relevant_columns_for_group(group_data, group_key, product_group_manager)
                    group_export = group_data[relevant_columns]
                    
                    # Clean data for Excel
                    clean_group_data = self._clean_data_for_excel(group_export)
                    
                    # Create safe sheet name (Excel limit: 31 characters)
                    safe_sheet_name = self._sanitize_sheet_name(group_name)
                    
                    # Write to sheet
                    clean_group_data.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    
                    print(f"✅ Planilha criada para {group_name}: {len(clean_group_data)} registros, {len(relevant_columns)} colunas")
                
                # Create field mapping sheet
                self._create_field_mapping_sheet(dataframe, product_group_manager, writer)
            
            if os.path.exists(excel_path):
                result['files_created'].append({
                    'format': 'excel',
                    'path': excel_path,
                    'filename': excel_filename,
                    'size': os.path.getsize(excel_path),
                    'description': 'Excel com planilhas separadas por grupo de produto'
                })
                print(f"✅ Excel por grupos: {excel_path}")
                
        except Exception as e:
            result['errors'].append(f"Erro na exportação Excel por grupos: {str(e)}")
            print(f"❌ Erro Excel por grupos: {str(e)}")
    
    def _export_excel_single(self, dataframe, export_folder, timestamp, result):
        """Export single Excel file"""
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
    
    def _export_json_by_groups(self, dataframe, export_folder, timestamp, product_group_manager, result):
        """Export JSON files separated by product group"""
        try:
            for group_key in dataframe['product_group'].unique():
                if pd.isna(group_key):
                    continue
                    
                group_data = dataframe[dataframe['product_group'] == group_key].copy()
                group_info = product_group_manager.get_group_info(group_key)
                group_name = group_info['name'] if group_info else group_key
                
                # Get relevant columns for this group
                relevant_columns = self._get_relevant_columns_for_group(group_data, group_key, product_group_manager)
                group_export = group_data[relevant_columns]
                
                # Create JSON structure
                json_data = {
                    'metadata': {
                        'export_timestamp': datetime.now().isoformat(),
                        'product_group': group_key,
                        'group_name': group_name,
                        'total_records': len(group_export),
                        'columns': list(relevant_columns),
                        'mandatory_fields': product_group_manager.get_mandatory_fields(group_key) if product_group_manager else []
                    },
                    'data': []
                }
                
                # Convert DataFrame to records
                for _, row in group_export.iterrows():
                    record = {}
                    for col, value in row.items():
                        if pd.isna(value):
                            record[col] = None
                        elif isinstance(value, (int, float, str, bool)):
                            record[col] = value
                        else:
                            record[col] = str(value)
                    json_data['data'].append(record)
                
                # Save JSON
                safe_group_name = self._sanitize_filename(group_name)
                json_filename = f"bibliotecario_{safe_group_name}_{timestamp}.json"
                json_path = os.path.join(export_folder, json_filename)
                
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)
                
                if os.path.exists(json_path):
                    result['files_created'].append({
                        'format': 'json',
                        'path': json_path,
                        'filename': json_filename,
                        'size': os.path.getsize(json_path),
                        'product_group': group_key,
                        'group_name': group_name,
                        'records': len(group_export)
                    })
                    print(f"✅ JSON para {group_name}: {json_path}")
                    
        except Exception as e:
            result['errors'].append(f"Erro na exportação JSON por grupos: {str(e)}")
    
    def _export_json_single(self, dataframe, export_folder, timestamp, result):
        """Export single JSON file"""
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
    
    def _get_relevant_columns_for_group(self, group_data, group_key, product_group_manager):
        """Get only relevant columns for a specific product group"""
        relevant_columns = []
        
        # Always include original columns
        original_cols = [col for col in group_data.columns 
                        if not col.startswith('extracted_') and not col.endswith('_cleaned')]
        relevant_columns.extend(original_cols)
        
        # Get mandatory fields for this group
        mandatory_fields = product_group_manager.get_mandatory_fields(group_key) if product_group_manager else []
        
        # Add extracted columns that have data OR are mandatory
        extracted_cols = [col for col in group_data.columns if col.startswith('extracted_')]
        
        for col in extracted_cols:
            field_name = col.replace('extracted_', '')
            
            # Include if it's mandatory OR has some data
            is_mandatory = field_name in mandatory_fields
            has_data = group_data[col].notna().any()
            
            if is_mandatory or has_data:
                relevant_columns.append(col)
        
        return relevant_columns
    
    def _clean_data_for_excel(self, dataframe):
        """Clean DataFrame for Excel export"""
        clean_df = dataframe.copy()
        
        for col in clean_df.select_dtypes(include=['object']).columns:
            clean_df[col] = clean_df[col].astype(str)
            clean_df[col] = clean_df[col].apply(lambda x: x[:self.max_cell_length] if len(str(x)) > self.max_cell_length else x)
            clean_df[col] = clean_df[col].replace(['nan', 'None', 'null', 'NULL', '<NA>'], '')
        
        return clean_df
    
    def _sanitize_filename(self, name):
        """Create safe filename from group name"""
        # Remove or replace invalid characters
        safe_name = re.sub(r'[^\w\s-]', '', name)
        safe_name = re.sub(r'[\s_]+', '_', safe_name)
        return safe_name[:50]  # Limit length
    
    def _sanitize_sheet_name(self, name):
        """Create safe Excel sheet name"""
        # Excel sheet names have strict limits
        safe_name = re.sub(r'[^\w\s-]', '', name)
        safe_name = re.sub(r'[\s_]+', '_', safe_name)
        return safe_name[:30]  # Excel limit is 31 characters
    
    def _create_groups_summary_sheet(self, dataframe, product_group_manager, writer):
        """Create summary sheet with group information"""
        try:
            summary_data = []
            
            summary_data.append(['Relatório de Exportação por Grupos de Produto', ''])
            summary_data.append(['Data de Geração', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            summary_data.append(['Total de Registros', len(dataframe)])
            summary_data.append(['', ''])
            
            summary_data.append(['Grupo de Produto', 'Registros', 'Campos Obrigatórios', 'Campos Extraídos'])
            
            for group_key in dataframe['product_group'].unique():
                if pd.isna(group_key):
                    continue
                    
                group_data = dataframe[dataframe['product_group'] == group_key]
                group_info = product_group_manager.get_group_info(group_key) if product_group_manager else None
                group_name = group_info['name'] if group_info else group_key
                
                mandatory_fields = product_group_manager.get_mandatory_fields(group_key) if product_group_manager else []
                extracted_cols = [col for col in group_data.columns if col.startswith('extracted_') and group_data[col].notna().any()]
                
                summary_data.append([
                    group_name,
                    len(group_data),
                    len(mandatory_fields),
                    len(extracted_cols)
                ])
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Resumo_Grupos', index=False, header=False)
            
        except Exception as e:
            print(f"⚠️ Erro ao criar planilha de resumo: {str(e)}")
    
    def _create_field_mapping_sheet(self, dataframe, product_group_manager, writer):
        """Create sheet showing field mapping per group"""
        try:
            mapping_data = []
            mapping_data.append(['Grupo', 'Campo Obrigatório', 'Campo Extraído', 'Taxa de Preenchimento'])
            
            for group_key in dataframe['product_group'].unique():
                if pd.isna(group_key):
                    continue
                    
                group_data = dataframe[dataframe['product_group'] == group_key]
                group_info = product_group_manager.get_group_info(group_key) if product_group_manager else None
                group_name = group_info['name'] if group_info else group_key
                
                mandatory_fields = product_group_manager.get_mandatory_fields(group_key) if product_group_manager else []
                
                for field in mandatory_fields:
                    extracted_col = f'extracted_{field}'
                    if extracted_col in group_data.columns:
                        filled_count = group_data[extracted_col].notna().sum()
                        total_count = len(group_data)
                        fill_rate = (filled_count / total_count) * 100 if total_count > 0 else 0
                        
                        mapping_data.append([
                            group_name,
                            field.replace('_', ' ').title(),
                            extracted_col,
                            f"{fill_rate:.1f}%"
                        ])
            
            if len(mapping_data) > 1:  # More than just header
                mapping_df = pd.DataFrame(mapping_data[1:], columns=mapping_data[0])
                mapping_df.to_excel(writer, sheet_name='Mapeamento_Campos', index=False)
                
        except Exception as e:
            print(f"⚠️ Erro ao criar planilha de mapeamento: {str(e)}")
    
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
            download_entry = {
                'format': file_info['format'].upper(),
                'path': file_info['path'],
                'filename': file_info['filename'],
                'size_mb': round(file_info['size'] / (1024 * 1024), 2)
            }
            
            # Add product group info if available
            if 'product_group' in file_info:
                download_entry['product_group'] = file_info['product_group']
                download_entry['group_name'] = file_info['group_name']
                download_entry['records'] = file_info['records']
            
            # Add description if available
            if 'description' in file_info:
                download_entry['description'] = file_info['description']
            
            download_info['files'].append(download_entry)
        
        return download_info

# Backward compatibility
ExportHandler = EnhancedExportHandler