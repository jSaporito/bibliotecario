import pandas as pd
import json
import os
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class EnhancedExportHandler:
    """Enhanced export handler with product group separation - MANDATORY FIELDS ONLY"""
    
    def __init__(self):
        self.max_cell_length = 32000
    
    def export_data(self, dataframe, output_dir, filename_base, formats=['csv', 'excel'], product_group_manager=None):
        """Enhanced export with product group separation - MANDATORY FIELDS + ID + HOSTING TYPE ONLY"""
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
                self._export_csv_mandatory_only(dataframe, export_folder, timestamp, product_group_manager, result)
            
            # Export Excel (consolidated with product group sheets - MANDATORY FIELDS ONLY)
            if 'excel' in formats:
                if has_product_groups:
                    self._export_excel_by_groups_mandatory_only(dataframe, export_folder, timestamp, product_group_manager, result)
                else:
                    self._export_excel_single_mandatory_only(dataframe, export_folder, timestamp, product_group_manager, result)
            
            # Export JSON (single file only, no group separation)
            if 'json' in formats:
                self._export_json_mandatory_only(dataframe, export_folder, timestamp, product_group_manager, result)
            
            result['success'] = len(result['files_created']) > 0
            
        except Exception as e:
            print(f"❌ Export error: {str(e)}")
            result['errors'].append(str(e))
        
        return result
    
    def _get_mandatory_columns_only(self, dataframe, group_key, product_group_manager):
        """Get ONLY mandatory columns for a specific product group + ID + hosting type"""
        mandatory_columns = []
        
        # ALWAYS include ID and hosting type from original CSV if they exist
        original_priority_columns = ['id', 'ID', 'Id', 'hosting_type', 'hosting_Type', 'Hosting_Type', 'HOSTING_TYPE']
        for col in original_priority_columns:
            if col in dataframe.columns:
                mandatory_columns.append(col)
        
        # Add product_group column if it exists
        if 'product_group' in dataframe.columns:
            mandatory_columns.append('product_group')
        
        # Get mandatory fields for this group from the product group manager
        if product_group_manager and group_key:
            mandatory_fields = product_group_manager.get_mandatory_fields(group_key)
            print(f"   Mandatory fields for {group_key}: {mandatory_fields}")
            
            # Add only the extracted versions of mandatory fields that actually exist and have data
            for field in mandatory_fields:
                extracted_field = f'extracted_{field}'
                if extracted_field in dataframe.columns:
                    # Check if this field has any non-null data in this group
                    group_data = dataframe[dataframe['product_group'] == group_key] if 'product_group' in dataframe.columns else dataframe
                    if group_data[extracted_field].notna().any():
                        mandatory_columns.append(extracted_field)
                        print(f"      Including {extracted_field} (has data)")
                    else:
                        print(f"      Skipping {extracted_field} (no data)")
        else:
            # If no product group manager or group key, include all extracted fields that have data
            extracted_cols = [col for col in dataframe.columns if col.startswith('extracted_')]
            for col in extracted_cols:
                if dataframe[col].notna().any():
                    mandatory_columns.append(col)
        
        # Remove duplicates while preserving order
        unique_columns = []
        for col in mandatory_columns:
            if col not in unique_columns:
                unique_columns.append(col)
        
        print(f"   Final columns for export: {unique_columns}")
        return unique_columns
    
    def _export_csv_mandatory_only(self, dataframe, export_folder, timestamp, product_group_manager, result):
        """Export single CSV file with only mandatory fields + ID + hosting type"""
        try:
            # Get all unique mandatory fields across all groups + priority columns
            all_mandatory_columns = set()
            
            # Always include priority columns if they exist
            priority_columns = ['id', 'ID', 'Id', 'hosting_type', 'hosting_Type', 'Hosting_Type', 'HOSTING_TYPE']
            for col in priority_columns:
                if col in dataframe.columns:
                    all_mandatory_columns.add(col)
            
            # Add product_group if exists
            if 'product_group' in dataframe.columns:
                all_mandatory_columns.add('product_group')
            
            # Get mandatory fields for each group
            if product_group_manager and 'product_group' in dataframe.columns:
                for group_key in dataframe['product_group'].dropna().unique():
                    mandatory_fields = product_group_manager.get_mandatory_fields(group_key)
                    for field in mandatory_fields:
                        extracted_field = f'extracted_{field}'
                        if extracted_field in dataframe.columns and dataframe[extracted_field].notna().any():
                            all_mandatory_columns.add(extracted_field)
            else:
                # Include all extracted fields that have data
                for col in dataframe.columns:
                    if col.startswith('extracted_') and dataframe[col].notna().any():
                        all_mandatory_columns.add(col)
            
            # Filter dataframe to only include mandatory columns that exist
            final_columns = [col for col in dataframe.columns if col in all_mandatory_columns]
            mandatory_only_df = dataframe[final_columns].copy()
            
            csv_filename = f"bibliotecario_mandatory_fields_{timestamp}.csv"
            csv_path = os.path.join(export_folder, csv_filename)
            
            mandatory_only_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            if os.path.exists(csv_path):
                result['files_created'].append({
                    'format': 'csv',
                    'path': csv_path,
                    'filename': csv_filename,
                    'size': os.path.getsize(csv_path),
                    'description': 'Apenas campos obrigatórios + ID + hosting type'
                })
                print(f"✅ CSV (mandatory only): {csv_path}")
                print(f"   Columns included: {len(final_columns)} - {final_columns}")
                
        except Exception as e:
            result['errors'].append(f"Erro na exportação CSV (mandatory only): {str(e)}")
    
    def _export_excel_by_groups_mandatory_only(self, dataframe, export_folder, timestamp, product_group_manager, result):
        """Export Excel file with separate sheets for each product group - MANDATORY FIELDS ONLY"""
        try:
            excel_filename = f"bibliotecario_mandatory_by_groups_{timestamp}.xlsx"
            excel_path = os.path.join(export_folder, excel_filename)
            
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Summary sheet first
                self._create_mandatory_groups_summary_sheet(dataframe, product_group_manager, writer)
                
                # Create sheet for each product group - MANDATORY ONLY
                for group_key in dataframe['product_group'].unique():
                    if pd.isna(group_key):
                        continue
                        
                    group_data = dataframe[dataframe['product_group'] == group_key].copy()
                    group_info = product_group_manager.get_group_info(group_key)
                    group_name = group_info['name'] if group_info else group_key
                    
                    # Get ONLY mandatory columns for this group
                    mandatory_columns = self._get_mandatory_columns_only(group_data, group_key, product_group_manager)
                    
                    # Filter group data to only mandatory columns
                    if mandatory_columns:
                        group_export = group_data[mandatory_columns]
                    else:
                        # Fallback: at least include ID and hosting type if they exist
                        fallback_cols = []
                        for col in ['id', 'ID', 'Id', 'hosting_type', 'hosting_Type', 'product_group']:
                            if col in group_data.columns:
                                fallback_cols.append(col)
                        group_export = group_data[fallback_cols] if fallback_cols else group_data.head(0)
                    
                    # Clean data for Excel
                    clean_group_data = self._clean_data_for_excel(group_export)
                    
                    # Create safe sheet name (Excel limit: 31 characters)
                    safe_sheet_name = self._sanitize_sheet_name(group_name)
                    
                    # Write to sheet
                    clean_group_data.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    
                    print(f"✅ Sheet created for {group_name}: {len(clean_group_data)} records, {len(mandatory_columns)} mandatory columns")
                
                # Create mandatory field mapping sheet
                self._create_mandatory_field_mapping_sheet(dataframe, product_group_manager, writer)
            
            if os.path.exists(excel_path):
                result['files_created'].append({
                    'format': 'excel',
                    'path': excel_path,
                    'filename': excel_filename,
                    'size': os.path.getsize(excel_path),
                    'description': 'Excel com planilhas por grupo - apenas campos obrigatórios + ID + hosting type'
                })
                print(f"✅ Excel (mandatory by groups): {excel_path}")
                
        except Exception as e:
            result['errors'].append(f"Erro na exportação Excel por grupos (mandatory only): {str(e)}")
            print(f"❌ Erro Excel por grupos: {str(e)}")
    
    def _export_excel_single_mandatory_only(self, dataframe, export_folder, timestamp, product_group_manager, result):
        """Export single Excel file with only mandatory fields + ID + hosting type"""
        try:
            # Get mandatory columns
            all_mandatory_columns = set()
            
            # Always include priority columns if they exist
            priority_columns = ['id', 'ID', 'Id', 'hosting_type', 'hosting_Type', 'Hosting_Type', 'HOSTING_TYPE']
            for col in priority_columns:
                if col in dataframe.columns:
                    all_mandatory_columns.add(col)
            
            # Include extracted fields that have data
            for col in dataframe.columns:
                if col.startswith('extracted_') and dataframe[col].notna().any():
                    all_mandatory_columns.add(col)
            
            # Filter to existing columns
            final_columns = [col for col in dataframe.columns if col in all_mandatory_columns]
            mandatory_only_df = dataframe[final_columns].copy()
            
            excel_filename = f"bibliotecario_mandatory_fields_{timestamp}.xlsx"
            excel_path = os.path.join(export_folder, excel_filename)
            
            # Clean data for Excel
            clean_df = self._clean_data_for_excel(mandatory_only_df)
            
            # Create Excel with multiple sheets
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                # Main data sheet
                clean_df.to_excel(writer, sheet_name='Mandatory_Fields_Only', index=False)
                
                # Summary sheet
                summary_data = self._create_mandatory_summary_data(dataframe, final_columns)
                if summary_data:
                    summary_df = pd.DataFrame(summary_data, columns=['Metric', 'Value'])
                    summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            if os.path.exists(excel_path):
                result['files_created'].append({
                    'format': 'excel',
                    'path': excel_path,
                    'filename': excel_filename,
                    'size': os.path.getsize(excel_path),
                    'description': 'Apenas campos obrigatórios + ID + hosting type'
                })
                print(f"✅ Excel (mandatory only): {excel_path}")
                print(f"   Columns included: {len(final_columns)} - {final_columns}")
                
        except Exception as e:
            result['errors'].append(f"Erro na exportação Excel (mandatory only): {str(e)}")
    
    def _export_json_mandatory_only(self, dataframe, export_folder, timestamp, product_group_manager, result):
        """Export JSON file with only mandatory fields + ID + hosting type"""
        try:
            # Get mandatory columns
            all_mandatory_columns = set()
            
            # Always include priority columns if they exist
            priority_columns = ['id', 'ID', 'Id', 'hosting_type', 'hosting_Type', 'Hosting_Type', 'HOSTING_TYPE']
            for col in priority_columns:
                if col in dataframe.columns:
                    all_mandatory_columns.add(col)
            
            # Add product_group if exists
            if 'product_group' in dataframe.columns:
                all_mandatory_columns.add('product_group')
            
            # Get mandatory fields
            if product_group_manager and 'product_group' in dataframe.columns:
                for group_key in dataframe['product_group'].dropna().unique():
                    mandatory_fields = product_group_manager.get_mandatory_fields(group_key)
                    for field in mandatory_fields:
                        extracted_field = f'extracted_{field}'
                        if extracted_field in dataframe.columns and dataframe[extracted_field].notna().any():
                            all_mandatory_columns.add(extracted_field)
            else:
                # Include all extracted fields that have data
                for col in dataframe.columns:
                    if col.startswith('extracted_') and dataframe[col].notna().any():
                        all_mandatory_columns.add(col)
            
            # Filter to existing columns
            final_columns = [col for col in dataframe.columns if col in all_mandatory_columns]
            mandatory_only_df = dataframe[final_columns].copy()
            
            # Create JSON structure
            json_data = {
                'metadata': {
                    'export_timestamp': datetime.now().isoformat(),
                    'export_type': 'mandatory_fields_only',
                    'total_records': len(mandatory_only_df),
                    'columns_included': final_columns,
                    'description': 'Export contains only mandatory fields + ID + hosting type',
                    'mandatory_fields_note': 'Only fields that are mandatory per product group + ID + hosting type from original CSV'
                },
                'data': []
            }
            
            # Convert DataFrame to records (limit to avoid huge files)
            sample_size = min(5000, len(mandatory_only_df))  # Limit JSON to first 5000 records
            for _, row in mandatory_only_df.head(sample_size).iterrows():
                record = {}
                for col, value in row.items():
                    if pd.isna(value):
                        record[col] = None
                    elif isinstance(value, (int, float, str, bool)):
                        record[col] = value
                    else:
                        record[col] = str(value)
                json_data['data'].append(record)
            
            # Add summary if we limited the data
            if len(mandatory_only_df) > sample_size:
                json_data['summary'] = {
                    'total_records_in_file': len(mandatory_only_df),
                    'records_in_json': sample_size,
                    'note': f'JSON limited to first {sample_size} records for file size management'
                }
            
            json_filename = f"bibliotecario_mandatory_fields_{timestamp}.json"
            json_path = os.path.join(export_folder, json_filename)
            
            # Write JSON
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            if os.path.exists(json_path):
                result['files_created'].append({
                    'format': 'json',
                    'path': json_path,
                    'filename': json_filename,
                    'size': os.path.getsize(json_path),
                    'description': 'Apenas campos obrigatórios + ID + hosting type'
                })
                print(f"✅ JSON (mandatory only): {json_path}")
                print(f"   Columns included: {len(final_columns)} - {final_columns}")
                
        except Exception as e:
            result['errors'].append(f"Erro na exportação JSON (mandatory only): {str(e)}")
    
    def _create_mandatory_groups_summary_sheet(self, dataframe, product_group_manager, writer):
        """Create summary sheet with mandatory fields information"""
        try:
            summary_data = []
            
            summary_data.append(['Relatório de Exportação - Apenas Campos Obrigatórios', ''])
            summary_data.append(['Data de Geração', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            summary_data.append(['Total de Registros', len(dataframe)])
            summary_data.append(['Tipo de Exportação', 'Apenas Campos Obrigatórios + ID + Hosting Type'])
            summary_data.append(['', ''])
            
            summary_data.append(['Grupo de Produto', 'Registros', 'Campos Obrigatórios', 'Campos Exportados'])
            
            for group_key in dataframe['product_group'].unique():
                if pd.isna(group_key):
                    continue
                    
                group_data = dataframe[dataframe['product_group'] == group_key]
                group_info = product_group_manager.get_group_info(group_key) if product_group_manager else None
                group_name = group_info['name'] if group_info else group_key
                
                mandatory_fields = product_group_manager.get_mandatory_fields(group_key) if product_group_manager else []
                mandatory_columns = self._get_mandatory_columns_only(group_data, group_key, product_group_manager)
                
                summary_data.append([
                    group_name,
                    len(group_data),
                    len(mandatory_fields),
                    len(mandatory_columns)
                ])
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Resumo_Mandatory', index=False, header=False)
            
        except Exception as e:
            print(f"⚠️ Erro ao criar planilha de resumo: {str(e)}")
    
    def _create_mandatory_field_mapping_sheet(self, dataframe, product_group_manager, writer):
        """Create sheet showing mandatory field mapping per group"""
        try:
            mapping_data = []
            mapping_data.append(['Grupo', 'Campo Obrigatório', 'Campo Extraído', 'Incluído na Exportação', 'Motivo'])
            
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
                        has_data = group_data[extracted_col].notna().any()
                        included = has_data
                        reason = "Tem dados" if has_data else "Sem dados"
                    else:
                        included = False
                        reason = "Campo não encontrado"
                    
                    mapping_data.append([
                        group_name,
                        field.replace('_', ' ').title(),
                        extracted_col,
                        "Sim" if included else "Não",
                        reason
                    ])
            
            if len(mapping_data) > 1:  # More than just header
                mapping_df = pd.DataFrame(mapping_data[1:], columns=mapping_data[0])
                mapping_df.to_excel(writer, sheet_name='Mapeamento_Mandatory', index=False)
                
        except Exception as e:
            print(f"⚠️ Erro ao criar planilha de mapeamento: {str(e)}")
    
    def _create_mandatory_summary_data(self, df, included_columns):
        """Create summary data for mandatory fields export"""
        try:
            summary = [
                ['Export Timestamp', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['Export Type', 'Mandatory Fields Only'],
                ['Total Records', len(df)],
                ['Columns Included', len(included_columns)],
                ['', ''],  # Empty row
            ]
            
            # Add column info
            summary.append(['Column Information', ''])
            
            # Count different types of columns
            priority_cols = [col for col in included_columns if col.lower() in ['id', 'hosting_type', 'product_group']]
            extracted_cols = [col for col in included_columns if col.startswith('extracted_')]
            
            summary.extend([
                ['Priority Columns (ID, hosting_type, etc.)', len(priority_cols)],
                ['Mandatory Extracted Fields', len(extracted_cols)],
                ['Total Exported Columns', len(included_columns)],
                ['', ''],
                ['Included Columns:', ''],
            ])
            
            # List all included columns
            for col in included_columns:
                col_type = "Priority" if col.lower() in ['id', 'hosting_type', 'product_group'] else "Extracted"
                summary.append([col, col_type])
            
            return summary
            
        except Exception as e:
            print(f"Warning: Could not create summary data: {e}")
            return None
    
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
    
    def create_download_info(self, export_results):
        """Create download information for the web interface"""
        if not export_results['success']:
            return None
        
        download_info = {
            'files': [],
            'total_files': len(export_results['files_created']),
            'timestamp': datetime.now().isoformat(),
            'export_type': 'mandatory_fields_only'
        }
        
        for file_info in export_results['files_created']:
            download_entry = {
                'format': file_info['format'].upper(),
                'path': file_info['path'],
                'filename': file_info['filename'],
                'size_mb': round(file_info['size'] / (1024 * 1024), 2)
            }
            
            # Add description
            if 'description' in file_info:
                download_entry['description'] = file_info['description']
            
            download_info['files'].append(download_entry)
        
        return download_info

# Backward compatibility
ExportHandler = EnhancedExportHandler