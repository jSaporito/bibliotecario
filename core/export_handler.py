import pandas as pd
import os
from datetime import datetime

class ExportHandler:
    """Class for handling data export to various formats"""
    
    def __init__(self):
        self.supported_formats = ['csv', 'excel', 'json']
    
    def export_data(self, dataframe, output_dir, filename_base, formats=['csv', 'excel']):
        """
        Export dataframe to multiple formats - ROBUST VERSION
        
        Args:
            dataframe (pd.DataFrame): Data to export
            output_dir (str): Directory to save files
            filename_base (str): Base filename (without extension)
            formats (list): List of formats to export ('csv', 'excel', 'json')
            
        Returns:
            dict: Export results with file paths and status
        """
        results = {
            'success': False,
            'files_created': [],
            'errors': []
        }
        
        try:
            # Ensure output directory exists with proper error handling
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                print(f"Created directory: {output_dir}")
            
            # Verify directory is writable
            if not os.access(output_dir, os.W_OK):
                results['errors'].append(f"Diretório sem permissão de escrita: {output_dir}")
                return results
            
            # Add timestamp to filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_with_timestamp = f"{filename_base}_{timestamp}"
            
            print(f"Exportando para: {output_dir}")
            print(f"Nome base do arquivo: {filename_with_timestamp}")
            print(f"Formatos: {formats}")
            
            # Always try CSV first (most reliable)
            if 'csv' in formats or 'both' in formats or 'all' in formats:
                try:
                    file_path = self._export_csv(dataframe, output_dir, filename_with_timestamp)
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path)
                        results['files_created'].append({
                            'format': 'csv',
                            'path': file_path,
                            'size': file_size
                        })
                        print(f"✓ Exportação CSV bem-sucedida: {file_path} ({file_size} bytes)")
                except Exception as e:
                    error_msg = f"Exportação CSV falhou: {str(e)}"
                    results['errors'].append(error_msg)
                    print(f"✗ {error_msg}")
            
            # Try Excel export (more complex, might fail)
            if any(fmt in formats for fmt in ['excel', 'both', 'all']):
                try:
                    file_path = self._export_excel(dataframe, output_dir, filename_with_timestamp)
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path)
                        results['files_created'].append({
                            'format': 'excel',
                            'path': file_path,
                            'size': file_size
                        })
                        print(f"✓ Exportação Excel bem-sucedida: {file_path} ({file_size} bytes)")
                except Exception as e:
                    error_msg = f"Exportação Excel falhou: {str(e)}"
                    results['errors'].append(error_msg)
                    print(f"✗ {error_msg}")
                    
                    # If Excel fails but CSV succeeded, that's still a partial success
                    if len(results['files_created']) > 0:
                        print("  Nota: Exportação CSV foi bem-sucedida, continuando...")
            
            # Try JSON export if requested
            if 'json' in formats or 'all' in formats:
                try:
                    file_path = self._export_json(dataframe, output_dir, filename_with_timestamp)
                    if os.path.exists(file_path):
                        file_size = os.path.getsize(file_path)
                        results['files_created'].append({
                            'format': 'json',
                            'path': file_path,
                            'size': file_size
                        })
                        print(f"✓ Exportação JSON bem-sucedida: {file_path} ({file_size} bytes)")
                except Exception as e:
                    error_msg = f"Exportação JSON falhou: {str(e)}"
                    results['errors'].append(error_msg)
                    print(f"✗ {error_msg}")
            
            # Success if at least one file was created
            results['success'] = len(results['files_created']) > 0
            
            if results['success']:
                print(f"✓ Exportação concluída: {len(results['files_created'])} arquivo(s) criado(s)")
            else:
                print("✗ Exportação falhou: Nenhum arquivo foi criado com sucesso")
                
        except Exception as e:
            error_msg = f"Erro geral de exportação: {str(e)}"
            results['errors'].append(error_msg)
            print(f"Erro do manipulador de exportação: {error_msg}")
        
        return results
    
    def _export_csv(self, dataframe, output_dir, filename_base):
        """Export dataframe to CSV format"""
        file_path = os.path.join(output_dir, f"{filename_base}.csv")
        print(f"Tentando criar CSV: {file_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            dataframe.to_csv(file_path, index=False, encoding='utf-8')
            print(f"Exportação CSV bem-sucedida: {file_path}")
            return file_path
        except Exception as e:
            print(f"Exportação CSV falhou: {str(e)}")
            raise
    
    def _export_excel(self, dataframe, output_dir, filename_base):
        """Export dataframe to Excel format with multiple sheets - SAFE VERSION"""
        file_path = os.path.join(output_dir, f"{filename_base}.xlsx")
        print(f"Tentando criar Excel: {file_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # Main data sheet - this is the most important
                print("  Escrevendo planilha de dados principal...")
                dataframe.to_excel(writer, sheet_name='Dados_Processados', index=False)
                
                try:
                    # Statistics sheet - if this fails, we still have the main data
                    print("  Criando planilha de estatísticas...")
                    stats_df = self._create_statistics_sheet(dataframe)
                    stats_df.to_excel(writer, sheet_name='Estatisticas', index=False)
                except Exception as stats_error:
                    print(f"  Aviso: Planilha de estatísticas falhou: {stats_error}")
                    # Create a simple error sheet instead
                    error_df = pd.DataFrame([
                        ['Erro', 'Geração de estatísticas falhou'],
                        ['Detalhes', str(stats_error)[:200]]
                    ])
                    error_df.to_excel(writer, sheet_name='Erro_Estatisticas', index=False)
                
                try:
                    # Sample data sheet - also optional
                    print("  Criando planilha de dados de amostra...")
                    sample_df = dataframe.head(100)
                    sample_df.to_excel(writer, sheet_name='Dados_Amostra', index=False)
                except Exception as sample_error:
                    print(f"  Aviso: Planilha de amostra falhou: {sample_error}")
            
            print(f"Exportação Excel bem-sucedida: {file_path}")
            return file_path
            
        except Exception as e:
            print(f"Exportação Excel falhou: {str(e)}")
            # Try a simpler Excel export without extra sheets
            try:
                print("  Tentando exportação Excel simples...")
                simple_path = file_path.replace('.xlsx', '_simples.xlsx')
                dataframe.to_excel(simple_path, index=False, engine='openpyxl')
                print(f"Exportação Excel simples bem-sucedida: {simple_path}")
                return simple_path
            except Exception as simple_error:
                print(f"Até exportação Excel simples falhou: {simple_error}")
                raise Exception(f"Exportação Excel falhou completamente: {str(e)} / {str(simple_error)}")
    
    
    def _export_json(self, dataframe, output_dir, filename_base):
        """Export dataframe to JSON format"""
        file_path = os.path.join(output_dir, f"{filename_base}.json")
        print(f"Attempting to create JSON: {file_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        try:
            dataframe.to_json(file_path, orient='records', indent=2)
            print(f"JSON export successful: {file_path}")
            return file_path
        except Exception as e:
            print(f"JSON export failed: {str(e)}")
            raise
    
    def _create_statistics_sheet(self, dataframe):
        """Create a statistics sheet for the exported data - SAFE VERSION"""
        stats_data = []
        
        try:
            # Basic info
            stats_data.append(['Metric', 'Value'])
            stats_data.append(['Total Rows', len(dataframe)])
            stats_data.append(['Total Columns', len(dataframe.columns)])
            stats_data.append(['Generated On', datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            stats_data.append(['', ''])  # Empty row
            
            # Column statistics
            stats_data.append(['Column Statistics', ''])
            stats_data.append(['Column Name', 'Non-Null Count', 'Null Count', 'Data Type'])
            
            for col in dataframe.columns:
                try:
                    # Safe way to count non-null values
                    non_null_series = dataframe[col].notna()
                    non_null_count = non_null_series.sum()
                    
                    # Convert to Python int safely
                    if hasattr(non_null_count, 'item'):
                        non_null = int(non_null_count.item())
                    else:
                        non_null = int(non_null_count)
                    
                    null_count = len(dataframe) - non_null
                    
                    # Safe dtype detection
                    try:
                        dtype = str(dataframe[col].dtype)
                    except:
                        dtype = 'mixed/unknown'
                    
                    stats_data.append([str(col), non_null, null_count, dtype])
                    
                except Exception as col_error:
                    # If individual column fails, add error info
                    stats_data.append([str(col), 'Error', 'Error', f'Error: {str(col_error)[:50]}'])
            
        except Exception as e:
            # If anything fails, create minimal stats
            stats_data = [
                ['Metric', 'Value'],
                ['Total Rows', len(dataframe) if hasattr(dataframe, '__len__') else 'Unknown'],
                ['Total Columns', len(dataframe.columns) if hasattr(dataframe, 'columns') else 'Unknown'],
                ['Generated On', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                ['', ''],
                ['Error', f'Statistics generation failed: {str(e)[:100]}']
            ]
        
        # Convert to DataFrame safely
        try:
            stats_df = pd.DataFrame(stats_data)
            return stats_df
        except Exception as df_error:
            # Return minimal DataFrame if even that fails
            return pd.DataFrame([
                ['Error', 'Statistics creation failed'],
                ['Details', str(df_error)[:100]]
            ])
    
    def create_download_info(self, export_results):
        """Create download information for the user"""
        if not export_results['success']:
            return None
        
        download_info = {
            'files': [],
            'total_size': 0,
            'formats_available': []
        }
        
        for file_info in export_results['files_created']:
            file_size_mb = file_info['size'] / (1024 * 1024)
            
            download_info['files'].append({
                'format': file_info['format'].upper(),
                'path': file_info['path'],
                'filename': os.path.basename(file_info['path']),
                'size_mb': round(file_size_mb, 2)
            })
            
            download_info['total_size'] += file_info['size']
            download_info['formats_available'].append(file_info['format'])
        
        download_info['total_size_mb'] = round(download_info['total_size'] / (1024 * 1024), 2)
        
        return download_info
    
    def cleanup_old_files(self, directory, max_age_hours=24):
        """Clean up old exported files"""
        if not os.path.exists(directory):
            return
        
        current_time = datetime.now()
        files_removed = 0
        
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            
            if os.path.isfile(file_path):
                file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                age_hours = (current_time - file_time).total_seconds() / 3600
                
                if age_hours > max_age_hours:
                    try:
                        os.remove(file_path)
                        files_removed += 1
                    except Exception:
                        pass  # Ignore errors when cleaning up
        
        return files_removed