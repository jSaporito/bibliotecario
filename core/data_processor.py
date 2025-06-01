"""
Group-Based Data Processor - Refactored
Main processor that orchestrates group-based cleaning and extraction
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class GroupBasedDataProcessor:
    """
    Enhanced data processor focused on product group classification
    Orchestrates group-based cleaning, extraction, and analysis
    """
    
    def __init__(self, chunk_size=5000, product_group_manager=None):
        self.chunk_size = chunk_size
        
        # Import dependencies with product group manager
        if product_group_manager:
            self.group_manager = product_group_manager
        else:
            from core.product_groups import product_group_manager
            self.group_manager = product_group_manager
        
        # Initialize processors with group manager
        from core.text_cleaner import GroupBasedTextCleaner
        from core.text_extractor import GroupBasedTextExtractor
        
        self.cleaner = GroupBasedTextCleaner(self.group_manager)
        self.extractor = GroupBasedTextExtractor(self.group_manager)
        
        # Processing statistics focused on groups
        self.stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'product_groups_found': {},
            'group_processing_summary': {},
            'mandatory_field_coverage': {},
            'chunks_processed': 0,
            'processing_time': None,
            'errors': [],
            'warnings': [],
            'quality_metrics': {}
        }
    
    def process_csv_by_groups(self, file_path, obs_column='obs', product_group_column='product_group',
                             enable_cleaning=True, enable_extraction=True, progress_callback=None):
        """
        Process CSV file with group-based optimization
        """
        start_time = datetime.now()
        
        def update_progress(message, progress=None):
            if progress_callback:
                progress_callback(message, progress)
            else:
                print(f"📊 Progress: {message}")
        
        try:
            update_progress("Iniciando processamento baseado em grupos de produto...", 5)
            
            # Step 1: Load and validate data
            df = self._load_and_validate_csv(file_path, obs_column, product_group_column)
            self.stats['total_rows'] = len(df)
            
            update_progress(f"Dados carregados: {len(df):,} registros", 10)
            
            # Step 2: Analyze product groups in dataset
            group_validation = self._analyze_product_groups(df, product_group_column)
            update_progress(f"Grupos de produto analisados: {group_validation['total_groups']} encontrados", 15)
            
            # Step 3: Process by groups (chunked if large dataset)
            if len(df) > self.chunk_size:
                update_progress("Processamento em lotes por grupo de produto...", 20)
                df = self._process_in_chunks_by_groups(df, obs_column, product_group_column, 
                                                    enable_cleaning, enable_extraction, progress_callback)
            else:
                update_progress("Processamento direto por grupo de produto...", 30)
                df = self._process_by_groups(df, obs_column, product_group_column, 
                                           enable_cleaning, enable_extraction)
                self.stats['chunks_processed'] = 1
            
            update_progress("Analisando completude de campos obrigatórios...", 85)
            
            # Step 4: Analyze mandatory field completeness by group
            self._analyze_mandatory_field_completeness(df, product_group_column)
            
            # Step 5: Calculate quality metrics by group
            self._calculate_group_quality_metrics(df, product_group_column)
            
            # Step 6: Final cleanup and summary
            df = self._finalize_group_processing(df, product_group_column)
            
            # Calculate processing time
            end_time = datetime.now()
            processing_time = end_time - start_time
            self.stats['processing_time'] = str(processing_time).split('.')[0]
            self.stats['processed_rows'] = len(df)
            
            update_progress("Processamento baseado em grupos concluído com sucesso!", 100)
            
            return {
                'success': True,
                'dataframe': df,
                'stats': self.stats,
                'message': 'Processamento por grupos de produto concluído com sucesso',
                'group_summary': self._generate_group_processing_summary()
            }
            
        except Exception as e:
            error_msg = f"Processamento por grupos falhou: {str(e)}"
            print(f"❌ {error_msg}")
            self.stats['errors'].append(error_msg)
            
            return {
                'success': False,
                'dataframe': None,
                'stats': self.stats,
                'errors': self.stats['errors'],
                'message': error_msg
            }
    
    def _load_and_validate_csv(self, file_path, obs_column, product_group_column):
        """
        Load CSV and validate required columns with encoding detection
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        # Try multiple encodings
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                print(f"✅ CSV carregado com codificação {encoding}")
                break
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        if df is None:
            raise Exception("Não foi possível carregar o CSV com nenhuma codificação suportada")
        
        # Validate required columns
        if obs_column not in df.columns:
            available_cols = ', '.join(df.columns[:5])
            raise ValueError(f"Coluna '{obs_column}' não encontrada. Disponíveis: {available_cols}...")
        
        if product_group_column not in df.columns:
            self.stats['warnings'].append(f"Coluna de grupo de produto '{product_group_column}' não encontrada. Processamento genérico será usado.")
            print(f"⚠️ Coluna '{product_group_column}' não encontrada. Adicionando coluna genérica.")
            df[product_group_column] = 'generic_group'
        
        return self._fix_data_types(df)
    
    def _fix_data_types(self, df):
        """
        Fix problematic data types for processing
        """
        try:
            df_fixed = df.copy()
            
            # Convert object columns with mixed types to string
            for col in df_fixed.columns:
                if df_fixed[col].dtype == 'object':
                    try:
                        # Check for mixed types
                        sample = df_fixed[col].dropna().head(100)
                        if len(sample) > 0:
                            types = set(type(x).__name__ for x in sample)
                            if len(types) > 1:
                                df_fixed[col] = df_fixed[col].astype(str)
                                df_fixed[col] = df_fixed[col].replace('nan', '')
                    except Exception:
                        continue
            
            print("✅ Tipos de dados corrigidos")
            return df_fixed
            
        except Exception as e:
            print(f"⚠️ Aviso na correção de tipos: {str(e)}")
            return df
    
    def _analyze_product_groups(self, df, product_group_column):
        """
        Analyze and validate product groups in the dataset
        """
        try:
            # Validate product groups using the manager
            validation_result = self.group_manager.validate_group_data(df, product_group_column)
            
            if validation_result['valid']:
                print(f"✅ Todos os grupos de produto são válidos")
            else:
                print(f"⚠️ Alguns grupos inválidos encontrados: {validation_result['invalid_groups']}")
                self.stats['warnings'].append(f"Grupos inválidos: {', '.join(validation_result['invalid_groups'])}")
            
            # Analyze distribution of groups
            product_groups = df[product_group_column].value_counts()
            print(f"📊 Distribuição de grupos de produto:")
            
            for group, count in product_groups.items():
                if pd.notna(group):
                    group_info = self.group_manager.get_group_info(group)
                    display_name = group_info['name'] if group_info else group
                    category = group_info.get('category', 'unknown') if group_info else 'unknown'
                    priority = group_info.get('priority_level', 'medium') if group_info else 'medium'
                    
                    print(f"   {display_name}: {count:,} registros ({category}, {priority})")
                    
                    self.stats['product_groups_found'][group] = {
                        'display_name': display_name,
                        'category': category,
                        'priority_level': priority,
                        'record_count': int(count),
                        'percentage': round((count / len(df)) * 100, 2),
                        'mandatory_fields': self.group_manager.get_mandatory_fields(group),
                        'mandatory_field_count': len(self.group_manager.get_mandatory_fields(group))
                    }
            
            return validation_result
            
        except Exception as e:
            self.stats['warnings'].append(f"Erro na análise de grupos: {str(e)}")
            return {'valid': False, 'error': str(e)}
    
    def _process_by_groups(self, df, obs_column, product_group_column, enable_cleaning, enable_extraction):
        """
        Process DataFrame by product groups with optimized rules per group
        """
        # Group by product group and process each separately
        def process_group(group):
            product_group = group[product_group_column].iloc[0] if len(group) > 0 else None
            group_name = self.group_manager.get_group_display_name(product_group)
            
            print(f"🔄 Processando grupo: {group_name} ({len(group)} registros)")
            
            # Step 1: Group-specific text cleaning
            if enable_cleaning:
                print(f"   🧹 Limpeza especializada para {group_name}")
                group = self.cleaner.clean_dataframe_by_groups(
                    group, obs_column, product_group_column
                )
            
            # Step 2: Group-specific field extraction (focus on mandatory fields)
            if enable_extraction:
                print(f"   🔍 Extração focada em campos obrigatórios para {group_name}")
                group = self.extractor.extract_dataframe_by_groups(
                    group, 
                    obs_column + '_cleaned' if enable_cleaning else obs_column,
                    product_group_column
                )
            
            # Step 3: Calculate group-specific metrics
            self._calculate_group_metrics(group, product_group, obs_column, enable_cleaning, enable_extraction)
            
            return group
        
        # Apply group-specific processing
        processed_df = df.groupby(product_group_column, group_keys=False).apply(process_group)
        
        return processed_df
    
    def _process_in_chunks_by_groups(self, df, obs_column, product_group_column, 
                                   enable_cleaning, enable_extraction, progress_callback):
        """
        Process large datasets in chunks while maintaining group integrity
        """
        def update_chunk_progress(message, chunk_progress):
            overall_progress = 20 + (chunk_progress / 100) * 65
            if progress_callback:
                progress_callback(message, overall_progress)
        
        # Sort by product group to keep groups together
        df_sorted = df.sort_values(by=product_group_column)
        
        chunks = []
        total_chunks = (len(df_sorted) + self.chunk_size - 1) // self.chunk_size
        
        for i in range(0, len(df_sorted), self.chunk_size):
            chunk_num = i // self.chunk_size + 1
            chunk = df_sorted.iloc[i:i + self.chunk_size].copy()
            
            update_chunk_progress(f"Processando lote {chunk_num}/{total_chunks} por grupos...", 
                                (chunk_num / total_chunks) * 100)
            
            # Process this chunk by groups
            processed_chunk = self._process_by_groups(chunk, obs_column, product_group_column,
                                                    enable_cleaning, enable_extraction)
            chunks.append(processed_chunk)
            
            self.stats['chunks_processed'] += 1
        
        # Combine all chunks
        if progress_callback:
            progress_callback("Combinando lotes processados...", 80)
        
        combined_df = pd.concat(chunks, ignore_index=True)
        
        # Restore original order if needed (optional)
        if 'original_index' in df.columns:
            combined_df = combined_df.sort_values('original_index').drop('original_index', axis=1)
        
        return combined_df
    
    def _calculate_group_metrics(self, group, product_group, obs_column, enable_cleaning, enable_extraction):
        """
        Calculate processing metrics for a specific product group
        """
        group_name = product_group if product_group else 'generic'
        
        metrics = {
            'total_records': len(group),
            'group_display_name': self.group_manager.get_group_display_name(product_group),
            'category': self.group_manager.get_group_category(product_group),
            'priority_level': self.group_manager.get_group_priority_level(product_group)
        }
        
        # Cleaning metrics
        if enable_cleaning and obs_column + '_cleaned' in group.columns:
            original_chars = group[obs_column].astype(str).str.len().sum()
            cleaned_chars = group[obs_column + '_cleaned'].astype(str).str.len().sum()
            
            metrics['cleaning'] = {
                'original_chars': original_chars,
                'cleaned_chars': cleaned_chars,
                'reduction_percent': round(100 * (1 - cleaned_chars / original_chars), 2) if original_chars > 0 else 0,
                'avg_reduction_per_record': round((original_chars - cleaned_chars) / len(group), 2) if len(group) > 0 else 0
            }
        
        # Extraction metrics (focus on mandatory fields)
        if enable_extraction:
            mandatory_fields = self.group_manager.get_mandatory_fields(product_group)
            extracted_cols = [col for col in group.columns if col.startswith('extracted_')]
            
            mandatory_extractions = 0
            total_extractions = 0
            mandatory_coverage = {}
            
            for field in mandatory_fields:
                field_col = f'extracted_{field}'
                if field_col in group.columns:
                    filled_count = group[field_col].notna().sum()
                    coverage_rate = (filled_count / len(group)) * 100 if len(group) > 0 else 0
                    mandatory_coverage[field] = coverage_rate
                    if coverage_rate > 50:  # Consider >50% as successful
                        mandatory_extractions += 1
            
            # Total extractions across all fields
            for col in extracted_cols:
                total_extractions += group[col].notna().sum()
            
            metrics['extraction'] = {
                'mandatory_fields_count': len(mandatory_fields),
                'mandatory_fields_successful': mandatory_extractions,
                'mandatory_coverage_rate': round((mandatory_extractions / len(mandatory_fields)) * 100, 2) if mandatory_fields else 0,
                'total_extractions': total_extractions,
                'avg_extractions_per_record': round(total_extractions / len(group), 2) if len(group) > 0 else 0,
                'mandatory_field_coverage': mandatory_coverage
            }
        
        self.stats['group_processing_summary'][group_name] = metrics
    
    def _analyze_mandatory_field_completeness(self, df, product_group_column):
        """
        Analyze mandatory field completeness using the group manager
        """
        try:
            completeness_analysis = self.group_manager.analyze_group_completeness(df, product_group_column)
            self.stats['mandatory_field_coverage'] = completeness_analysis
            
            print("📊 Análise de completude de campos obrigatórios:")
            for group_key, analysis in completeness_analysis.items():
                if 'error' in analysis:
                    continue
                    
                print(f"   {analysis['name']}:")
                print(f"      Registros: {analysis['total_records']:,}")
                print(f"      Completude geral: {analysis['overall_completeness']:.1f}%")
                
                # Show fields with low completeness
                low_completeness = [
                    (field, stats) for field, stats in analysis['completeness_stats'].items()
                    if stats['completeness_rate'] < 70
                ]
                
                if low_completeness:
                    print(f"      Campos que precisam de atenção:")
                    for field, stats in low_completeness[:3]:
                        print(f"         {field}: {stats['completeness_rate']:.1f}% completo")
                
        except Exception as e:
            self.stats['warnings'].append(f"Erro na análise de completude: {str(e)}")
    
    def _calculate_group_quality_metrics(self, df, product_group_column):
        """
        Calculate overall quality metrics by product group
        """
        quality_metrics = {}
        
        for group_key in df[product_group_column].unique():
            if pd.isna(group_key):
                continue
                
            group_data = df[df[product_group_column] == group_key]
            group_name = self.group_manager.get_group_display_name(group_key)
            
            # Calculate quality score based on multiple factors
            quality_score = 0
            
            # Factor 1: Mandatory field completeness (40% weight)
            if group_key in self.stats['mandatory_field_coverage']:
                completeness = self.stats['mandatory_field_coverage'][group_key].get('overall_completeness', 0)
                quality_score += (completeness / 100) * 40
            
            # Factor 2: Data cleaning effectiveness (20% weight)
            if group_key in self.stats['group_processing_summary']:
                cleaning_metrics = self.stats['group_processing_summary'][group_key].get('cleaning', {})
                reduction_percent = cleaning_metrics.get('reduction_percent', 0)
                # Optimal cleaning is 10-30% reduction
                if 10 <= reduction_percent <= 30:
                    quality_score += 20
                elif reduction_percent > 0:
                    quality_score += min(20, reduction_percent / 2)
            
            # Factor 3: Extraction coverage (30% weight)
            if group_key in self.stats['group_processing_summary']:
                extraction_metrics = self.stats['group_processing_summary'][group_key].get('extraction', {})
                coverage_rate = extraction_metrics.get('mandatory_coverage_rate', 0)
                quality_score += (coverage_rate / 100) * 30
            
            # Factor 4: Data consistency (10% weight)
            # Check for consistent data types in extracted fields
            consistency_score = 0
            extracted_cols = [col for col in group_data.columns if col.startswith('extracted_')]
            if extracted_cols:
                valid_extractions = 0
                for col in extracted_cols:
                    non_null_count = group_data[col].notna().sum()
                    if non_null_count > 0:
                        # Check if values are consistent (not empty strings, reasonable lengths)
                        valid_values = group_data[col].dropna().astype(str)
                        valid_values = valid_values[valid_values.str.len() > 0]
                        if len(valid_values) >= non_null_count * 0.8:  # 80% of non-null should be valid
                            valid_extractions += 1
                
                if extracted_cols:
                    consistency_score = (valid_extractions / len(extracted_cols)) * 10
            
            quality_score += consistency_score
            
            quality_metrics[group_key] = {
                'group_display_name': group_name,
                'category': self.group_manager.get_group_category(group_key),
                'priority_level': self.group_manager.get_group_priority_level(group_key),
                'quality_score': round(quality_score, 2),
                'quality_grade': self._get_quality_grade(quality_score),
                'total_records': len(group_data),
                'recommendations': self._generate_quality_recommendations(group_key, quality_score)
            }
        
        self.stats['quality_metrics'] = quality_metrics
    
    def _get_quality_grade(self, quality_score):
        """
        Convert quality score to letter grade
        """
        if quality_score >= 90:
            return 'A'
        elif quality_score >= 80:
            return 'B'
        elif quality_score >= 70:
            return 'C'
        elif quality_score >= 60:
            return 'D'
        else:
            return 'F'
    
    def _generate_quality_recommendations(self, group_key, quality_score):
        """
        Generate specific recommendations based on quality score
        """
        recommendations = []
        
        if quality_score < 60:
            recommendations.append("Crítico: Revisar padrões de extração e limpeza para este grupo")
        elif quality_score < 75:
            recommendations.append("Melhorar: Otimizar extração de campos obrigatórios")
        elif quality_score >= 90:
            recommendations.append("Excelente: Grupo processado com alta qualidade")
        
        # Group-specific recommendations
        group_info = self.group_manager.get_group_info(group_key)
        if group_info:
            priority_level = group_info.get('priority_level', 'medium')
            if priority_level == 'critical' and quality_score < 80:
                recommendations.append("Atenção: Grupo crítico com qualidade abaixo do esperado")
        
        return recommendations
    
    def _finalize_group_processing(self, df, product_group_column):
        """
        Final cleanup and group-specific optimizations
        """
        try:
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Clean up string columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace(['nan', 'None', 'null', 'NULL', '<NA>'], '')
                    
                    # Remove control characters
                    df[col] = df[col].str.replace(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', regex=True)
                    
                    # Limit text length for Excel compatibility
                    max_length = 32000
                    long_mask = df[col].str.len() > max_length
                    if long_mask.any():
                        df.loc[long_mask, col] = (
                            df.loc[long_mask, col].str[:max_length] + "...[TRUNCADO]"
                        )
            
            # Add group-specific summary columns
            self._add_group_summary_columns(df, product_group_column)
            
            print(f"✅ Finalização concluída. Forma final: {df.shape}")
            return df
            
        except Exception as e:
            print(f"❌ Erro na finalização: {str(e)}")
            self.stats['warnings'].append(f"Problemas na finalização: {str(e)}")
            return df
    
    def _add_group_summary_columns(self, df, product_group_column):
        """
        Add summary columns with group-aware metrics
        """
        try:
            # Add extraction success summary by group
            extracted_cols = [col for col in df.columns if col.startswith('extracted_')]
            
            if extracted_cols:
                # Count successful extractions per row
                df['total_extractions'] = df[extracted_cols].notna().sum(axis=1)
                df['extraction_success_rate'] = (df['total_extractions'] / len(extracted_cols) * 100).round(2)
                
                # Add mandatory field completeness per row
                df['mandatory_completeness'] = 0.0
                
                for group_key in df[product_group_column].unique():
                    if pd.isna(group_key):
                        continue
                        
                    group_mask = df[product_group_column] == group_key
                    mandatory_fields = self.group_manager.get_mandatory_fields(group_key)
                    
                    if mandatory_fields:
                        mandatory_cols = [f'extracted_{field}' for field in mandatory_fields 
                                        if f'extracted_{field}' in df.columns]
                        
                        if mandatory_cols:
                            mandatory_filled = df.loc[group_mask, mandatory_cols].notna().sum(axis=1)
                            mandatory_rate = (mandatory_filled / len(mandatory_cols) * 100).round(2)
                            df.loc[group_mask, 'mandatory_completeness'] = mandatory_rate
            
            # Add group validation flags
            df['valid_product_group'] = df[product_group_column].apply(
                lambda x: self.group_manager.is_valid_group(x) if pd.notna(x) else False
            )
            
            # Add quality indicators
            df['processing_quality'] = 'unknown'
            
            for group_key in df[product_group_column].unique():
                if pd.isna(group_key):
                    continue
                    
                group_mask = df[product_group_column] == group_key
                
                if group_key in self.stats['quality_metrics']:
                    quality_grade = self.stats['quality_metrics'][group_key]['quality_grade']
                    df.loc[group_mask, 'processing_quality'] = quality_grade
                
        except Exception as e:
            self.stats['warnings'].append(f"Erro ao adicionar colunas de resumo: {str(e)}")
    
    def _generate_group_processing_summary(self):
        """
        Generate comprehensive processing summary focused on groups
        """
        summary = {
            'overview': {
                'total_rows_processed': self.stats['processed_rows'],
                'chunks_processed': self.stats['chunks_processed'],
                'processing_time': self.stats['processing_time'],
                'product_groups_found': len(self.stats['product_groups_found']),
                'success_rate': f"{(self.stats['processed_rows'] / max(self.stats['total_rows'], 1)) * 100:.1f}%"
            },
            'product_groups': self.stats.get('product_groups_found', {}),
            'group_processing': self.stats.get('group_processing_summary', {}),
            'mandatory_field_coverage': self.stats.get('mandatory_field_coverage', {}),
            'quality_metrics': self.stats.get('quality_metrics', {}),
            'issues': {
                'errors': len(self.stats['errors']),
                'warnings': len(self.stats['warnings']),
                'error_details': self.stats['errors'],
                'warning_details': self.stats['warnings']
            },
            'recommendations': self._generate_overall_recommendations()
        }
        
        return summary
    
    def _generate_overall_recommendations(self):
        """
        Generate overall recommendations based on processing results
        """
        recommendations = []
        
        # Check for groups with low quality
        if self.stats.get('quality_metrics'):
            low_quality_groups = [
                group_data['group_display_name'] 
                for group_data in self.stats['quality_metrics'].values()
                if group_data['quality_score'] < 70
            ]
            
            if low_quality_groups:
                recommendations.append({
                    'type': 'quality',
                    'priority': 'high',
                    'message': f"Grupos com baixa qualidade detectados: {', '.join(low_quality_groups)}. Considere revisar padrões de extração."
                })
        
        # Check for mandatory field coverage
        if self.stats.get('mandatory_field_coverage'):
            low_coverage_groups = [
                analysis['name']
                for analysis in self.stats['mandatory_field_coverage'].values()
                if 'overall_completeness' in analysis and analysis['overall_completeness'] < 60
            ]
            
            if low_coverage_groups:
                recommendations.append({
                    'type': 'coverage',
                    'priority': 'high',
                    'message': f"Baixa completude de campos obrigatórios em: {', '.join(low_coverage_groups)}. Revisar padrões de extração."
                })
        
        # Check processing efficiency
        if self.stats['chunks_processed'] > 5:
            recommendations.append({
                'type': 'performance',
                'priority': 'medium',
                'message': f"Dataset processado em {self.stats['chunks_processed']} lotes. Considere aumentar o tamanho do chunk para melhor performance."
            })
        
        # Check for warnings
        if len(self.stats['warnings']) > 3:
            recommendations.append({
                'type': 'data_quality',
                'priority': 'medium',
                'message': f"Múltiplos avisos detectados ({len(self.stats['warnings'])}). Revisar qualidade dos dados de entrada."
            })
        
        return recommendations
    
    def get_group_processing_report(self):
        """
        Get detailed report focused on group-based processing
        """
        return {
            'processing_summary': self._generate_group_processing_summary(),
            'group_analysis': self.stats.get('mandatory_field_coverage', {}),
            'quality_metrics': self.stats.get('quality_metrics', {}),
            'cleaning_summary': self.cleaner.get_group_cleaning_summary(None) if hasattr(self.cleaner, 'get_group_cleaning_summary') else {},
            'extraction_summary': self.extractor.get_extraction_stats_by_group(None) if hasattr(self.extractor, 'get_extraction_stats_by_group') else {},
            'generated_at': datetime.now().isoformat()
        }
    
    def preview_group_processing(self, file_path, obs_column='obs', product_group_column='product_group', sample_size=5):
        """
        Preview group-based processing on a small sample
        """
        try:
            # Read small sample
            df_sample = pd.read_csv(file_path, nrows=sample_size*10)  # Read extra to ensure we have enough per group
            
            if obs_column not in df_sample.columns:
                return {'error': f"Coluna '{obs_column}' não encontrada"}
            
            if product_group_column not in df_sample.columns:
                return {'error': f"Coluna '{product_group_column}' não encontrada"}
            
            # Group the sample and take representative samples
            preview_results = []
            
            for group_key in df_sample[product_group_column].dropna().unique()[:3]:  # Max 3 groups
                group_data = df_sample[df_sample[product_group_column] == group_key].head(sample_size)
                
                group_info = self.group_manager.get_group_info(group_key)
                group_name = group_info['name'] if group_info else group_key
                mandatory_fields = self.group_manager.get_mandatory_fields(group_key)
                
                for idx, row in group_data.iterrows():
                    text = row[obs_column]
                    
                    if pd.isna(text) or not isinstance(text, str):
                        continue
                    
                    # Preview cleaning
                    cleaned_text = self.cleaner.clean_text_by_group(text, group_key)
                    cleaning_stats = self.cleaner.get_cleaning_stats(text, cleaned_text)
                    
                    # Preview mandatory field extraction
                    mandatory_extracted = self.extractor.extract_mandatory_fields_only(cleaned_text, group_key)
                    mandatory_success = sum(1 for v in mandatory_extracted.values() if v is not None)
                    
                    preview_results.append({
                        'group_key': group_key,
                        'group_name': group_name,
                        'group_category': self.group_manager.get_group_category(group_key),
                        'group_priority': self.group_manager.get_group_priority_level(group_key),
                        'original_length': len(text),
                        'cleaned_length': len(cleaned_text),
                        'cleaning_reduction': cleaning_stats.get('reduction_percent', 0),
                        'mandatory_fields_total': len(mandatory_fields),
                        'mandatory_fields_extracted': mandatory_success,
                        'mandatory_completeness': round((mandatory_success / len(mandatory_fields) * 100), 2) if mandatory_fields else 0,
                        'sample_extractions': {k: v for k, v in mandatory_extracted.items() if v is not None}
                    })
            
            return {
                'success': True,
                'preview_results': preview_results,
                'total_groups_found': len(df_sample[product_group_column].dropna().unique()),
                'processing_approach': 'group_optimized',
                'estimated_quality': 'high' if all(r['mandatory_completeness'] > 60 for r in preview_results) else 'medium'
            }
            
        except Exception as e:
            return {'error': f"Preview falhou: {str(e)}"}


# Backward compatibility
EnhancedTelecomDataProcessor = GroupBasedDataProcessor