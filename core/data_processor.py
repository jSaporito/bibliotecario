import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Import the enhanced modules
try:
    from .text_cleaner import EnhancedTelecomTextCleaner
    from .text_extractor import EnhancedTelecomTextExtractor
    from .product_groups import product_group_manager
except ImportError:
    # Fallback for standalone usage
    from text_cleaner import EnhancedTelecomTextCleaner
    from text_extractor import EnhancedTelecomTextExtractor
    from product_groups import ProductGroupManager

class EnhancedTelecomDataProcessor:
    """
    Enhanced telecom data processor with product group classification support
    Processes data based on product groups and their mandatory fields
    """
    
    def __init__(self, chunk_size=5000):
        self.chunk_size = chunk_size
        self.cleaner = EnhancedTelecomTextCleaner()
        self.extractor = EnhancedTelecomTextExtractor()
        self.product_group_manager = product_group_manager
        
        self.stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'product_groups_found': {},
            'mandatory_fields_extracted': {},
            'chunks_processed': 0,
            'cleaning_stats': {},
            'extraction_stats': {},
            'group_completeness': {},
            'errors': [],
            'warnings': [],
            'processing_time': None
        }
    
    def safe_convert_to_int(self, series, column_name="unknown"):
        """
        Safely convert a pandas series to integers, handling problematic values
        """
        try:
            original_length = len(series)
            
            # Handle missing values first
            series = series.fillna(0)
            
            # Convert to string to handle mixed types
            series = series.astype(str)
            
            # Handle common problematic values
            series = series.replace({
                'nan': '0',
                'NaN': '0', 
                'None': '0',
                'null': '0',
                'NULL': '0',
                '': '0',
                ' ': '0',
                'N/A': '0',
                'n/a': '0',
                '-': '0',
                'inf': '0',
                'infinity': '0'
            })
            
            # Remove non-numeric characters except digits, decimal points, and minus signs
            series = series.str.replace(r'[^\d.-]', '', regex=True)
            
            # Handle empty strings after cleaning
            series = series.replace('', '0')
            series = series.replace('-', '0')
            series = series.replace('.', '0')
            series = series.replace('.-', '0')
            
            # Convert to numeric first (handles edge cases)
            series = pd.to_numeric(series, errors='coerce')
            
            # Fill any remaining NaNs with 0
            series = series.fillna(0)
            
            # Convert to integer
            series = series.astype(int)
            
            print(f"✅ Successfully converted column '{column_name}' to integers ({original_length} values)")
            return series
            
        except Exception as e:
            print(f"❌ Error converting column '{column_name}' to int: {str(e)}")
            self.stats['warnings'].append(f"Could not convert {column_name} to int, using zeros")
            return pd.Series([0] * len(series), index=series.index, dtype=int)
    
    def _fix_data_types(self, df):
        """Fix problematic data types"""
        try:
            df_fixed = df.copy()
            
            # Convert object columns with mixed types to string
            for col in df_fixed.columns:
                if df_fixed[col].dtype == 'object':
                    try:
                        # Check if column contains mixed types
                        sample = df_fixed[col].dropna().head(100)
                        if len(sample) > 0:
                            types = set(type(x).__name__ for x in sample)
                            if len(types) > 1:
                                df_fixed[col] = df_fixed[col].astype(str)
                                df_fixed[col] = df_fixed[col].replace('nan', '')
                    except Exception:
                        continue
            
            print("✅ Data types fixed successfully")
            return df_fixed
            
        except Exception as e:
            print(f"⚠️ Warning in data type fixing: {str(e)}")
            return df
    
    def process_csv(self, file_path, obs_column='obs', product_group_column='product_group',
                   enable_cleaning=True, enable_extraction=True, progress_callback=None):
        """
        Process CSV file with product group classification and mandatory field focus
        """
        start_time = datetime.now()
        
        def update_progress(message, progress=None):
            if progress_callback:
                progress_callback(message, progress)
            else:
                print(f"Progress: {message}")
        
        try:
            update_progress("Starting enhanced CSV processing with product groups...", 5)
            
            # Validate file
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Read CSV with encoding detection
            update_progress("Reading CSV file...", 10)
            df = self._read_csv_safely(file_path)
            
            self.stats['total_rows'] = len(df)
            print(f"✅ Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")
            
            # Validate required columns
            if obs_column not in df.columns:
                available_cols = ', '.join(df.columns[:5])
                raise ValueError(f"Column '{obs_column}' not found. Available: {available_cols}...")
            
            # Check for product group column
            has_product_groups = product_group_column in df.columns
            if not has_product_groups:
                self.stats['warnings'].append(f"Product group column '{product_group_column}' not found. Using generic processing.")
                print(f"⚠️ Product group column '{product_group_column}' not found. Processing will use generic approach.")
            else:
                # Analyze product groups in the data
                self._analyze_product_groups(df, product_group_column)
            
            update_progress("Fixing data types...", 15)
            df = self._fix_data_types(df)
            
            # Process in chunks if large dataset
            if len(df) > self.chunk_size:
                update_progress("Processing in chunks with product group optimization...", 20)
                df = self._process_in_chunks(df, obs_column, product_group_column, 
                                           enable_cleaning, enable_extraction, progress_callback)
            else:
                update_progress("Processing data with product group optimization...", 30)
                df = self._process_chunk(df, obs_column, product_group_column, 
                                       enable_cleaning, enable_extraction)
                self.stats['chunks_processed'] = 1
            
            update_progress("Analyzing product group completeness...", 85)
            
            # Analyze completeness by product group
            if has_product_groups:
                self._analyze_group_completeness(df, product_group_column)
            
            # Final cleanup and statistics
            df = self._finalize_processing(df)
            
            # Calculate processing time
            end_time = datetime.now()
            processing_time = end_time - start_time
            self.stats['processing_time'] = str(processing_time).split('.')[0]
            
            self.stats['processed_rows'] = len(df)
            
            update_progress("Processing completed successfully!", 100)
            
            return {
                'success': True,
                'dataframe': df,
                'stats': self.stats,
                'message': 'Enhanced processing with product groups completed successfully'
            }
            
        except Exception as e:
            error_msg = f"Enhanced CSV processing failed: {str(e)}"
            print(f"❌ {error_msg}")
            self.stats['errors'].append(error_msg)
            
            return {
                'success': False,
                'dataframe': None,
                'stats': self.stats,
                'errors': self.stats['errors'],
                'message': error_msg
            }
    
    def _read_csv_safely(self, file_path):
        """Safely read CSV with encoding detection"""
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                print(f"✅ Successfully read CSV with {encoding} encoding")
                return df
            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                if encoding == encodings[-1]:  # Last encoding
                    raise e
                continue
        
        # If all encodings fail, try with python engine
        try:
            df = pd.read_csv(file_path, encoding='utf-8', engine='python', low_memory=False)
            print("✅ Successfully read CSV with python engine")
            return df
        except Exception as e:
            raise Exception(f"Could not read CSV file with any encoding: {str(e)}")
    
    def _analyze_product_groups(self, df, product_group_column):
        """Analyze product groups present in the data"""
        try:
            product_groups = df[product_group_column].value_counts()
            print(f"📊 Product groups found:")
            
            for group, count in product_groups.items():
                if pd.notna(group):
                    group_info = self.product_group_manager.get_group_info(group)
                    display_name = group_info['name'] if group_info else group
                    print(f"   {display_name}: {count:,} records")
                    
                    self.stats['product_groups_found'][group] = {
                        'display_name': display_name,
                        'record_count': int(count),
                        'percentage': round((count / len(df)) * 100, 2)
                    }
            
        except Exception as e:
            self.stats['warnings'].append(f"Error analyzing product groups: {str(e)}")
    
    def _process_in_chunks(self, df, obs_column, product_group_column, enable_cleaning, 
                          enable_extraction, progress_callback):
        """Process large dataset in chunks with product group optimization"""
        chunks = []
        total_chunks = (len(df) + self.chunk_size - 1) // self.chunk_size
        
        def update_chunk_progress(message, chunk_progress):
            # Convert chunk progress to overall progress (20-85%)
            overall_progress = 20 + (chunk_progress / 100) * 65
            if progress_callback:
                progress_callback(message, overall_progress)
        
        for i in range(0, len(df), self.chunk_size):
            chunk_num = i // self.chunk_size + 1
            chunk = df.iloc[i:i + self.chunk_size].copy()
            
            update_chunk_progress(f"Processing chunk {chunk_num}/{total_chunks} with product groups...", 
                                (chunk_num / total_chunks) * 100)
            
            processed_chunk = self._process_chunk(chunk, obs_column, product_group_column,
                                                enable_cleaning, enable_extraction)
            chunks.append(processed_chunk)
            
            self.stats['chunks_processed'] += 1
        
        # Combine all chunks
        if progress_callback:
            progress_callback("Combining processed chunks...", 80)
        
        combined_df = pd.concat(chunks, ignore_index=True)
        return combined_df
    
    def _process_chunk(self, chunk, obs_column, product_group_column, enable_cleaning, enable_extraction):
        """Process a single chunk of data with product group awareness"""
        try:
            processed_chunk = chunk.copy()
            
            # Process obs column if it exists
            if obs_column in processed_chunk.columns:
                
                # Step 1: Clean text if enabled (product group aware)
                if enable_cleaning:
                    print(f"🧹 Cleaning text with product group optimization...")
                    
                    if product_group_column in processed_chunk.columns:
                        # Group-aware cleaning
                        processed_chunk = self.cleaner.clean_dataframe_by_groups(
                            processed_chunk, obs_column, product_group_column
                        )
                    else:
                        # Generic cleaning
                        processed_chunk[obs_column + '_cleaned'] = processed_chunk[obs_column].apply(
                            lambda x: self.cleaner.clean_text(x, product_group=None)
                        )
                    
                    # Collect cleaning statistics
                    self._update_cleaning_stats(processed_chunk, obs_column)
                
                # Step 2: Extract structured fields if enabled (product group focused)
                if enable_extraction:
                    print(f"🔍 Extracting fields with product group focus...")
                    
                    if product_group_column in processed_chunk.columns:
                        # Group-aware extraction
                        processed_chunk = self.extractor.extract_dataframe_by_groups(
                            processed_chunk, 
                            obs_column + '_cleaned' if enable_cleaning else obs_column,
                            product_group_column
                        )
                    else:
                        # Generic extraction
                        text_column = obs_column + '_cleaned' if enable_cleaning else obs_column
                        
                        extraction_results = []
                        field_names = self.extractor.get_field_list()
                        
                        for idx, text in processed_chunk[text_column].items():
                            if pd.isna(text) or not isinstance(text, str):
                                extraction_results.append({field: None for field in field_names})
                                continue
                            
                            extracted = self.extractor.extract_all_fields(text)
                            extraction_results.append(extracted)
                        
                        # Add extracted fields as new columns
                        for field in field_names:
                            processed_chunk[f'extracted_{field}'] = [result[field] for result in extraction_results]
                    
                    # Collect extraction statistics
                    self._update_extraction_stats(processed_chunk, product_group_column)
            
            return processed_chunk
            
        except Exception as e:
            print(f"❌ Error processing chunk: {str(e)}")
            self.stats['errors'].append(f"Chunk processing error: {str(e)}")
            return chunk
    
    def _update_cleaning_stats(self, chunk, obs_column):
        """Update cleaning statistics from processed chunk"""
        try:
            if obs_column + '_cleaned' not in chunk.columns:
                return
            
            total_chars_removed = 0
            total_lines_removed = 0
            
            for idx, row in chunk.iterrows():
                original = str(row[obs_column]) if pd.notna(row[obs_column]) else ""
                cleaned = str(row[obs_column + '_cleaned']) if pd.notna(row[obs_column + '_cleaned']) else ""
                
                if original and cleaned:
                    stats = self.cleaner.get_cleaning_stats(original, cleaned)
                    total_chars_removed += stats.get('original_length', 0) - stats.get('cleaned_length', 0)
                    total_lines_removed += stats.get('lines_removed', 0)
            
            if 'total_chars_removed' not in self.stats['cleaning_stats']:
                self.stats['cleaning_stats']['total_chars_removed'] = 0
                self.stats['cleaning_stats']['total_lines_removed'] = 0
            
            self.stats['cleaning_stats']['total_chars_removed'] += total_chars_removed
            self.stats['cleaning_stats']['total_lines_removed'] += total_lines_removed
            
        except Exception as e:
            self.stats['warnings'].append(f"Error updating cleaning stats: {str(e)}")
    
    def _update_extraction_stats(self, chunk, product_group_column):
        """Update extraction statistics from processed chunk"""
        try:
            extracted_cols = [col for col in chunk.columns if col.startswith('extracted_')]
            
            if not extracted_cols:
                return
            
            total_extractions = 0
            successful_records = 0
            
            for idx, row in chunk.iterrows():
                record_extractions = 0
                for col in extracted_cols:
                    if pd.notna(row[col]) and str(row[col]).strip():
                        record_extractions += 1
                        total_extractions += 1
                
                if record_extractions > 0:
                    successful_records += 1
            
            if 'total_extractions' not in self.stats['extraction_stats']:
                self.stats['extraction_stats']['total_extractions'] = 0
                self.stats['extraction_stats']['successful_records'] = 0
                self.stats['extraction_stats']['fields_available'] = len(extracted_cols)
            
            self.stats['extraction_stats']['total_extractions'] += total_extractions
            self.stats['extraction_stats']['successful_records'] += successful_records
            
        except Exception as e:
            self.stats['warnings'].append(f"Error updating extraction stats: {str(e)}")
    
    def _analyze_group_completeness(self, df, product_group_column):
        """Analyze mandatory field completeness by product group"""
        try:
            print("📊 Analyzing mandatory field completeness by product group...")
            
            completeness_analysis = self.product_group_manager.analyze_group_completeness(
                df, product_group_column
            )
            
            self.stats['group_completeness'] = completeness_analysis
            
            # Print summary
            for group_key, analysis in completeness_analysis.items():
                print(f"   {analysis['name']}:")
                print(f"      Records: {analysis['total_records']:,}")
                print(f"      Overall completeness: {analysis['overall_completeness']:.1f}%")
                
                # Show top incomplete mandatory fields
                incomplete_fields = [
                    (field, stats) for field, stats in analysis['completeness_stats'].items() 
                    if stats['completeness_rate'] < 80
                ]
                
                if incomplete_fields:
                    print(f"      Fields needing attention:")
                    for field, stats in incomplete_fields[:3]:  # Top 3 incomplete
                        print(f"         {field}: {stats['completeness_rate']:.1f}% complete")
            
        except Exception as e:
            self.stats['warnings'].append(f"Error analyzing group completeness: {str(e)}")
    
    def _finalize_processing(self, df):
        """Final cleanup and optimization with product group considerations"""
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
                            df.loc[long_mask, col].str[:max_length] + "...[TRUNCATED]"
                        )
            
            # Add product group summary columns
            self._add_summary_columns(df)
            
            print(f"✅ Final cleanup completed. Shape: {df.shape}")
            return df
            
        except Exception as e:
            print(f"❌ Error in final cleanup: {str(e)}")
            self.stats['warnings'].append(f"Final cleanup had issues: {str(e)}")
            return df
    
    def _add_summary_columns(self, df):
        """Add summary columns for analysis"""
        try:
            # Add extraction success summary
            extracted_cols = [col for col in df.columns if col.startswith('extracted_')]
            
            if extracted_cols:
                # Count successful extractions per row
                df['extraction_count'] = df[extracted_cols].notna().sum(axis=1)
                df['extraction_success_rate'] = (df['extraction_count'] / len(extracted_cols) * 100).round(2)
                
                # Add completeness flag
                df['extraction_complete'] = df['extraction_success_rate'] >= 50  # 50% threshold
            
            # Add product group validation flags
            if 'product_group' in df.columns:
                df['valid_product_group'] = df['product_group'].apply(
                    lambda x: x in self.product_group_manager.get_all_groups() if pd.notna(x) else False
                )
            
        except Exception as e:
            self.stats['warnings'].append(f"Error adding summary columns: {str(e)}")
    
    def get_processing_summary(self):
        """Get a comprehensive summary of processing results"""
        summary = {
            'overview': {
                'total_rows_processed': self.stats['processed_rows'],
                'chunks_processed': self.stats['chunks_processed'],
                'processing_time': self.stats['processing_time'],
                'success_rate': f"{(self.stats['processed_rows'] / max(self.stats['total_rows'], 1)) * 100:.1f}%"
            },
            'product_groups': self.stats.get('product_groups_found', {}),
            'cleaning': self.stats.get('cleaning_stats', {}),
            'extraction': self.stats.get('extraction_stats', {}),
            'group_completeness': self.stats.get('group_completeness', {}),
            'issues': {
                'errors': len(self.stats['errors']),
                'warnings': len(self.stats['warnings']),
                'error_details': self.stats['errors'],
                'warning_details': self.stats['warnings']
            }
        }
        
        return summary
    
    def get_mandatory_field_report(self):
        """Get detailed report on mandatory field extraction by product group"""
        if not self.stats.get('group_completeness'):
            return {}
        
        report = {}
        
        for group_key, analysis in self.stats['group_completeness'].items():
            group_report = {
                'group_name': analysis['name'],
                'total_records': analysis['total_records'],
                'mandatory_fields': analysis['mandatory_fields'],
                'overall_completeness': analysis['overall_completeness'],
                'field_details': {},
                'recommendations': []
            }
            
            # Analyze each mandatory field
            for field, stats in analysis['completeness_stats'].items():
                group_report['field_details'][field] = {
                    'completeness_rate': stats['completeness_rate'],
                    'filled_records': stats['filled_records'],
                    'status': 'excellent' if stats['completeness_rate'] >= 90 else
                             'good' if stats['completeness_rate'] >= 70 else
                             'needs_attention' if stats['completeness_rate'] >= 50 else
                             'critical'
                }
                
                # Generate recommendations
                if stats['completeness_rate'] < 50:
                    group_report['recommendations'].append(
                        f"Critical: {field} has only {stats['completeness_rate']:.1f}% completeness"
                    )
                elif stats['completeness_rate'] < 70:
                    group_report['recommendations'].append(
                        f"Improve: {field} could be enhanced ({stats['completeness_rate']:.1f}% current)"
                    )
            
            report[group_key] = group_report
        
        return report
    
    def preview_processing(self, file_path, obs_column='obs', product_group_column='product_group', sample_size=3):
        """Preview what processing will do on a small sample with product group info"""
        try:
            # Read just a few rows
            df_sample = pd.read_csv(file_path, nrows=sample_size)
            
            if obs_column not in df_sample.columns:
                return {'error': f"Column '{obs_column}' not found"}
            
            has_product_groups = product_group_column in df_sample.columns
            
            preview_results = []
            
            for idx, row in df_sample.iterrows():
                text = row[obs_column]
                product_group = row[product_group_column] if has_product_groups and pd.notna(row[product_group_column]) else None
                
                if pd.isna(text) or not isinstance(text, str):
                    continue
                
                # Clean text
                cleaned = self.cleaner.clean_text(text, product_group=product_group)
                cleaning_stats = self.cleaner.get_cleaning_stats(text, cleaned)
                
                # Extract fields (mandatory focus if product group available)
                if product_group:
                    mandatory_extracted = self.extractor.extract_mandatory_fields_only(cleaned, product_group)
                    all_extracted = self.extractor.extract_all_fields(cleaned, product_group)
                    
                    mandatory_fields = self.product_group_manager.get_mandatory_fields(product_group)
                    mandatory_success_count = sum(1 for v in mandatory_extracted.values() if v is not None)
                else:
                    all_extracted = self.extractor.extract_all_fields(cleaned)
                    mandatory_extracted = {}
                    mandatory_fields = []
                    mandatory_success_count = 0
                
                preview_results.append({
                    'row_index': idx,
                    'product_group': product_group,
                    'product_group_name': self.product_group_manager.get_group_display_name(product_group) if product_group else 'Unknown',
                    'original_length': len(text),
                    'cleaned_length': len(cleaned),
                    'cleaning_reduction': cleaning_stats.get('reduction_percent', 0),
                    'mandatory_fields_count': len(mandatory_fields),
                    'mandatory_fields_extracted': mandatory_success_count,
                    'mandatory_completeness': round((mandatory_success_count / len(mandatory_fields) * 100), 2) if mandatory_fields else 0,
                    'total_fields_extracted': sum(1 for v in all_extracted.values() if v is not None),
                    'sample_mandatory_extractions': {k: v for k, v in mandatory_extracted.items() if v is not None},
                    'sample_all_extractions': {k: v for k, v in all_extracted.items() if v is not None}
                })
            
            return {
                'success': True,
                'has_product_groups': has_product_groups,
                'sample_results': preview_results,
                'total_extractable_fields': len(self.extractor.get_field_list()),
                'product_groups_in_sample': list(set([r['product_group'] for r in preview_results if r['product_group']]))
            }
            
        except Exception as e:
            return {'error': f"Preview failed: {str(e)}"}
    
    # Backward compatibility methods
    def process_csv_simple(self, file_path, obs_column='obs', chunk_size=None, progress_callback=None):
        """Backward compatibility wrapper"""
        if chunk_size:
            self.chunk_size = chunk_size
        
        return self.process_csv(
            file_path=file_path,
            obs_column=obs_column,
            product_group_column='product_group',  # Default column name
            enable_cleaning=True,
            enable_extraction=True,
            progress_callback=progress_callback
        )

# For backward compatibility
TelecomDataProcessor = EnhancedTelecomDataProcessor