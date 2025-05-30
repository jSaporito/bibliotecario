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
except ImportError:
    # Fallback for standalone usage
    from text_cleaner import EnhancedTelecomTextCleaner
    from text_extractor import EnhancedTelecomTextExtractor

class EnhancedTelecomDataProcessor:
    """
    Enhanced telecom data processor with integrated text cleaning and field extraction
    This fixes the 'cannot convert the series to <class 'int'>' error
    """
    
    def __init__(self, chunk_size=5000):
        self.chunk_size = chunk_size
        self.cleaner = EnhancedTelecomTextCleaner()
        self.extractor = EnhancedTelecomTextExtractor()
        
        self.stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'extracted_fields': 0,
            'chunks_processed': 0,
            'cleaning_stats': {},
            'extraction_stats': {},
            'errors': [],
            'warnings': [],
            'processing_time': None
        }
    
    def safe_convert_to_int(self, series, column_name="unknown"):
        """
        Safely convert a pandas series to integers, handling problematic values
        This is the KEY function that fixes your error
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
            # Return series filled with zeros as ultimate fallback
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
    
    def process_csv(self, file_path, obs_column='obs', enable_cleaning=True, 
                   enable_extraction=True, progress_callback=None):
        """
        Process CSV file with enhanced text cleaning and field extraction
        """
        start_time = datetime.now()
        
        def update_progress(message, progress=None):
            if progress_callback:
                progress_callback(message, progress)
            else:
                print(f"Progress: {message}")
        
        try:
            update_progress("Starting enhanced CSV processing...", 5)
            
            # Validate file
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Read CSV with encoding detection
            update_progress("Reading CSV file...", 10)
            df = self._read_csv_safely(file_path)
            
            self.stats['total_rows'] = len(df)
            print(f"✅ Loaded CSV: {df.shape[0]} rows, {df.shape[1]} columns")
            
            # Validate obs column
            if obs_column not in df.columns:
                available_cols = ', '.join(df.columns[:5])
                raise ValueError(f"Column '{obs_column}' not found. Available: {available_cols}...")
            
            update_progress("Fixing data types...", 15)
            df = self._fix_data_types(df)
            
            # Process in chunks if large dataset
            if len(df) > self.chunk_size:
                update_progress("Processing in chunks...", 20)
                df = self._process_in_chunks(df, obs_column, enable_cleaning, 
                                           enable_extraction, progress_callback)
            else:
                update_progress("Processing data...", 30)
                df = self._process_chunk(df, obs_column, enable_cleaning, enable_extraction)
                self.stats['chunks_processed'] = 1
            
            update_progress("Finalizing processing...", 85)
            
            # Final cleanup and statistics
            df = self._finalize_processing(df)
            
            # Calculate processing time
            end_time = datetime.now()
            processing_time = end_time - start_time
            self.stats['processing_time'] = str(processing_time).split('.')[0]  # Remove microseconds
            
            self.stats['processed_rows'] = len(df)
            
            update_progress("Processing completed successfully!", 100)
            
            return {
                'success': True,
                'dataframe': df,
                'stats': self.stats,
                'message': 'Enhanced processing completed successfully'
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
    
    def _process_in_chunks(self, df, obs_column, enable_cleaning, enable_extraction, progress_callback):
        """Process large dataset in chunks"""
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
            
            update_chunk_progress(f"Processing chunk {chunk_num}/{total_chunks}...", 
                                (chunk_num / total_chunks) * 100)
            
            processed_chunk = self._process_chunk(chunk, obs_column, enable_cleaning, enable_extraction)
            chunks.append(processed_chunk)
            
            self.stats['chunks_processed'] += 1
        
        # Combine all chunks
        if progress_callback:
            progress_callback("Combining processed chunks...", 80)
        
        combined_df = pd.concat(chunks, ignore_index=True)
        return combined_df
    
    def _process_chunk(self, chunk, obs_column, enable_cleaning, enable_extraction):
        """Process a single chunk of data"""
        try:
            processed_chunk = chunk.copy()
            
            # Process obs column if it exists
            if obs_column in processed_chunk.columns:
                
                # Step 1: Clean text if enabled
                if enable_cleaning:
                    print(f"🧹 Cleaning text in column '{obs_column}'...")
                    
                    clean_results = []
                    cleaning_stats = {'total_chars_removed': 0, 'total_lines_removed': 0}
                    
                    for idx, text in processed_chunk[obs_column].items():
                        if pd.isna(text) or not isinstance(text, str):
                            clean_results.append(text)
                            continue
                        
                        # Clean the text
                        cleaned_text = self.cleaner.clean_text(text)
                        clean_results.append(cleaned_text)
                        
                        # Collect cleaning statistics
                        stats = self.cleaner.get_cleaning_stats(text, cleaned_text)
                        cleaning_stats['total_chars_removed'] += (stats.get('original_length', 0) - 
                                                                stats.get('cleaned_length', 0))
                        cleaning_stats['total_lines_removed'] += stats.get('lines_removed', 0)
                    
                    # Replace original column with cleaned version
                    processed_chunk[obs_column + '_cleaned'] = clean_results
                    self.stats['cleaning_stats'] = cleaning_stats
                
                # Step 2: Extract structured fields if enabled
                if enable_extraction:
                    print(f"🔍 Extracting fields from column '{obs_column}'...")
                    
                    # Use cleaned text if available, otherwise original
                    text_column = obs_column + '_cleaned' if enable_cleaning else obs_column
                    
                    extraction_results = []
                    extraction_stats = {'fields_extracted': 0, 'successful_extractions': 0}
                    
                    # Get all possible field names
                    field_names = self.extractor.get_field_list()
                    
                    for idx, text in processed_chunk[text_column].items():
                        if pd.isna(text) or not isinstance(text, str):
                            extraction_results.append({field: None for field in field_names})
                            continue
                        
                        # Extract fields
                        extracted = self.extractor.extract_all_fields(text)
                        extraction_results.append(extracted)
                        
                        # Count successful extractions
                        non_null_fields = sum(1 for v in extracted.values() if v is not None)
                        if non_null_fields > 0:
                            extraction_stats['successful_extractions'] += 1
                            extraction_stats['fields_extracted'] += non_null_fields
                    
                    # Add extracted fields as new columns
                    for field in field_names:
                        processed_chunk[f'extracted_{field}'] = [result[field] for result in extraction_results]
                    
                    self.stats['extraction_stats'] = extraction_stats
                    self.stats['extracted_fields'] = len(field_names)
            
            return processed_chunk
            
        except Exception as e:
            print(f"❌ Error processing chunk: {str(e)}")
            self.stats['errors'].append(f"Chunk processing error: {str(e)}")
            return chunk
    
    def _finalize_processing(self, df):
        """Final cleanup and optimization"""
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
            
            print(f"✅ Final cleanup completed. Shape: {df.shape}")
            return df
            
        except Exception as e:
            print(f"❌ Error in final cleanup: {str(e)}")
            self.stats['warnings'].append(f"Final cleanup had issues: {str(e)}")
            return df
    
    def get_processing_summary(self):
        """Get a comprehensive summary of processing results"""
        return {
            'overview': {
                'total_rows_processed': self.stats['processed_rows'],
                'chunks_processed': self.stats['chunks_processed'],
                'processing_time': self.stats['processing_time'],
                'success_rate': f"{(self.stats['processed_rows'] / max(self.stats['total_rows'], 1)) * 100:.1f}%"
            },
            'cleaning': self.stats.get('cleaning_stats', {}),
            'extraction': {
                'fields_available': self.stats['extracted_fields'],
                'extraction_stats': self.stats.get('extraction_stats', {})
            },
            'issues': {
                'errors': len(self.stats['errors']),
                'warnings': len(self.stats['warnings'])
            }
        }
    
    def preview_processing(self, file_path, obs_column='obs', sample_size=3):
        """Preview what processing will do on a small sample"""
        try:
            # Read just a few rows
            df_sample = pd.read_csv(file_path, nrows=sample_size)
            
            if obs_column not in df_sample.columns:
                return {'error': f"Column '{obs_column}' not found"}
            
            preview_results = []
            
            for idx, text in df_sample[obs_column].items():
                if pd.isna(text) or not isinstance(text, str):
                    continue
                
                # Clean text
                cleaned = self.cleaner.clean_text(text)
                cleaning_stats = self.cleaner.get_cleaning_stats(text, cleaned)
                
                # Extract fields
                extracted = self.extractor.extract_all_fields(cleaned)
                
                preview_results.append({
                    'row_index': idx,
                    'original_length': len(text),
                    'cleaned_length': len(cleaned),
                    'cleaning_reduction': cleaning_stats.get('reduction_percent', 0),
                    'fields_extracted': sum(1 for v in extracted.values() if v is not None),
                    'sample_extracted_fields': {k: v for k, v in extracted.items() if v is not None}
                })
            
            return {
                'success': True,
                'sample_results': preview_results,
                'total_extractable_fields': len(self.extractor.get_field_list())
            }
            
        except Exception as e:
            return {'error': f"Preview failed: {str(e)}"}
    
    # Backward compatibility method
    def process_csv_simple(self, file_path, obs_column='obs', chunk_size=None, progress_callback=None):
        """Backward compatibility wrapper"""
        if chunk_size:
            self.chunk_size = chunk_size
        
        return self.process_csv(
            file_path=file_path,
            obs_column=obs_column,
            enable_cleaning=True,
            enable_extraction=True,
            progress_callback=progress_callback
        )