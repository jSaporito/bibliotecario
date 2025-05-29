
import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class EnhancedTelecomDataProcessor:
    """
    Enhanced telecom data processor with fixed data type handling
    This fixes the 'cannot convert the series to <class 'int'>' error
    """
    
    def __init__(self, chunk_size=5000):
        self.chunk_size = chunk_size
        self.stats = {
            'total_rows': 0,
            'processed_rows': 0,
            'errors': [],
            'warnings': []
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
    
    def safe_convert_to_float(self, series, column_name="unknown"):
        """
        Safely convert a pandas series to floats, handling problematic values
        """
        try:
            original_length = len(series)
            
            # Handle missing values first
            series = series.fillna(0.0)
            
            # Convert to string to handle mixed types
            series = series.astype(str)
            
            # Handle common problematic values
            series = series.replace({
                'nan': '0.0',
                'NaN': '0.0',
                'None': '0.0', 
                'null': '0.0',
                'NULL': '0.0',
                '': '0.0',
                ' ': '0.0',
                'N/A': '0.0',
                'n/a': '0.0',
                'inf': '0.0',
                'infinity': '0.0'
            })
            
            # Remove non-numeric characters except digits, decimal points, and minus signs
            series = series.str.replace(r'[^\d.-]', '', regex=True)
            
            # Handle empty strings after cleaning
            series = series.replace('', '0.0')
            series = series.replace('-', '0.0')
            
            # Convert to numeric
            series = pd.to_numeric(series, errors='coerce')
            
            # Fill any remaining NaNs with 0
            series = series.fillna(0.0)
            
            print(f"✅ Successfully converted column '{column_name}' to floats ({original_length} values)")
            return series
            
        except Exception as e:
            print(f"❌ Error converting column '{column_name}' to float: {str(e)}")
            self.stats['warnings'].append(f"Could not convert {column_name} to float, using zeros")
            # Return series filled with zeros as ultimate fallback
            return pd.Series([0.0] * len(series), index=series.index, dtype=float)
    
    def fix_data_types(self, df):
        """
        Fix data types in the DataFrame to prevent conversion errors
        This is called before any processing that might cause type conversion issues
        """
        print("🔧 Fixing data types to prevent conversion errors...")
        
        try:
            df_fixed = df.copy()
            
            # Identify columns that should be numeric based on their names or content
            numeric_int_columns = []
            numeric_float_columns = []
            
            for col in df_fixed.columns:
                col_lower = col.lower()
                
                # Check column names for obvious numeric indicators
                if any(keyword in col_lower for keyword in [
                    'id', 'count', 'number', 'num', 'qty', 'quantity', 
                    'age', 'year', 'month', 'day', 'index', 'rank'
                ]):
                    numeric_int_columns.append(col)
                elif any(keyword in col_lower for keyword in [
                    'price', 'cost', 'amount', 'rate', 'percent', 'score',
                    'weight', 'height', 'distance', 'speed', 'temp'
                ]):
                    numeric_float_columns.append(col)
                elif df_fixed[col].dtype == 'object':
                    # Sample the column to see if it looks numeric
                    sample = df_fixed[col].dropna().head(100).astype(str)
                    if len(sample) > 10:  # Only check if we have enough samples
                        # Count values that look like integers
                        int_like = sample.str.match(r'^-?\d+$').sum()
                        # Count values that look like floats
                        float_like = sample.str.match(r'^-?\d*\.?\d+$').sum()
                        
                        total_samples = len(sample)
                        
                        if int_like > total_samples * 0.7:  # 70% look like integers
                            numeric_int_columns.append(col)
                        elif (int_like + float_like) > total_samples * 0.7:  # 70% look numeric
                            numeric_float_columns.append(col)
            
            print(f"Identified {len(numeric_int_columns)} integer columns: {numeric_int_columns}")
            print(f"Identified {len(numeric_float_columns)} float columns: {numeric_float_columns}")
            
            # Convert integer columns
            for col in numeric_int_columns:
                df_fixed[col] = self.safe_convert_to_int(df_fixed[col], col)
            
            # Convert float columns  
            for col in numeric_float_columns:
                df_fixed[col] = self.safe_convert_to_float(df_fixed[col], col)
            
            print("✅ Data type fixing completed successfully")
            return df_fixed
            
        except Exception as e:
            print(f"❌ Error in fix_data_types: {str(e)}")
            self.stats['errors'].append(f"Data type fixing failed: {str(e)}")
            return df  # Return original DataFrame if fixing fails
    
    def process_csv(self, file_path, obs_column='obs', progress_callback=None):
        """
        Process CSV file with enhanced error handling and data type fixing
        This is the main method that was causing your error
        """
        def update_progress(message, progress=None):
            if progress_callback:
                progress_callback(message, progress)
            else:
                print(f"Progress: {message}")
        
        try:
            update_progress("Starting CSV processing...", 10)
            
            # Check if file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Read the CSV file with error handling
            update_progress("Reading CSV file...", 20)
            
            try:
                # Try UTF-8 first
                df = pd.read_csv(file_path, low_memory=False)
                print(f"✅ Successfully read CSV with UTF-8 encoding")
            except UnicodeDecodeError:
                try:
                    # Try latin-1 if UTF-8 fails
                    df = pd.read_csv(file_path, low_memory=False, encoding='latin-1')
                    print(f"✅ Successfully read CSV with latin-1 encoding")
                except Exception as e:
                    # Try with python engine as last resort
                    df = pd.read_csv(file_path, low_memory=False, encoding='utf-8', engine='python')
                    print(f"✅ Successfully read CSV with python engine")
            
            self.stats['total_rows'] = len(df)
            print(f"Initial data shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            
            update_progress("Fixing data types...", 30)
            
            # 🚨 THIS IS THE KEY FIX - Fix data types BEFORE any processing
            df = self.fix_data_types(df)
            
            update_progress("Processing data in chunks...", 40)
            
            # Process the data (your existing business logic goes here)
            if len(df) > self.chunk_size:
                # Process in chunks
                chunks = []
                total_chunks = (len(df) + self.chunk_size - 1) // self.chunk_size
                
                for i in range(0, len(df), self.chunk_size):
                    chunk_num = i // self.chunk_size + 1
                    chunk = df.iloc[i:i + self.chunk_size].copy()
                    
                    update_progress(f"Processing chunk {chunk_num}/{total_chunks}...", 
                                  40 + (chunk_num / total_chunks) * 30)
                    
                    # Process this chunk (add your specific business logic here)
                    processed_chunk = self.process_chunk(chunk, obs_column)
                    chunks.append(processed_chunk)
                
                # Combine all chunks
                update_progress("Combining processed chunks...", 75)
                df = pd.concat(chunks, ignore_index=True)
            else:
                # Process as single chunk
                update_progress("Processing data...", 50)
                df = self.process_chunk(df, obs_column)
            
            self.stats['processed_rows'] = len(df)
            
            update_progress("Finalizing processing...", 80)
            
            # Final cleanup and validation
            df = self.final_cleanup(df)
            
            return {
                'success': True,
                'dataframe': df,
                'stats': self.stats,
                'message': 'Processing completed successfully'
            }
            
        except Exception as e:
            error_msg = f"CSV processing failed: {str(e)}"
            print(f"❌ {error_msg}")
            self.stats['errors'].append(error_msg)
            
            return {
                'success': False,
                'dataframe': None,
                'stats': self.stats,
                'errors': self.stats['errors'],
                'message': error_msg
            }
    
    def process_chunk(self, chunk, obs_column):
        """
        Process a single chunk of data
        Add your specific business logic here
        """
        try:
            # Example processing - replace with your actual logic
            processed_chunk = chunk.copy()
            
            # Clean the observation column if it exists
            if obs_column in processed_chunk.columns:
                # Convert to string and clean
                processed_chunk[obs_column] = processed_chunk[obs_column].astype(str)
                processed_chunk[obs_column] = processed_chunk[obs_column].replace('nan', '')
                
                # Remove very long strings that might cause Excel issues
                max_length = 32000
                long_mask = processed_chunk[obs_column].str.len() > max_length
                if long_mask.any():
                    processed_chunk.loc[long_mask, obs_column] = (
                        processed_chunk.loc[long_mask, obs_column].str[:max_length] + "...[TRUNCATED]"
                    )
            
            # Add any other specific processing logic here
            # For example:
            # - Data validation
            # - Calculations
            # - Transformations
            # - Filtering
            
            return processed_chunk
            
        except Exception as e:
            print(f"❌ Error processing chunk: {str(e)}")
            self.stats['errors'].append(f"Chunk processing error: {str(e)}")
            return chunk  # Return original chunk if processing fails
    
    def final_cleanup(self, df):
        """
        Final cleanup and validation of the processed DataFrame
        """
        try:
            # Remove any completely empty rows
            df = df.dropna(how='all')
            
            # Clean up any remaining problematic values
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Replace problematic strings
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace(['nan', 'None', 'null', 'NULL'], '')
                    
                    # Remove control characters
                    df[col] = df[col].str.replace(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', regex=True)
            
            print(f"✅ Final cleanup completed. Shape: {df.shape}")
            return df
            
        except Exception as e:
            print(f"❌ Error in final cleanup: {str(e)}")
            self.stats['warnings'].append(f"Final cleanup had issues: {str(e)}")
            return df