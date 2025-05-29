import pandas as pd
import os
import traceback
from datetime import datetime
from .text_cleaner import TelecomTextCleaner
from .text_extractor import TelecomTextExtractor

class EnhancedTelecomDataProcessor:
    """Enhanced data processor specifically for telecom data"""
    
    def __init__(self, chunk_size=5000):
        self.chunk_size = chunk_size
        self.cleaner = TelecomTextCleaner()
        self.extractor = TelecomTextExtractor()
    
    def process_csv(self, input_file, obs_column='obs', progress_callback=None):
        """Process CSV with enhanced telecom extraction"""
        result = {
            'success': False,
            'dataframe': None,
            'stats': {},
            'errors': []
        }
        
        try:
            if progress_callback:
                progress_callback("Initializing telecom data processor...", 5)
            
            all_chunks = []
            chunk_num = 0
            total_rows = 0
            successful_extractions = 0
            
            for chunk_df in pd.read_csv(input_file, chunksize=self.chunk_size):
                chunk_num += 1
                total_rows += len(chunk_df)
                
                if obs_column not in chunk_df.columns:
                    result['errors'].append(f"Column '{obs_column}' not found")
                    return result
                
                if progress_callback:
                    progress_callback(f"Processing telecom data chunk {chunk_num}...", 20 + (chunk_num * 30))
                
                # Enhanced cleaning
                chunk_df[f'{obs_column}_cleaned'] = chunk_df[obs_column].apply(
                    lambda x: self.cleaner.clean_text(x) if pd.notna(x) else x
                )
                
                # Enhanced extraction
                extracted_data = []
                for _, row in chunk_df.iterrows():
                    obs_text = row.get(f'{obs_column}_cleaned', '')
                    if pd.notna(obs_text) and str(obs_text).strip():
                        extracted = self.extractor.process_text(str(obs_text))
                        # Count successful extractions
                        if any(v is not None for v in extracted.values()):
                            successful_extractions += 1
                    else:
                        extracted = {field: None for field in self.extractor.get_field_list()}
                    
                    extracted_data.append(extracted)
                
                # Combine data
                extracted_df = pd.DataFrame(extracted_data)
                combined_chunk = pd.concat([chunk_df.reset_index(drop=True), extracted_df], axis=1)
                all_chunks.append(combined_chunk)
            
            if progress_callback:
                progress_callback("Finalizing telecom data processing...", 80)
            
            # Combine all chunks
            final_df = pd.concat(all_chunks, ignore_index=True)
            
            # Calculate enhanced stats
            field_stats = {}
            for field in self.extractor.get_field_list():
                if field in final_df.columns:
                    non_null_count = final_df[field].notna().sum()
                    field_stats[field] = {
                        'count': int(non_null_count),
                        'percentage': round((non_null_count / total_rows) * 100, 1),
                        'sample_values': final_df[field].dropna().unique()[:3].tolist()
                    }
            
            result.update({
                'success': True,
                'dataframe': final_df,
                'stats': {
                    'total_rows': total_rows,
                    'successful_extractions': successful_extractions,
                    'extraction_rate': round((successful_extractions / total_rows) * 100, 1),
                    'field_stats': field_stats,
                    'chunks_processed': chunk_num
                }
            })
            
            if progress_callback:
                progress_callback(f"Telecom processing completed! {successful_extractions}/{total_rows} successful extractions", 100)
                
        except Exception as e:
            result['errors'].append(str(e))
        
        return result