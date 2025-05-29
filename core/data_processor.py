import pandas as pd
import os
import numpy as np
from datetime import datetime
from collections import defaultdict
import logging
import re

# Import the enhanced classes
from .text_cleaner import TextCleaner
from .text_extractor import TextExtractor

class DataProcessor:
    """Enhanced main class for processing CSV files with advanced text extraction and cleaning"""
    
    def __init__(self, chunk_size=5000, enable_advanced_stats=True, log_level=logging.INFO):
        self.chunk_size = chunk_size
        self.enable_advanced_stats = enable_advanced_stats
        
        # Initialize enhanced components
        self.extractor = TextExtractor()
        self.cleaner = TextCleaner()
        
        # Processing statistics
        self.processing_stats = {}
        self.field_extraction_history = defaultdict(list)
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        
        # Quality metrics
        self.quality_metrics = {
            'extraction_confidence': {},
            'cleaning_effectiveness': {},
            'pattern_success_rates': defaultdict(int),
            'field_coverage': defaultdict(int)
        }
    
    def process_csv(self, input_file_path, obs_column='obs', progress_callback=None, 
                   enable_validation=True, output_cleaning_report=False):
     
        results = {
            'success': False,
            'dataframe': None,
            'stats': {},
            'errors': [],
            'warnings': [],
            'quality_report': {},
            'cleaning_report': {} if output_cleaning_report else None
        }
        
        try:
            # Validate input file
            if not os.path.exists(input_file_path):
                results['errors'].append(f"Input file not found: {input_file_path}")
                return results
            
            # Initialize comprehensive stats
            self.processing_stats = {
                'total_rows': 0,
                'chunks_processed': 0,
                'original_chars': 0,
                'cleaned_chars': 0,
                'extracted_fields': 0,
                'successful_extractions': 0,
                'failed_extractions': 0,
                'start_time': datetime.now(),
                'processing_phases': {
                    'reading': 0,
                    'cleaning': 0,
                    'extracting': 0,
                    'validating': 0
                }
            }
            
            # Reset quality metrics
            self.quality_metrics = {
                'extraction_confidence': {},
                'cleaning_effectiveness': {},
                'pattern_success_rates': defaultdict(int),
                'field_coverage': defaultdict(int),
                'data_quality_issues': []
            }
            
            if progress_callback:
                progress_callback("Starting CSV processing...", 0)
            
            # Determine file size for progress tracking
            file_size = os.path.getsize(input_file_path)
            self.logger.info(f"Processing file: {input_file_path} ({file_size} bytes)")
            
            # Process file in enhanced chunks
            all_chunks = []
            chunk_num = 0
            total_chunks = self._estimate_chunk_count(input_file_path)
            
            # Phase 1: Read and process chunks
            phase_start = datetime.now()
            
            for chunk_df in pd.read_csv(input_file_path, chunksize=self.chunk_size):
                chunk_num += 1
                
                if progress_callback:
                    progress = min(90, (chunk_num / total_chunks) * 80)  # 80% for processing
                    progress_callback(f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk_df)} rows)...", progress)
                
                # Enhanced chunk processing
                processed_chunk, chunk_quality = self._process_chunk_enhanced(
                    chunk_df, obs_column, chunk_num, enable_validation
                )
                
                if processed_chunk is not None:
                    all_chunks.append(processed_chunk)
                    
                    # Update quality metrics
                    self._update_quality_metrics(chunk_quality)
                    
                    # Update stats
                    self.processing_stats['chunks_processed'] += 1
                    self.processing_stats['total_rows'] += len(chunk_df)
                    
                    if progress_callback:
                        progress_callback(
                            f"Chunk {chunk_num} completed. "
                            f"Processed: {self.processing_stats['total_rows']} rows. "
                            f"Success rate: {self._calculate_success_rate():.1f}%"
                        )
                else:
                    results['warnings'].append(f"Chunk {chunk_num} failed processing")
            
            self.processing_stats['processing_phases']['reading'] = (datetime.now() - phase_start).total_seconds()
            
            if not all_chunks:
                results['errors'].append("No chunks were successfully processed")
                return results
            
            # Phase 2: Combine chunks
            if progress_callback:
                progress_callback("Combining processed chunks...", 85)
            
            phase_start = datetime.now()
            final_df = pd.concat(all_chunks, ignore_index=True)
            self.processing_stats['processing_phases']['combining'] = (datetime.now() - phase_start).total_seconds()
            
            # Phase 3: Post-processing validation and enhancement
            if enable_validation:
                if progress_callback:
                    progress_callback("Performing data validation...", 90)
                
                phase_start = datetime.now()
                validation_results = self._validate_extracted_data(final_df)
                results['quality_report'] = validation_results
                self.processing_stats['processing_phases']['validating'] = (datetime.now() - phase_start).total_seconds()
            
            # Phase 4: Generate comprehensive statistics
            if progress_callback:
                progress_callback("Generating statistics...", 95)
            
            # Calculate enhanced field statistics
            field_stats = self._calculate_enhanced_field_stats(final_df)
            
            # Generate cleaning report if requested
            if output_cleaning_report:
                results['cleaning_report'] = self._generate_cleaning_report()
            
            # Finalize processing stats
            self.processing_stats['end_time'] = datetime.now()
            self.processing_stats['processing_time'] = (
                self.processing_stats['end_time'] - self.processing_stats['start_time']
            ).total_seconds()
            
            # Calculate overall success rate
            success_rate = self._calculate_success_rate()
            
            results.update({
                'success': True,
                'dataframe': final_df,
                'stats': {
                    'processing': self.processing_stats,
                    'fields': field_stats,
                    'quality': self.quality_metrics,
                    'success_rate': success_rate
                }
            })
            
            if progress_callback:
                progress_callback(f"Processing completed! Success rate: {success_rate:.1f}%", 100)
            
            self.logger.info(f"Processing completed successfully. Processed {self.processing_stats['total_rows']} rows")
            
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            results['errors'].append(error_msg)
            results['success'] = False
            self.logger.error(error_msg)
        
        return results
    
    def _estimate_chunk_count(self, file_path):
        """Estimate number of chunks for progress tracking"""
        try:
            # Quick estimate based on file size and average row size
            with open(file_path, 'r', encoding='utf-8') as f:
                sample = f.read(10000)  # Read first 10KB
                sample_lines = len(sample.split('\n'))
            
            file_size = os.path.getsize(file_path)
            estimated_lines = (file_size / 10000) * sample_lines
            estimated_chunks = max(1, int(estimated_lines / self.chunk_size))
            
            return estimated_chunks
        except:
            return 10  # Fallback estimate
    
    def _process_chunk_enhanced(self, chunk_df, obs_column, chunk_num, enable_validation):
        """Enhanced chunk processing with quality tracking"""
        if obs_column not in chunk_df.columns:
            raise ValueError(f"Column '{obs_column}' not found in CSV file")
        
        chunk_quality = {
            'chunk_num': chunk_num,
            'total_rows': len(chunk_df),
            'successful_cleanings': 0,
            'successful_extractions': 0,
            'cleaning_stats': [],
            'extraction_stats': [],
            'validation_issues': []
        }
        
        try:
            # Step 1: Enhanced text cleaning with statistics
            chunk_df[f'{obs_column}_cleaned'] = chunk_df[obs_column].apply(
                lambda x: self._clean_with_stats(x, chunk_quality)
            )
            
            # Step 2: Excel sanitization
            chunk_df[obs_column] = chunk_df[obs_column].apply(
                lambda x: self.cleaner.sanitize_for_excel(str(x)) if pd.notna(x) else x
            )
            chunk_df[f'{obs_column}_cleaned'] = chunk_df[f'{obs_column}_cleaned'].apply(
                lambda x: self.cleaner.sanitize_for_excel(str(x)) if pd.notna(x) else x
            )
            
            # Step 3: Enhanced data extraction
            chunk_results = []
            
            for idx, row in chunk_df.iterrows():
                obs_cleaned = row.get(f'{obs_column}_cleaned', '')
                obs_original = row.get(obs_column, '')
                
                # Enhanced extraction with confidence tracking
                extraction_result = self._extract_with_confidence(obs_cleaned, obs_original, chunk_quality)
                chunk_results.append(extraction_result)
            
            # Create DataFrame with extracted data
            extracted_df = pd.DataFrame(chunk_results)
            
            # Combine with original data
            combined_chunk = pd.concat([chunk_df.reset_index(drop=True), extracted_df], axis=1)
            
            # Step 4: Chunk-level validation
            if enable_validation:
                validation_issues = self._validate_chunk_data(combined_chunk, chunk_quality)
                chunk_quality['validation_issues'] = validation_issues
            
            return combined_chunk, chunk_quality
            
        except Exception as e:
            self.logger.error(f"Chunk {chunk_num} processing failed: {str(e)}")
            return None, chunk_quality
    
    def _clean_with_stats(self, text, chunk_quality):
        """Clean text while tracking statistics"""
        if pd.isna(text) or not isinstance(text, str):
            return text
        
        original_text = str(text)
        cleaned_text = self.cleaner.clean_text(original_text)
        
        # Track cleaning effectiveness
        if cleaned_text != original_text:
            cleaning_stats = self.cleaner.get_cleaning_stats(original_text, cleaned_text)
            chunk_quality['cleaning_stats'].append(cleaning_stats)
            chunk_quality['successful_cleanings'] += 1
            
            # Update global stats
            self.processing_stats['original_chars'] += cleaning_stats.get('original_length', 0)
            self.processing_stats['cleaned_chars'] += cleaning_stats.get('cleaned_length', 0)
        
        return cleaned_text
    
    def _extract_with_confidence(self, obs_cleaned, obs_original, chunk_quality):
        """Enhanced extraction with confidence scoring and better error handling"""
        try:
            try:
                extracted_clean = self.extractor.process_text(obs_cleaned)
            except Exception as e:
                self.logger.warning(f"Extraction failed on cleaned text: {str(e)}")
                extracted_clean = {field: None for field in self.extractor.get_field_list() or []}
            
            try:
                extracted_original = self.extractor.process_text(obs_original)
            except Exception as e:
                self.logger.warning(f"Extraction failed on original text: {str(e)}")
                extracted_original = {field: None for field in self.extractor.get_field_list() or []}
            
            final_extracted = {}
            extraction_confidence = {}

            fields = self.extractor.get_field_list() or []
            for field in fields:
                clean_value = extracted_clean.get(field)
                original_value = extracted_original.get(field)
                
                try:
                    # Confidence-based selection
                    if clean_value and original_value:
                        if self._validate_field_value(clean_value, field):
                            final_extracted[field] = clean_value
                            extraction_confidence[field] = 0.8
                        elif self._validate_field_value(original_value, field):
                            final_extracted[field] = original_value
                            extraction_confidence[field] = 0.6
                        else:
                            final_extracted[field] = None
                            extraction_confidence[field] = 0.0
                    elif clean_value:
                        if self._validate_field_value(clean_value, field):
                            final_extracted[field] = clean_value
                            extraction_confidence[field] = 0.7
                        else:
                            final_extracted[field] = None
                            extraction_confidence[field] = 0.0
                    elif original_value:
                        if self._validate_field_value(original_value, field):
                            final_extracted[field] = original_value
                            extraction_confidence[field] = 0.5
                        else:
                            final_extracted[field] = None
                            extraction_confidence[field] = 0.0
                    else:
                        final_extracted[field] = None
                        extraction_confidence[field] = 0.0
                except Exception as e:
                    self.logger.warning(f"Validation or assignment error for field '{field}': {e}")
                    final_extracted[field] = None
                    extraction_confidence[field] = 0.0
            
            try:
                cleaned_data = self.cleaner.clean_extracted_data(final_extracted)
            except Exception as e:
                self.logger.warning(f"Data cleaning failed: {str(e)}")
                cleaned_data = final_extracted
            
            successful_fields = sum(1 for v in cleaned_data.values() if v is not None)
            if successful_fields > 0:
                chunk_quality['successful_extractions'] += 1
                self.processing_stats['successful_extractions'] += 1
                self.processing_stats['extracted_fields'] += successful_fields
            else:
                self.processing_stats['failed_extractions'] += 1
            
            for field, confidence in extraction_confidence.items():
                if field not in self.quality_metrics['extraction_confidence']:
                    self.quality_metrics['extraction_confidence'][field] = []
                self.quality_metrics['extraction_confidence'][field].append(confidence)
            
            return cleaned_data

        except Exception as e:
            self.logger.error(f"Critical extraction error: {str(e)}")
            self.processing_stats['failed_extractions'] += 1
            return {field: None for field in self.extractor.get_field_list() or []}
    
    def _validate_field_value(self, value, field):
        # Convert to string if needed
        if value is None:
            return False
        
        # Defensive: if it's not str or bytes, convert to str
        if not isinstance(value, (str, bytes)):
            value = str(value)
        
        # Now you can safely run regex or other string ops
        # Example:
        if field == 'vlan':
            # example regex or numeric check
            return bool(re.match(r'^\d+$', value))
        
        # other fields validation here...
        return True
    
    def _validate_chunk_data(self, chunk_df, chunk_quality):
        """Validate chunk data for quality issues"""
        validation_issues = []
        
        try:
            # Check for duplicate values in unique fields
            unique_fields = ['serial', 'mac', 'serial_code']
            for field in unique_fields:
                if field in chunk_df.columns:
                    non_null_values = chunk_df[field].dropna()
                    if len(non_null_values) != len(non_null_values.unique()):
                        duplicates = non_null_values[non_null_values.duplicated()].unique()
                        validation_issues.append({
                            'type': 'duplicate_values',
                            'field': field,
                            'values': duplicates.tolist()
                        })
            
            # Check for suspicious patterns
            for field in self.extractor.get_field_list():
                if field in chunk_df.columns:
                    values = chunk_df[field].dropna()
                    if len(values) > 0:
                        # Check for too many identical values
                        value_counts = values.value_counts()
                        if len(value_counts) > 0:
                            most_common_count = value_counts.iloc[0]
                            if most_common_count > len(values) * 0.8:  # 80% same value
                                validation_issues.append({
                                    'type': 'suspicious_repetition',
                                    'field': field,
                                    'value': value_counts.index[0],
                                    'count': most_common_count
                                })
            
        except Exception as e:
            validation_issues.append({
                'type': 'validation_error',
                'message': str(e)
            })
        
        return validation_issues
    
    def _update_quality_metrics(self, chunk_quality):
        """Update global quality metrics from chunk results"""
        # Update field coverage
        for field in self.extractor.get_field_list():
            if chunk_quality['successful_extractions'] > 0:
                self.quality_metrics['field_coverage'][field] += chunk_quality['successful_extractions']
        
        # Update cleaning effectiveness
        if chunk_quality['cleaning_stats']:
            avg_reduction = np.mean([
                stats.get('reduction_percentage', 0) 
                for stats in chunk_quality['cleaning_stats']
            ])
            self.quality_metrics['cleaning_effectiveness'][chunk_quality['chunk_num']] = avg_reduction
        
        # Track validation issues
        if chunk_quality['validation_issues']:
            self.quality_metrics['data_quality_issues'].extend(chunk_quality['validation_issues'])
    
    def _calculate_success_rate(self):
        """Calculate overall extraction success rate"""
        total_attempts = self.processing_stats.get('successful_extractions', 0) + self.processing_stats.get('failed_extractions', 0)
        if total_attempts == 0:
            return 0.0
        
        return (self.processing_stats.get('successful_extractions', 0) / total_attempts) * 100
    
    def _calculate_enhanced_field_stats(self, df):
        """Calculate comprehensive field statistics with robust error handling"""
        field_stats = {}
        total_rows = len(df)
        
        for field in self.extractor.get_field_list():
            try:
                if field in df.columns:
                    # Safe basic statistics calculation
                    try:
                        non_null_series = df[field].notna()
                        # Safe conversion to int
                        if hasattr(non_null_series, 'sum'):
                            non_null_sum = non_null_series.sum()
                            if hasattr(non_null_sum, 'item'):
                                non_null_count = int(non_null_sum.item())
                            else:
                                non_null_count = int(non_null_sum)
                        else:
                            non_null_count = 0
                    except (ValueError, TypeError, AttributeError):
                        non_null_count = 0
                    
                    percentage = (non_null_count / total_rows) * 100 if total_rows > 0 else 0
                    
                    # Enhanced statistics with error handling
                    try:
                        unique_values = df[field].dropna().nunique()
                    except Exception:
                        unique_values = 0
                    
                    sample_values = self._get_sample_values_safe(df[field], max_samples=5)
                    
                    # Confidence metrics with safe access
                    avg_confidence = 0.0
                    try:
                        if field in self.quality_metrics.get('extraction_confidence', {}):
                            confidences = self.quality_metrics['extraction_confidence'][field]
                            if confidences and len(confidences) > 0:
                                avg_confidence = sum(confidences) / len(confidences)
                    except (KeyError, TypeError, ZeroDivisionError):
                        avg_confidence = 0.0
                    
                    # Data quality assessment
                    quality_score = self._calculate_field_quality_score(df[field], field)
                    
                    field_stats[field] = {
                        'count': non_null_count,
                        'percentage': round(percentage, 1),
                        'unique_values': unique_values,
                        'sample_values': sample_values,
                        'avg_confidence': round(avg_confidence, 3),
                        'quality_score': round(quality_score, 3),
                        'data_type': str(df[field].dtype) if field in df.columns else 'object'
                    }
                else:
                    field_stats[field] = {
                        'count': 0,
                        'percentage': 0.0,
                        'unique_values': 0,
                        'sample_values': [],
                        'avg_confidence': 0.0,
                        'quality_score': 0.0,
                        'data_type': 'object'
                    }
            except Exception as e:
                self.logger.error(f"Error calculating stats for field {field}: {str(e)}")
                field_stats[field] = {
                    'count': 0,
                    'percentage': 0.0,
                    'unique_values': 0,
                    'sample_values': [],
                    'avg_confidence': 0.0,
                    'quality_score': 0.0,
                    'data_type': 'error',
                    'error': str(e)
                }
        
        return field_stats
    
    def _calculate_field_quality_score(self, series, field):
        """Calculate quality score for a field (0.0 to 1.0) with robust error handling"""
        try:
            if series is None or len(series) == 0:
                return 0.0
                
            non_null_values = series.dropna()
            if len(non_null_values) == 0:
                return 0.0
            
            quality_score = 0.0
            
            # Factor 1: Fill rate (40% weight)
            try:
                fill_rate = len(non_null_values) / len(series)
                quality_score += fill_rate * 0.4
            except (ZeroDivisionError, TypeError):
                pass
            
            # Factor 2: Uniqueness for unique fields (20% weight)
            try:
                if field in ['serial', 'mac', 'serial_code']:
                    uniqueness = non_null_values.nunique() / len(non_null_values)
                    quality_score += uniqueness * 0.2
                else:
                    # For non-unique fields, reasonable diversity is good
                    if non_null_values.nunique() > 1:
                        quality_score += 0.2
            except (ZeroDivisionError, TypeError, AttributeError):
                pass
            
            # Factor 3: Format validity (40% weight)
            try:
                valid_count = 0
                for value in non_null_values:
                    try:
                        if self._validate_field_value(value, field):
                            valid_count += 1
                    except Exception:
                        continue
                
                if len(non_null_values) > 0:
                    format_validity = valid_count / len(non_null_values)
                    quality_score += format_validity * 0.4
            except (ZeroDivisionError, TypeError):
                pass
            
            return min(1.0, max(0.0, quality_score))
            
        except Exception:
            return 0.0
    
    def _get_sample_values_safe(self, series, max_samples=5):
        """Get sample values from a series safely"""
        try:
            non_null_values = series.dropna()
            if len(non_null_values) == 0:
                return []
            
            unique_values = non_null_values.unique()
            sample_size = min(max_samples, len(unique_values))
            
            return [str(val) for val in unique_values[:sample_size]]
        except Exception:
            return []
    
    def _validate_extracted_data(self, df):
        """Comprehensive data validation"""
        validation_results = {
            'overall_quality': 'good',
            'issues_found': [],
            'recommendations': [],
            'field_quality_summary': {}
        }
        
        try:
            total_issues = 0
            
            # Check each field for quality issues
            for field in self.extractor.get_field_list():
                if field in df.columns:
                    field_issues = []
                    
                    # Check fill rate
                    fill_rate = df[field].notna().sum() / len(df)
                    if fill_rate < 0.1:  # Less than 10%
                        field_issues.append(f"Very low fill rate: {fill_rate:.1%}")
                    
                    # Check for suspicious patterns
                    non_null_values = df[field].dropna()
                    if len(non_null_values) > 0:
                        # Check for too many identical values
                        value_counts = non_null_values.value_counts()
                        if len(value_counts) > 0:
                            most_common_ratio = value_counts.iloc[0] / len(non_null_values)
                            if most_common_ratio > 0.8:
                                field_issues.append(f"Suspicious repetition: {most_common_ratio:.1%} same value")
                    
                    # Check format validity
                    if len(non_null_values) > 0:
                        valid_count = sum(1 for val in non_null_values if self._validate_field_value(val, field))
                        validity_rate = valid_count / len(non_null_values)
                        if validity_rate < 0.8:
                            field_issues.append(f"Low format validity: {validity_rate:.1%}")
                    
                    if field_issues:
                        validation_results['issues_found'].extend([f"{field}: {issue}" for issue in field_issues])
                        total_issues += len(field_issues)
                    
                    validation_results['field_quality_summary'][field] = {
                        'fill_rate': fill_rate,
                        'issues_count': len(field_issues),
                        'status': 'good' if len(field_issues) == 0 else 'needs_attention'
                    }
            
            # Overall quality assessment
            if total_issues == 0:
                validation_results['overall_quality'] = 'excellent'
            elif total_issues <= 5:
                validation_results['overall_quality'] = 'good'
            elif total_issues <= 15:
                validation_results['overall_quality'] = 'fair'
            else:
                validation_results['overall_quality'] = 'poor'
            
            # Generate recommendations
            if total_issues > 0:
                validation_results['recommendations'] = self._generate_quality_recommendations(validation_results)
            
        except Exception as e:
            validation_results['issues_found'].append(f"Validation error: {str(e)}")
            validation_results['overall_quality'] = 'unknown'
        
        return validation_results
    
    def _generate_quality_recommendations(self, validation_results):
        """Generate recommendations based on validation results"""
        recommendations = []
        
        # Low fill rate recommendations
        low_fill_fields = [
            field for field, summary in validation_results['field_quality_summary'].items()
            if summary['fill_rate'] < 0.2
        ]
        
        if low_fill_fields:
            recommendations.append(
                f"Consider improving text cleaning patterns for low-extraction fields: {', '.join(low_fill_fields)}"
            )
        
        # Repetition recommendations
        repetition_issues = [
            issue for issue in validation_results['issues_found']
            if 'repetition' in issue.lower()
        ]
        
        if repetition_issues:
            recommendations.append(
                "Review extraction patterns to avoid false positives causing repetitive values"
            )
        
        # Format validity recommendations
        validity_issues = [
            issue for issue in validation_results['issues_found']
            if 'validity' in issue.lower()
        ]
        
        if validity_issues:
            recommendations.append(
                "Enhance field validation rules to improve data format consistency"
            )
        
        return recommendations
    
    def _generate_cleaning_report(self):
        """Generate detailed cleaning effectiveness report"""
        report = {
            'summary': {},
            'effectiveness_by_chunk': {},
            'pattern_analysis': {},
            'recommendations': []
        }
        
        try:
            # Summary statistics
            total_original = self.processing_stats.get('original_chars', 0)
            total_cleaned = self.processing_stats.get('cleaned_chars', 0)
            
            if total_original > 0:
                overall_reduction = ((total_original - total_cleaned) / total_original) * 100
                report['summary'] = {
                    'total_original_chars': total_original,
                    'total_cleaned_chars': total_cleaned,
                    'overall_reduction_percentage': round(overall_reduction, 2),
                    'avg_chars_per_row_before': round(total_original / max(1, self.processing_stats.get('total_rows', 1)), 2),
                    'avg_chars_per_row_after': round(total_cleaned / max(1, self.processing_stats.get('total_rows', 1)), 2)
                }
            
            # Effectiveness by chunk
            if self.quality_metrics['cleaning_effectiveness']:
                report['effectiveness_by_chunk'] = dict(self.quality_metrics['cleaning_effectiveness'])
                
                avg_effectiveness = np.mean(list(self.quality_metrics['cleaning_effectiveness'].values()))
                report['summary']['avg_cleaning_effectiveness'] = round(avg_effectiveness, 2)
            
        except Exception as e:
            report['error'] = str(e)
        
        return report
    
    def get_processing_summary(self):
        """Get comprehensive processing summary with safe formatting"""
        if not self.processing_stats:
            return "No processing completed yet."
        
        # Calculate rates and percentages safely
        total_rows = self.processing_stats.get('total_rows', 0)
        successful_extractions = self.processing_stats.get('successful_extractions', 0)
        failed_extractions = self.processing_stats.get('failed_extractions', 0)
        
        total_attempts = successful_extractions + failed_extractions
        success_rate = (successful_extractions / max(1, total_attempts)) * 100
        
        original_chars = self.processing_stats.get('original_chars', 0)
        cleaned_chars = self.processing_stats.get('cleaned_chars', 0)
        reduction = ((original_chars - cleaned_chars) / max(1, original_chars)) * 100
        
        # Phase timing
        phases = self.processing_stats.get('processing_phases', {})
        total_time = self.processing_stats.get('processing_time', 0)
        
        try:
            summary = {
                'processing_overview': {
                    'total_rows': total_rows,
                    'chunks_processed': self.processing_stats.get('chunks_processed', 0),
                    'processing_time_seconds': round(total_time, 2),
                    'rows_per_second': round(total_rows / max(0.1, total_time), 2)
                },
                'extraction_performance': {
                    'successful_extractions': successful_extractions,
                    'failed_extractions': failed_extractions,
                    'success_rate_percentage': round(success_rate, 1),
                    'total_fields_extracted': self.processing_stats.get('extracted_fields', 0),
                    'avg_fields_per_successful_row': round(
                        self.processing_stats.get('extracted_fields', 0) / max(1, successful_extractions), 2
                    )
                },
                'text_cleaning': {
                    'original_chars': original_chars,
                    'cleaned_chars': cleaned_chars,
                    'reduction_percentage': round(reduction, 1),
                    'cleaning_effectiveness': 'High' if reduction > 30 else 'Medium' if reduction > 10 else 'Low'
                },
                'performance_breakdown': {
                    'reading_time': round(phases.get('reading', 0), 2),
                    'cleaning_time': round(phases.get('cleaning', 0), 2),
                    'extracting_time': round(phases.get('extracting', 0), 2),
                    'validating_time': round(phases.get('validating', 0), 2)
                }
            }
            
            return summary
            
        except Exception as e:
            # Return a safe minimal summary if formatting fails
            return {
                'processing_overview': {
                    'total_rows': total_rows,
                    'processing_time_seconds': round(total_time, 2),
                    'status': 'completed_with_formatting_errors'
                },
                'error': f"Summary formatting error: {str(e)}"
            }
    
    def export_quality_report(self, output_path=None):
        """Export detailed quality report to file"""
        if not output_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"quality_report_{timestamp}.json"
        
        quality_report = {
            'timestamp': datetime.now().isoformat(),
            'processing_summary': self.get_processing_summary(),
            'quality_metrics': {
                'extraction_confidence': {
                    field: {
                        'avg': round(np.mean(confidences), 3),
                        'min': round(np.min(confidences), 3),
                        'max': round(np.max(confidences), 3),
                        'count': len(confidences)
                    }
                    for field, confidences in self.quality_metrics['extraction_confidence'].items()
                    if confidences
                },
                'field_coverage': dict(self.quality_metrics['field_coverage']),
                'data_quality_issues': self.quality_metrics['data_quality_issues']
            }
        }
        
        try:
            import json
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(quality_report, f, indent=2, ensure_ascii=False)
            
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to export quality report: {str(e)}")
            return None