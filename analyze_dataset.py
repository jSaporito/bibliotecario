#!/usr/bin/env python3
"""
Large Sample Analyzer for 33K Row Dataset
Analyzes patterns in obs column to fine-tune text cleaning and extraction
"""

import pandas as pd
import re
import os
from collections import Counter, defaultdict
from datetime import datetime
import json

class LargeSampleAnalyzer:
    """
    Analyzes large CSV datasets to optimize text cleaning and extraction patterns
    """
    
    def __init__(self):
        self.analysis_results = {}
        self.patterns_found = defaultdict(list)
        self.noise_patterns = defaultdict(int)
        self.field_patterns = defaultdict(int)
    
    def analyze_large_sample(self, csv_file_path, obs_column='obs', sample_sizes=[1000, 5000, 10000]):
        """
        Analyze multiple sample sizes to understand data patterns
        """
        print("🔍 Large Sample Analysis Starting...")
        print("=" * 60)
        
        try:
            # First, get basic file info
            print("📊 Getting file overview...")
            file_info = self._get_file_overview(csv_file_path, obs_column)
            print(f"Total rows: {file_info['total_rows']:,}")
            print(f"Target column: '{obs_column}' ({file_info['obs_column_exists']})")
            print(f"Non-null obs entries: {file_info['non_null_obs']:,}")
            
            # Analyze different sample sizes
            all_results = {}
            
            for sample_size in sample_sizes:
                if sample_size > file_info['total_rows']:
                    print(f"⚠️ Skipping sample size {sample_size} (larger than dataset)")
                    continue
                
                print(f"\n🎯 Analyzing sample of {sample_size:,} rows...")
                
                # Get stratified sample
                sample_df = self._get_stratified_sample(csv_file_path, obs_column, sample_size)
                
                # Analyze this sample
                sample_results = self._analyze_sample(sample_df, obs_column, sample_size)
                all_results[sample_size] = sample_results
                
                # Print key findings
                self._print_sample_summary(sample_results, sample_size)
            
            # Compare results across sample sizes
            print(f"\n📈 Cross-Sample Analysis...")
            comparison = self._compare_samples(all_results)
            
            # Generate recommendations
            print(f"\n💡 Recommendations for 33K Dataset...")
            recommendations = self._generate_recommendations(all_results, file_info)
            
            # Save detailed analysis
            analysis_report = {
                'file_info': file_info,
                'sample_analyses': all_results,
                'comparison': comparison,
                'recommendations': recommendations,
                'timestamp': datetime.now().isoformat()
            }
            
            report_file = f"analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_report, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"\n📋 Detailed analysis saved to: {report_file}")
            
            return analysis_report
            
        except Exception as e:
            print(f"❌ Analysis failed: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def _get_file_overview(self, csv_file_path, obs_column):
        """Get basic file information"""
        try:
            # Read just the header and a few rows for quick analysis
            df_sample = pd.read_csv(csv_file_path, nrows=10)
            
            # Get total row count efficiently
            total_rows = sum(1 for _ in open(csv_file_path, 'r', encoding='utf-8')) - 1  # -1 for header
            
            info = {
                'file_path': csv_file_path,
                'total_rows': total_rows,
                'total_columns': len(df_sample.columns),
                'columns': list(df_sample.columns),
                'obs_column_exists': obs_column in df_sample.columns,
            }
            
            # Count non-null obs entries
            if obs_column in df_sample.columns:
                # Sample approach to estimate non-null entries
                sample_size = min(1000, total_rows)
                df_check = pd.read_csv(csv_file_path, nrows=sample_size)
                non_null_ratio = df_check[obs_column].notna().sum() / len(df_check)
                info['non_null_obs'] = int(total_rows * non_null_ratio)
            else:
                info['non_null_obs'] = 0
            
            return info
            
        except Exception as e:
            return {'error': str(e)}
    
    def _get_stratified_sample(self, csv_file_path, obs_column, sample_size):
        """Get a representative stratified sample"""
        try:
            # Read the full dataset (for datasets up to 33K this should be manageable)
            print(f"   Reading dataset for sampling...")
            df = pd.read_csv(csv_file_path, low_memory=False)
            
            # Filter to rows with non-null obs data
            df_with_obs = df[df[obs_column].notna() & (df[obs_column] != '')].copy()
            
            if len(df_with_obs) == 0:
                print("   ⚠️ No valid obs data found")
                return df.head(sample_size)
            
            # Create stratified sample based on text length
            df_with_obs['text_length'] = df_with_obs[obs_column].astype(str).str.len()
            
            # Create length bins
            df_with_obs['length_bin'] = pd.qcut(df_with_obs['text_length'], 
                                              q=5, labels=['very_short', 'short', 'medium', 'long', 'very_long'],
                                              duplicates='drop')
            
            # Sample from each bin
            samples = []
            per_bin = sample_size // df_with_obs['length_bin'].nunique()
            
            for bin_name in df_with_obs['length_bin'].unique():
                if pd.isna(bin_name):
                    continue
                bin_data = df_with_obs[df_with_obs['length_bin'] == bin_name]
                bin_sample = bin_data.sample(n=min(per_bin, len(bin_data)), random_state=42)
                samples.append(bin_sample)
            
            # Combine samples
            if samples:
                stratified_sample = pd.concat(samples, ignore_index=True)
                # Fill up to requested sample size if needed
                if len(stratified_sample) < sample_size:
                    remaining = sample_size - len(stratified_sample)
                    extra_sample = df_with_obs.sample(n=min(remaining, len(df_with_obs)), random_state=42)
                    stratified_sample = pd.concat([stratified_sample, extra_sample], ignore_index=True).drop_duplicates()
            else:
                stratified_sample = df_with_obs.head(sample_size)
            
            print(f"   ✅ Stratified sample created: {len(stratified_sample)} rows")
            return stratified_sample
            
        except Exception as e:
            print(f"   ⚠️ Stratified sampling failed, using random sample: {e}")
            # Fallback to simple random sample
            df = pd.read_csv(csv_file_path, nrows=sample_size * 2)  # Read extra to ensure we have enough
            df_filtered = df[df[obs_column].notna()]
            return df_filtered.head(sample_size)
    
    def _analyze_sample(self, df, obs_column, sample_size):
        """Analyze a sample for patterns"""
        results = {
            'sample_size': sample_size,
            'text_stats': {},
            'noise_patterns': {},
            'field_patterns': {},
            'extraction_potential': {},
            'cleaning_impact': {}
        }
        
        try:
            obs_texts = df[obs_column].dropna().astype(str)
            
            # Basic text statistics
            results['text_stats'] = {
                'total_entries': len(obs_texts),
                'avg_length': obs_texts.str.len().mean(),
                'median_length': obs_texts.str.len().median(),
                'max_length': obs_texts.str.len().max(),
                'min_length': obs_texts.str.len().min(),
                'total_chars': obs_texts.str.len().sum(),
                'avg_lines': obs_texts.str.split('\n').str.len().mean(),
            }
            
            # Analyze noise patterns
            noise_patterns = {
                'separator_lines': 0,
                'empty_lines': 0,
                'debug_lines': 0,
                'command_noise': 0,
                'html_tags': 0
            }
            
            # Analyze field extraction potential
            field_patterns = {
                'ip_addresses': 0,
                'vlan_ids': 0,
                'serial_numbers': 0,
                'ticket_numbers': 0,
                'equipment_codes': 0,
                'service_codes': 0,
                'asn_numbers': 0,
                'mac_addresses': 0
            }
            
            # Pattern definitions
            noise_regex = {
                'separator_lines': r'^[-=_~*+#]{10,}',
                'empty_lines': r'^\s*$',
                'debug_lines': r'^\s*(DEBUG|INFO|WARNING|ERROR):',
                'command_noise': r'^\s*(quit|exit|end|return)\s*$',
                'html_tags': r'<[^>]+>'
            }
            
            field_regex = {
                'ip_addresses': r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
                'vlan_ids': r'VLAN\s*:?\s*\d+',
                'serial_numbers': r'SN[:\s]*[A-Za-z0-9]+',
                'ticket_numbers': r'#\d{8}-\d+',
                'equipment_codes': r'OLT-[A-Z0-9-]+',
                'service_codes': r'[A-Z]{3,}/[A-Z]{2,}/\d+',
                'asn_numbers': r'AS\s*Cliente[:\s]*\d+',
                'mac_addresses': r'([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}'
            }
            
            # Count patterns in sample
            for text in obs_texts.head(min(500, len(obs_texts))):  # Analyze subset for speed
                # Count noise patterns
                for pattern_name, regex in noise_regex.items():
                    matches = len(re.findall(regex, text, re.MULTILINE | re.IGNORECASE))
                    noise_patterns[pattern_name] += matches
                
                # Count field patterns
                for pattern_name, regex in field_regex.items():
                    matches = len(re.findall(regex, text, re.IGNORECASE))
                    field_patterns[pattern_name] += matches
            
            results['noise_patterns'] = noise_patterns
            results['field_patterns'] = field_patterns
            
            # Estimate cleaning impact
            sample_text = '\n'.join(obs_texts.head(100))
            cleaned_text = self._simulate_cleaning(sample_text)
            
            results['cleaning_impact'] = {
                'original_length': len(sample_text),
                'cleaned_length': len(cleaned_text),
                'reduction_percent': round(100 * (1 - len(cleaned_text) / len(sample_text)), 2) if len(sample_text) > 0 else 0,
                'estimated_total_reduction_mb': round((len(sample_text) - len(cleaned_text)) * sample_size / (1024 * 1024), 2)
            }
            
            return results
            
        except Exception as e:
            results['error'] = str(e)
            return results
    
    def _simulate_cleaning(self, text):
        """Simulate text cleaning to estimate impact"""
        if not text:
            return text
        
        cleaned = text
        
        # Remove separator lines
        cleaned = re.sub(r'^[-=_~*+#]{10,}.*$', '', cleaned, flags=re.MULTILINE)
        
        # Remove empty hash lines
        cleaned = re.sub(r'^#+\s*$', '', cleaned, flags=re.MULTILINE)
        
        # Remove debug lines
        cleaned = re.sub(r'^\s*(DEBUG|INFO|WARNING|ERROR):.*$', '', cleaned, flags=re.MULTILINE)
        
        # Remove command noise
        cleaned = re.sub(r'^\s*(quit|exit|end|return)\s*$', '', cleaned, flags=re.MULTILINE)
        
        # Clean multiple newlines
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
        
        # Remove excessive whitespace
        cleaned = re.sub(r'[ \t]{3,}', ' ', cleaned)
        
        return cleaned.strip()
    
    def _print_sample_summary(self, results, sample_size):
        """Print summary of sample analysis"""
        if 'error' in results:
            print(f"   ❌ Analysis failed: {results['error']}")
            return
        
        stats = results['text_stats']
        noise = results['noise_patterns']
        fields = results['field_patterns']
        cleaning = results['cleaning_impact']
        
        print(f"   📊 Text Statistics:")
        print(f"      Avg length: {stats['avg_length']:.0f} chars")
        print(f"      Avg lines: {stats['avg_lines']:.1f}")
        print(f"      Total data: {stats['total_chars'] / (1024*1024):.1f} MB")
        
        print(f"   🧹 Noise Found:")
        total_noise = sum(noise.values())
        print(f"      Total noise items: {total_noise:,}")
        for pattern, count in sorted(noise.items(), key=lambda x: x[1], reverse=True)[:3]:
            if count > 0:
                print(f"      {pattern.replace('_', ' ').title()}: {count:,}")
        
        print(f"   🔍 Fields Found:")
        total_fields = sum(fields.values())
        print(f"      Total field matches: {total_fields:,}")
        for pattern, count in sorted(fields.items(), key=lambda x: x[1], reverse=True)[:3]:
            if count > 0:
                print(f"      {pattern.replace('_', ' ').title()}: {count:,}")
        
        print(f"   🎯 Cleaning Impact:")
        print(f"      Text reduction: {cleaning['reduction_percent']}%")
        print(f"      Estimated savings: {cleaning['estimated_total_reduction_mb']:.1f} MB")
    
    def _compare_samples(self, all_results):
        """Compare results across different sample sizes"""
        comparison = {
            'consistency': {},
            'scaling_factors': {}
        }
        
        # Check consistency of patterns across sample sizes
        sample_sizes = sorted(all_results.keys())
        
        if len(sample_sizes) > 1:
            base_size = sample_sizes[0]
            base_results = all_results[base_size]
            
            for size in sample_sizes[1:]:
                current_results = all_results[size]
                
                # Compare field pattern ratios
                base_fields = base_results['field_patterns']
                current_fields = current_results['field_patterns']
                
                pattern_consistency = {}
                for pattern in base_fields:
                    if base_fields[pattern] > 0 and current_fields[pattern] > 0:
                        ratio = current_fields[pattern] / current_fields['total_entries'] / (base_fields[pattern] / base_results['sample_size'])
                        pattern_consistency[pattern] = ratio
                
                comparison['consistency'][size] = pattern_consistency
        
        return comparison
    
    def _generate_recommendations(self, all_results, file_info):
        """Generate recommendations for processing the full 33K dataset"""
        recommendations = {
            'processing_strategy': {},
            'optimization_suggestions': [],
            'expected_results': {}
        }
        
        # Get the largest sample results
        largest_sample = max(all_results.keys())
        best_results = all_results[largest_sample]
        
        total_rows = file_info['total_rows']
        
        # Processing strategy
        if total_rows > 10000:
            recommendations['processing_strategy'] = {
                'chunk_size': 2000,
                'parallel_processing': True,
                'memory_management': 'Use chunked processing to avoid memory issues'
            }
        else:
            recommendations['processing_strategy'] = {
                'chunk_size': 5000,
                'parallel_processing': False,
                'memory_management': 'Can process in single batch'
            }
        
        # Optimization suggestions
        if best_results['cleaning_impact']['reduction_percent'] > 10:
            recommendations['optimization_suggestions'].append(
                f"Text cleaning will reduce data by ~{best_results['cleaning_impact']['reduction_percent']}%, "
                f"saving ~{best_results['cleaning_impact']['estimated_total_reduction_mb'] * (total_rows / largest_sample):.1f} MB"
            )
        
        total_field_potential = sum(best_results['field_patterns'].values())
        if total_field_potential > largest_sample * 0.5:  # More than 0.5 fields per row on average
            recommendations['optimization_suggestions'].append(
                f"High field extraction potential: ~{total_field_potential * (total_rows / largest_sample):.0f} total fields extractable"
            )
        
        # Expected results for full dataset
        scale_factor = total_rows / largest_sample
        recommendations['expected_results'] = {
            'estimated_processing_time': f"{(total_rows / 1000):.1f}-{(total_rows / 500):.1f} minutes",
            'estimated_fields_extracted': int(total_field_potential * scale_factor),
            'estimated_data_reduction_mb': round(best_results['cleaning_impact']['estimated_total_reduction_mb'] * scale_factor, 1),
            'recommended_export_formats': ['excel', 'csv'] if total_rows < 50000 else ['csv', 'json']
        }
        
        return recommendations

def main():
    CSV_FILE = "your_33k_dataset.csv"  # Update this path
    OBS_COLUMN = "obs"
    SAMPLE_SIZES = [1000, 5000, 10000]  # Progressive sample sizes
    
    print("🚀 Large Dataset Analysis Tool")
    print("=" * 50)
    
    if not os.path.exists(CSV_FILE):
        print(f"❌ CSV file not found: {CSV_FILE}")
        print("Please update the CSV_FILE path in the script")
        return
    
    # Run analysis
    analyzer = LargeSampleAnalyzer()
    results = analyzer.analyze_large_sample(CSV_FILE, OBS_COLUMN, SAMPLE_SIZES)
    
    if results:
        print(f"\n✅ Analysis completed successfully!")
        print(f"📋 Check the generated JSON report for detailed findings")
        
        # Print key recommendations
        if 'recommendations' in results:
            recs = results['recommendations']
            print(f"\n🎯 Key Recommendations:")
            print(f"   Chunk size: {recs['processing_strategy']['chunk_size']:,} rows")
            print(f"   Expected processing time: {recs['expected_results']['estimated_processing_time']}")
            print(f"   Expected fields extracted: {recs['expected_results']['estimated_fields_extracted']:,}")
            print(f"   Data reduction: {recs['expected_results']['estimated_data_reduction_mb']} MB")
    else:
        print(f"❌ Analysis failed")

if __name__ == "__main__":
    main()