__version__ = '2.0.0'
__author__ = 'Bibliotecario Development Team'

from collections import defaultdict
import numpy as np

# Import enhanced classes from separate modules
from .data_processor import DataProcessor
from .text_cleaner import TextCleaner
from .text_extractor import TextExtractor
from .export_handler import ExportHandler



# Package-level constants
DEFAULT_CHUNK_SIZE = 5000
SUPPORTED_FORMATS = ['csv', 'excel', 'json']

# Initialize extractor to get dynamic field list
_dummy_extractor = TextExtractor()
ALL_FIELDS = _dummy_extractor.get_field_list()
DEFAULT_FIELD_COUNT = len(ALL_FIELDS)

# Enhanced field categories with better organization
NETWORK_FIELDS = [
    'ip_management', 'gateway', 'ip_block', 'ip_telephony',
    'vlan', 'asn', 'mac', 'prefixes'
]

HARDWARE_FIELDS = [
    'cpe', 'serial', 'model_onu', 'onu_id', 'pon_port',
    'slot', 'serial_code'
]

CONFIGURATION_FIELDS = [
    'wifi_ssid', 'wifi_passcode', 'login_pppoe',
    'interface_1', 'interface_2', 'access_point'
]

BUSINESS_FIELDS = [
    'technology_id', 'provider_id', 'pop', 'pop_description',
    'partnerid', 'circuitpartner'
]

# Field priority mapping for extraction preferences
FIELD_PRIORITIES = {
    'ip_management': 10,
    'gateway': 9,
    'ip_telephony': 8,
    'ip_block': 7,
    'mac': 6,
    'vlan': 5,
    'asn': 4,
    'serial': 3,
    'cpe': 2,
    'pop': 1
}

def get_version():
    """Get package version"""
    return __version__

def get_supported_fields():
    """Get list of all supported extraction fields"""
    return ALL_FIELDS.copy()

def get_fields_by_category():
    """Get fields organized by category"""
    return {
        'network': NETWORK_FIELDS,
        'hardware': HARDWARE_FIELDS,
        'configuration': CONFIGURATION_FIELDS,
        'business': BUSINESS_FIELDS
    }

def get_field_priorities():
    """Get field priority mapping for extraction"""
    return FIELD_PRIORITIES.copy()

def create_processor(chunk_size=None, enable_advanced_stats=True, log_level='INFO'):
    """
    Factory function to create an enhanced DataProcessor instance
    
    Args:
        chunk_size (int): Number of rows to process per chunk
        enable_advanced_stats (bool): Enable advanced statistics collection
        log_level (str): Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR')
        
    Returns:
        DataProcessor: Configured processor instance
    """
    import logging
    
    if chunk_size is None:
        chunk_size = DEFAULT_CHUNK_SIZE
    
    # Convert string log level to logging constant
    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR
    }
    
    numeric_level = log_level_map.get(log_level.upper(), logging.INFO)
    
    return DataProcessor(
        chunk_size=chunk_size,
        enable_advanced_stats=enable_advanced_stats,
        log_level=numeric_level
    )

def create_extractor():
    """
    Factory function to create an enhanced TextExtractor instance
    
    Returns:
        TextExtractor: Configured extractor instance
    """
    return TextExtractor()

def create_cleaner():
    """
    Factory function to create an enhanced TextCleaner instance
    
    Returns:
        TextCleaner: Configured cleaner instance
    """
    return TextCleaner()

def create_exporter():
    """
    Factory function to create an ExportHandler instance
    
    Returns:
        ExportHandler: Configured export handler instance
    """
    return ExportHandler()

def validate_field_name(field_name):
    """
    Validate if a field name is supported
    
    Args:
        field_name (str): Field name to validate
        
    Returns:
        bool: True if field is supported, False otherwise
    """
    return field_name in ALL_FIELDS

def get_field_category(field_name):
    """
    Get the category of a field
    
    Args:
        field_name (str): Field name
        
    Returns:
        str: Category name or None if field not found
    """
    categories = get_fields_by_category()
    
    for category, fields in categories.items():
        if field_name in fields:
            return category
    
    return None

def get_processing_stats_template():
    """
    Get an enhanced template for processing statistics
    
    Returns:
        dict: Empty statistics template with enhanced metrics
    """
    return {
        'processing_overview': {
            'total_rows': 0,
            'chunks_processed': 0,
            'processing_time_seconds': 0.0,
            'rows_per_second': 0.0
        },
        'extraction_performance': {
            'successful_extractions': 0,
            'failed_extractions': 0,
            'success_rate_percentage': 0.0,
            'total_fields_extracted': 0,
            'avg_fields_per_row': 0.0
        },
        'text_cleaning': {
            'original_chars': 0,
            'cleaned_chars': 0,
            'reduction_percentage': 0.0,
            'cleaning_effectiveness': 'Unknown'
        },
        'field_statistics': {
            field: {
                'count': 0, 
                'percentage': 0.0,
                'unique_values': 0,
                'quality_score': 0.0,
                'avg_confidence': 0.0
            } for field in ALL_FIELDS
        },
        'quality_metrics': {
            'overall_quality': 'unknown',
            'issues_found': [],
            'recommendations': []
        }
    }

# Enhanced initialization
def initialize_core(verbose=True):
    """
    Initialize the enhanced core package
    
    Args:
        verbose (bool): Print initialization information
    """
    if verbose:
        print(f"Bibliotecario Core v{__version__} initialized")
        print(f"Enhanced features: Advanced statistics, confidence scoring, validation")
        print(f"Supported fields: {len(ALL_FIELDS)}")
        print(f"Field categories: {len(get_fields_by_category())}")
        print(f"Default chunk size: {DEFAULT_CHUNK_SIZE}")

# Enhanced validation functions
def validate_chunk_size(chunk_size):
    """
    Validate chunk size parameter with enhanced checks
    
    Args:
        chunk_size (int): Chunk size to validate
        
    Returns:
        dict: Validation result with status and recommendations
    """
    result = {'valid': False, 'message': '', 'recommended': None}
    
    if not isinstance(chunk_size, int):
        result['message'] = 'Chunk size must be an integer'
        result['recommended'] = DEFAULT_CHUNK_SIZE
        return result
    
    if chunk_size < 100:
        result['message'] = 'Chunk size too small (minimum: 100)'
        result['recommended'] = 1000
        return result
    
    if chunk_size > 50000:
        result['message'] = 'Chunk size too large (maximum: 50000)'
        result['recommended'] = 10000
        return result
    
    # Optimal range recommendations
    if chunk_size < 1000:
        result['message'] = 'Chunk size is valid but small. Consider larger chunks for better performance.'
        result['recommended'] = 5000
    elif chunk_size > 20000:
        result['message'] = 'Chunk size is valid but large. Consider smaller chunks if memory is limited.'
        result['recommended'] = 10000
    else:
        result['message'] = 'Chunk size is optimal'
    
    result['valid'] = True
    return result

def validate_export_format(format_name):
    """
    Enhanced export format validation
    
    Args:
        format_name (str): Format name to validate
        
    Returns:
        dict: Validation result with details
    """
    result = {'valid': False, 'message': '', 'available_formats': SUPPORTED_FORMATS}
    
    if not isinstance(format_name, str):
        result['message'] = 'Format name must be a string'
        return result
    
    format_lower = format_name.lower()
    
    if format_lower in SUPPORTED_FORMATS:
        result['valid'] = True
        result['message'] = f'Format {format_name} is supported'
    elif format_lower in ['both', 'all']:
        result['valid'] = True
        result['message'] = f'Multi-format option {format_name} is supported'
    else:
        result['message'] = f'Format {format_name} not supported. Available: {", ".join(SUPPORTED_FORMATS)}'
    
    return result

# Enhanced error classes with better context
class CoreProcessingError(Exception):
    """Base exception for core processing errors with enhanced context"""
    def __init__(self, message, error_code=None, context=None):
        super().__init__(message)
        self.error_code = error_code
        self.context = context or {}

class TextExtractionError(CoreProcessingError):
    """Exception for text extraction errors with field context"""
    def __init__(self, message, field=None, pattern=None, **kwargs):
        super().__init__(message, **kwargs)
        self.field = field
        self.pattern = pattern

class TextCleaningError(CoreProcessingError):
    """Exception for text cleaning errors with text context"""
    def __init__(self, message, original_text=None, **kwargs):
        super().__init__(message, **kwargs)
        self.original_text = original_text

class DataProcessingError(CoreProcessingError):
    """Exception for data processing errors with chunk context"""
    def __init__(self, message, chunk_number=None, row_number=None, **kwargs):
        super().__init__(message, **kwargs)
        self.chunk_number = chunk_number
        self.row_number = row_number

class ExportError(CoreProcessingError):
    """Exception for export errors with format context"""
    def __init__(self, message, format_name=None, file_path=None, **kwargs):
        super().__init__(message, **kwargs)
        self.format_name = format_name
        self.file_path = file_path

class ValidationError(CoreProcessingError):
    """Exception for validation errors with field and value context"""
    def __init__(self, message, field=None, value=None, **kwargs):
        super().__init__(message, **kwargs)
        self.field = field
        self.value = value

# Enhanced convenience functions
def quick_extract(text, fields=None, confidence_threshold=0.3):
    """
    Quick text extraction with confidence filtering
    
    Args:
        text (str): Text to extract from
        fields (list): Specific fields to extract (optional)
        confidence_threshold (float): Minimum confidence score (0.0-1.0)
        
    Returns:
        dict: Extracted data with confidence scores
    """
    extractor = create_extractor()
    result = extractor.process_text(text)
    
    # Get extraction statistics for confidence calculation
    stats = extractor.get_extraction_stats(text)
    
    # Filter by confidence if threshold is set
    if confidence_threshold > 0:
        # Simple confidence estimation based on pattern matches
        confident_results = {}
        for field, value in result.items():
            if value and stats['field_matches'].get(field, 0) > 0:
                # Basic confidence: more matches = higher confidence
                confidence = min(1.0, stats['field_matches'][field] * 0.2)
                if confidence >= confidence_threshold:
                    confident_results[field] = value
        result = confident_results
    
    # Filter by specific fields if requested
    if fields:
        result = {k: v for k, v in result.items() if k in fields}
    
    return result

def quick_clean(text, get_stats=False):
    """
    Quick text cleaning with optional statistics
    
    Args:
        text (str): Text to clean
        get_stats (bool): Return cleaning statistics
        
    Returns:
        str or tuple: Cleaned text, optionally with statistics
    """
    cleaner = create_cleaner()
    cleaned_text = cleaner.clean_text(text)
    
    if get_stats:
        stats = cleaner.get_cleaning_stats(text, cleaned_text)
        return cleaned_text, stats
    
    return cleaned_text

def quick_process(csv_path, obs_column='obs', chunk_size=None, enable_validation=True, 
                 output_formats=['csv'], progress_callback=None):
    """
    Quick CSV processing with enhanced options
    
    Args:
        csv_path (str): Path to CSV file
        obs_column (str): Column name containing text
        chunk_size (int): Processing chunk size
        enable_validation (bool): Enable data validation
        output_formats (list): Export formats ['csv', 'excel', 'json']
        progress_callback (callable): Progress update function
        
    Returns:
        dict: Enhanced processing results
    """
    processor = create_processor(chunk_size)
    
    # Process the CSV
    results = processor.process_csv(
        csv_path, 
        obs_column=obs_column,
        progress_callback=progress_callback,
        enable_validation=enable_validation
    )
    
    # Add export functionality if processing succeeded
    if results['success'] and results['dataframe'] is not None:
        try:
            from .export_handler import ExportHandler
            exporter = ExportHandler()
            
            # Generate output filename
            import os
            base_name = os.path.splitext(os.path.basename(csv_path))[0]
            output_dir = os.path.dirname(csv_path) or '.'
            
            # Export in requested formats
            export_results = exporter.export_data(
                results['dataframe'],
                output_dir,
                f"{base_name}_processed",
                output_formats
            )
            
            results['export_results'] = export_results
            
        except Exception as e:
            results['warnings'] = results.get('warnings', [])
            results['warnings'].append(f"Export failed: {str(e)}")
    
    return results

def analyze_text_sample(text_sample, show_patterns=True, show_confidence=True):
    """
    Analyze a text sample to understand extraction potential
    
    Args:
        text_sample (str): Sample text to analyze
        show_patterns (bool): Show pattern matching details
        show_confidence (bool): Show confidence estimates
        
    Returns:
        dict: Analysis results
    """
    extractor = create_extractor()
    cleaner = create_cleaner()
    
    analysis = {
        'original_text': text_sample,
        'text_stats': {
            'length': len(text_sample),
            'lines': len(text_sample.split('\n')),
            'words': len(text_sample.split())
        }
    }
    
    # Cleaning analysis
    cleaned_text = cleaner.clean_text(text_sample)
    cleaning_stats = cleaner.get_cleaning_stats(text_sample, cleaned_text)
    
    analysis['cleaning_analysis'] = {
        'cleaned_text': cleaned_text,
        'cleaning_stats': cleaning_stats,
        'effectiveness': 'High' if cleaning_stats.get('reduction_percentage', 0) > 30 else 
                        'Medium' if cleaning_stats.get('reduction_percentage', 0) > 10 else 'Low'
    }
    
    # Extraction analysis
    extraction_results = extractor.process_text(cleaned_text)
    extraction_stats = extractor.get_extraction_stats(cleaned_text)
    
    analysis['extraction_analysis'] = {
        'extracted_fields': {k: v for k, v in extraction_results.items() if v is not None},
        'extraction_stats': extraction_stats,
        'success_rate': len([v for v in extraction_results.values() if v is not None]) / len(extraction_results)
    }
    
    # Pattern analysis
    if show_patterns:
        common_patterns = extractor.extract_common_patterns(text_sample)
        analysis['pattern_analysis'] = common_patterns
    
    # Confidence analysis
    if show_confidence:
        confidence_scores = {}
        for field, value in extraction_results.items():
            if value is not None:
                # Estimate confidence based on pattern matches and validation
                pattern_matches = extraction_stats['field_matches'].get(field, 0)
                is_valid = extractor._validate_field_value(value, field) if hasattr(extractor, '_validate_field_value') else True
                
                confidence = min(1.0, (pattern_matches * 0.3) + (0.7 if is_valid else 0))
                confidence_scores[field] = round(confidence, 3)
        
        analysis['confidence_scores'] = confidence_scores
    
    return analysis

def get_field_recommendations(csv_path, obs_column='obs', sample_size=100):
    """
    Analyze CSV file to recommend field extraction improvements
    
    Args:
        csv_path (str): Path to CSV file
        obs_column (str): Column containing text
        sample_size (int): Number of rows to sample for analysis
        
    Returns:
        dict: Recommendations for improving extraction
    """
    import pandas as pd
    
    try:
        # Read sample of data
        df_sample = pd.read_csv(csv_path, nrows=sample_size)
        
        if obs_column not in df_sample.columns:
            return {'error': f"Column '{obs_column}' not found in CSV"}
        
        recommendations = {
            'field_potential': {},
            'cleaning_suggestions': [],
            'pattern_suggestions': [],
            'overall_assessment': ''
        }
        
        extractor = create_extractor()
        cleaner = create_cleaner()
        
        # Analyze each text sample
        extraction_counts = defaultdict(int)
        cleaning_effectiveness = []
        common_issues = defaultdict(int)
        
        for idx, row in df_sample.iterrows():
            if pd.notna(row[obs_column]):
                text = str(row[obs_column])
                
                # Cleaning analysis
                cleaned = cleaner.clean_text(text)
                stats = cleaner.get_cleaning_stats(text, cleaned)
                cleaning_effectiveness.append(stats.get('reduction_percentage', 0))
                
                # Extraction analysis
                extracted = extractor.process_text(cleaned)
                for field, value in extracted.items():
                    if value is not None:
                        extraction_counts[field] += 1
        
        # Generate field potential assessment
        total_samples = len(df_sample)
        for field in extractor.get_field_list():
            count = extraction_counts[field]
            percentage = (count / total_samples) * 100
            
            if percentage > 50:
                potential = 'High'
            elif percentage > 20:
                potential = 'Medium'
            elif percentage > 5:
                potential = 'Low'
            else:
                potential = 'Very Low'
            
            recommendations['field_potential'][field] = {
                'extraction_rate': round(percentage, 1),
                'potential': potential,
                'sample_count': count
            }
        
        # Cleaning suggestions
        avg_cleaning = np.mean(cleaning_effectiveness) if cleaning_effectiveness else 0
        if avg_cleaning < 10:
            recommendations['cleaning_suggestions'].append(
                "Consider reviewing noise patterns - cleaning effectiveness is low"
            )
        elif avg_cleaning > 50:
            recommendations['cleaning_suggestions'].append(
                "High cleaning effectiveness detected - good noise removal"
            )
        
        # Overall assessment
        high_potential_fields = sum(1 for field_info in recommendations['field_potential'].values() 
                                  if field_info['potential'] in ['High', 'Medium'])
        
        if high_potential_fields > 10:
            recommendations['overall_assessment'] = 'Excellent extraction potential'
        elif high_potential_fields > 5:
            recommendations['overall_assessment'] = 'Good extraction potential'
        elif high_potential_fields > 2:
            recommendations['overall_assessment'] = 'Fair extraction potential'
        else:
            recommendations['overall_assessment'] = 'Low extraction potential - consider improving text patterns'
        
        return recommendations
        
    except Exception as e:
        return {'error': f"Analysis failed: {str(e)}"}

# Package metadata with enhanced exports
__all__ = [
    # Core classes
    'TextExtractor',
    'TextCleaner', 
    'DataProcessor',
    'ExportHandler',
    
    # Factory functions
    'create_processor',
    'create_extractor',
    'create_cleaner',
    'create_exporter',
    
    # Information functions
    'get_version',
    'get_supported_fields',
    'get_fields_by_category',
    'get_field_priorities',
    'validate_field_name',
    'get_field_category',
    'get_processing_stats_template',
    
    # Enhanced convenience functions
    'quick_extract',
    'quick_clean',
    'quick_process',
    'analyze_text_sample',
    'get_field_recommendations',
    
    # Validation functions
    'validate_chunk_size',
    'validate_export_format',
    
    # Initialization
    'initialize_core',
    
    # Enhanced error classes
    'CoreProcessingError',
    'TextExtractionError',
    'TextCleaningError',
    'DataProcessingError',
    'ExportError',
    'ValidationError',
    
    # Constants
    'ALL_FIELDS',
    'NETWORK_FIELDS',
    'HARDWARE_FIELDS',
    'CONFIGURATION_FIELDS',
    'BUSINESS_FIELDS',
    'FIELD_PRIORITIES',
    'DEFAULT_CHUNK_SIZE',
    'SUPPORTED_FORMATS'
]

# Package configuration
PACKAGE_CONFIG = {
    'version': __version__,
    'enhanced_features': True,
    'default_chunk_size': DEFAULT_CHUNK_SIZE,
    'supported_formats': SUPPORTED_FORMATS,
    'total_fields': len(ALL_FIELDS),
    'field_categories': len(get_fields_by_category())
}