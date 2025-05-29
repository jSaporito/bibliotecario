import os
import uuid
import pandas as pd
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from flask import current_app

def allowed_file(filename):
    """Check if uploaded file has allowed extension"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in current_app.config['ALLOWED_EXTENSIONS']

def generate_unique_filename(original_filename):
    """Generate unique filename with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    secure_name = secure_filename(original_filename)
    name, ext = os.path.splitext(secure_name)
    return f"{timestamp}_{name}{ext}"

def get_file_info(filepath):
    """Get basic information about a file"""
    if not os.path.exists(filepath):
        return None
    
    file_stats = os.stat(filepath)
    size_mb = file_stats.st_size / (1024 * 1024)
    modified_time = datetime.fromtimestamp(file_stats.st_mtime)
    
    return {
        'size_mb': round(size_mb, 2),
        'size_bytes': file_stats.st_size,
        'modified': modified_time,
        'exists': True
    }

def validate_csv_file(filepath, required_column=None):
    """Validate CSV file and check for required columns"""
    try:
        # Try to read first few rows
        sample_df = pd.read_csv(filepath, nrows=5)
        
        validation_result = {
            'valid': True,
            'columns': list(sample_df.columns),
            'sample_rows': len(sample_df),
            'has_required_column': True,
            'errors': []
        }
        
        # Check for required column
        if required_column and required_column not in sample_df.columns:
            validation_result['valid'] = False
            validation_result['has_required_column'] = False
            validation_result['errors'].append(f"Required column '{required_column}' not found")
        
        return validation_result
        
    except pd.errors.EmptyDataError:
        return {
            'valid': False,
            'errors': ['CSV file is empty']
        }
    except pd.errors.ParserError as e:
        return {
            'valid': False,
            'errors': [f'CSV parsing error: {str(e)}']
        }
    except Exception as e:
        return {
            'valid': False,
            'errors': [f'File validation error: {str(e)}']
        }

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"

def format_duration(seconds):
    """Format duration in human readable format"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"

def format_number(number):
    """Format number with thousands separator"""
    if isinstance(number, (int, float)):
        return f"{number:,}"
    return str(number)

def calculate_processing_eta(processed_rows, total_rows, elapsed_time):
    """Calculate estimated time of arrival for processing completion"""
    if processed_rows == 0 or elapsed_time == 0:
        return None
    
    rows_per_second = processed_rows / elapsed_time
    remaining_rows = total_rows - processed_rows
    
    if rows_per_second > 0:
        eta_seconds = remaining_rows / rows_per_second
        return eta_seconds
    
    return None

def get_processing_progress(processed_rows, total_rows):
    """Calculate processing progress percentage"""
    if total_rows == 0:
        return 0
    
    progress = (processed_rows / total_rows) * 100
    return min(100, max(0, progress))

def sanitize_session_data(session_data):
    """Sanitize session data for safe storage"""
    safe_data = {}
    
    for key, value in session_data.items():
        if key in ['session_id', 'filename', 'obs_column', 'chunk_size', 
                  'enable_cleaning', 'export_formats', 'status', 'progress', 
                  'message']:
            safe_data[key] = value
        elif key in ['start_time', 'end_time'] and isinstance(value, datetime):
            safe_data[key] = value.isoformat()
    
    return safe_data

def cleanup_old_files(directory, max_age_hours=24):
    """Clean up old files from a directory"""
    if not os.path.exists(directory):
        return 0
    
    current_time = datetime.now()
    files_removed = 0
    
    try:
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
    except Exception:
        pass  # Ignore directory access errors
    
    return files_removed

def get_csv_preview(filepath, rows=5):
    """Get a preview of CSV data"""
    try:
        df = pd.read_csv(filepath, nrows=rows)
        
        preview_data = {
            'columns': list(df.columns),
            'rows': df.to_dict('records'),
            'total_columns': len(df.columns),
            'sample_rows': len(df)
        }
        
        return preview_data
        
    except Exception as e:
        return {
            'error': str(e),
            'columns': [],
            'rows': [],
            'total_columns': 0,
            'sample_rows': 0
        }

def estimate_processing_time(file_size_mb, chunk_size=5000):
    """Estimate processing time based on file size"""
    # Rough estimation based on file size and chunk size
    estimated_rows = file_size_mb * 1000  # Rough estimate
    estimated_chunks = estimated_rows / chunk_size
    
    # Estimated time per chunk (in seconds)
    time_per_chunk = 2.0  # Conservative estimate
    
    total_time = estimated_chunks * time_per_chunk
    return max(30, total_time)  # Minimum 30 seconds

def validate_processing_config(config):
    """Validate processing configuration"""
    errors = []
    
    # Check chunk size
    chunk_size = config.get('chunk_size', 5000)
    if not isinstance(chunk_size, int) or chunk_size < 100 or chunk_size > 10000:
        errors.append("Chunk size must be between 100 and 10,000")
    
    # Check obs column
    obs_column = config.get('obs_column', '')
    if not obs_column or not isinstance(obs_column, str):
        errors.append("Text column name is required")
    
    # Check export formats
    export_formats = config.get('export_formats', 'both')
    valid_formats = ['csv', 'excel', 'both', 'all']
    if export_formats not in valid_formats:
        errors.append(f"Export format must be one of: {', '.join(valid_formats)}")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors
    }

def create_download_link(session_id, file_format, filename):
    """Create a download link for processed files"""
    return {
        'url': f'/download/{session_id}/{file_format.lower()}',
        'format': file_format.upper(),
        'filename': filename,
        'display_name': f"Download {file_format.upper()}"
    }

def get_field_display_names():
    """Get human-readable display names for extracted fields"""
    return {
        'technology_id': 'Technology ID',
        'provider_id': 'Provider ID',
        'pop': 'POP',
        'gateway': 'Gateway',
        'interface_1': 'Interface 1',
        'interface_2': 'Interface 2',
        'access_point': 'Access Point',
        'cpe': 'CPE/Equipment',
        'ip_management': 'IP Management',
        'ip_telephony': 'IP Telephony',
        'ip_block': 'IP Block',
        'vlan': 'VLAN',
        'login_pppoe': 'PPPoE Login',
        'asn': 'ASN',
        'prefixes': 'Prefixes',
        'pon_port': 'PON Port',
        'onu_id': 'ONU ID',
        'model_onu': 'ONU Model',
        'slot': 'Slot',
        'serial': 'Serial Number',
        'partnerid': 'Partner ID',
        'circuitpartner': 'Circuit Partner',
        'serial_code': 'Serial Code',
        'mac': 'MAC Address',
        'wifi_ssid': 'WiFi SSID',
        'wifi_passcode': 'WiFi Password',
        'pop_description': 'POP Description'
    }

def generate_session_id():
    """Generate a unique session ID"""
    return str(uuid.uuid4())

def is_valid_session_id(session_id):
    """Validate session ID format"""
    try:
        uuid.UUID(session_id)
        return True
    except ValueError:
        return False

def get_upload_path(filename):
    """Get full upload path for a filename"""
    return os.path.join(current_app.config['UPLOAD_FOLDER'], filename)

def get_download_path(filename):
    """Get full download path for a filename"""
    return os.path.join(current_app.config['DOWNLOAD_FOLDER'], filename)

def log_processing_event(session_id, event, details=None):
    """Log processing events for debugging"""
    timestamp = datetime.now().isoformat()
    log_entry = f"[{timestamp}] Session {session_id[:8]}: {event}"
    
    if details:
        log_entry += f" - {details}"
    
    # You can extend this to write to actual log files
    print(log_entry)
    
    return log_entry