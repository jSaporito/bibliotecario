import os
import pandas as pd
from datetime import datetime
from werkzeug.utils import secure_filename

def generate_unique_filename(original_filename):
    """Generate unique filename with timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    secure_name = secure_filename(original_filename)
    name, ext = os.path.splitext(secure_name)
    return f"{timestamp}_{name}{ext}"

def validate_csv_file(filepath, required_column=None):
    """Validate CSV file"""
    try:
        sample_df = pd.read_csv(filepath, nrows=5)
        
        result = {
            'valid': True,
            'columns': list(sample_df.columns),
            'errors': []
        }
        
        if required_column and required_column not in sample_df.columns:
            result['valid'] = False
            result['errors'].append(f"Column '{required_column}' not found")
        
        return result
        
    except Exception as e:
        return {
            'valid': False,
            'errors': [str(e)]
        }

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB"]
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.1f} {size_names[i]}"

def cleanup_old_files(directory, max_age_hours=24):
    """Clean up old files"""
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
                    except:
                        pass
    except:
        pass
    
    return files_removed