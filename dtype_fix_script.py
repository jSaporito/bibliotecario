#!/usr/bin/env python3
"""
Script to find and analyze dtype usage issues
"""

import re
import os

def analyze_dtype_usage():
    """Analyze dtype usage in the routes.py file"""
    
    routes_file = "app/routes.py"
    
    if not os.path.exists(routes_file):
        print("❌ routes.py not found")
        return
    
    print("🔍 Analyzing dtype usage in routes.py...")
    
    with open(routes_file, 'r') as f:
        lines = f.readlines()
    
    # Look for problematic patterns
    problematic_lines = []
    
    for i, line in enumerate(lines, 1):
        # Check for direct DataFrame.dtype usage (problematic)
        if re.search(r'\b\w+\.dtype\b', line) and 'df[' not in line and 'dataframe[' not in line:
            # This might be calling .dtype on a DataFrame object directly
            problematic_lines.append((i, line.strip(), "Possible DataFrame.dtype usage"))
        
        # Look for specific error patterns
        if '.dtype' in line:
            print(f"Line {i}: {line.strip()}")
            
            # Check the context - what variable is calling .dtype?
            dtype_match = re.search(r'(\w+)\.dtype', line)
            if dtype_match:
                var_name = dtype_match.group(1)
                print(f"   Variable calling .dtype: '{var_name}'")
                
                # Look backwards to see how this variable was defined
                for j in range(max(0, i-10), i):
                    if var_name in lines[j] and ('=' in lines[j] or 'pd.DataFrame' in lines[j]):
                        print(f"   Definition context (line {j+1}): {lines[j].strip()}")
    
    if problematic_lines:
        print("\n❌ PROBLEMATIC LINES FOUND:")
        for line_num, line_content, issue in problematic_lines:
            print(f"   Line {line_num}: {line_content}")
            print(f"   Issue: {issue}")
    else:
        print("\n✅ No obvious DataFrame.dtype issues found")
        print("The error might be more subtle - check the context around these lines")

def suggest_fixes():
    """Suggest fixes for common dtype issues"""
    
    print("\n🔧 COMMON FIXES:")
    print("="*50)
    
    fixes = [
        ("❌ df.dtype", "✅ df.dtypes", "For all column types"),
        ("❌ result.dtype", "✅ result.dtypes", "If result is a DataFrame"),
        ("❌ data.dtype", "✅ data.dtypes", "If data is a DataFrame"),
        ("✅ df['col'].dtype", "✅ df['col'].dtype", "This is correct for single columns"),
        ("✅ series.dtype", "✅ series.dtype", "This is correct for Series objects"),
    ]
    
    for wrong, right, explanation in fixes:
        print(f"{wrong:<20} → {right:<20} {explanation}")

def create_safe_dtype_function():
    """Create a safe dtype access function"""
    
    safe_function = '''
def safe_get_dtype(obj, column_name=None):
    """Safely get dtype information"""
    try:
        if column_name is not None:
            # Getting dtype for specific column
            if hasattr(obj, 'columns') and column_name in obj.columns:
                return obj[column_name].dtype
            else:
                print(f"⚠️  Column '{column_name}' not found")
                return None
        else:
            # Getting dtype info for the object
            if hasattr(obj, 'dtypes'):  # DataFrame
                return obj.dtypes
            elif hasattr(obj, 'dtype'):  # Series
                return obj.dtype
            else:
                print(f"⚠️  Object type {type(obj)} doesn't have dtype")
                return None
    except Exception as e:
        print(f"❌ Error getting dtype: {e}")
        return None

# Usage examples:
# safe_get_dtype(df)  # For DataFrame - gets all dtypes
# safe_get_dtype(df, 'column_name')  # For specific column
# safe_get_dtype(series)  # For Series - gets dtype
'''
    
    print("\n🛡️  SAFE DTYPE FUNCTION:")
    print("="*50)
    print(safe_function)

if __name__ == "__main__":
    analyze_dtype_usage()
    suggest_fixes() 
    create_safe_dtype_function()