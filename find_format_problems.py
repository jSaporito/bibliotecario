#!/usr/bin/env python3
"""
Script to find problematic format specifiers in Python files
"""
import re
import os
import sys

def find_format_problems(file_path):
    """Find potential format specifier problems in a Python file"""
    problems = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return problems
    
    for line_num, line in enumerate(lines, 1):
        line_stripped = line.strip()
        
        # Skip comments and empty lines
        if not line_stripped or line_stripped.startswith('#'):
            continue
        
        # Pattern 1: f-strings with format specifiers
        f_string_patterns = [
            r'f"[^"]*\{[^}]*:[^}]*\}[^"]*"',  # f"...{var:spec}..."
            r"f'[^']*\{[^}]*:[^}]*\}[^']*'",  # f'...{var:spec}...'
        ]
        
        for pattern in f_string_patterns:
            matches = re.finditer(pattern, line)
            for match in matches:
                f_string = match.group()
                
                # Check for problematic patterns
                format_specs = re.findall(r'\{[^}]*:[^}]*\}', f_string)
                for spec in format_specs:
                    spec_content = spec[1:-1]  # Remove { }
                    if ':' in spec_content:
                        var_name, format_part = spec_content.split(':', 1)
                        
                        # Common problems:
                        if ' .' in format_part or ', .' in format_part:
                            problems.append({
                                'file': file_path,
                                'line': line_num,
                                'text': line.rstrip(),
                                'problem': f'Space before decimal in format spec: {spec}',
                                'type': 'space_before_decimal'
                            })
                        
                        if format_part.count(':') > 0:
                            problems.append({
                                'file': file_path,
                                'line': line_num,
                                'text': line.rstrip(),
                                'problem': f'Double colon in format spec: {spec}',
                                'type': 'double_colon'
                            })
                        
                        if re.search(r'f\.\d+', format_part):
                            problems.append({
                                'file': file_path,
                                'line': line_num,
                                'text': line.rstrip(),
                                'problem': f'Wrong format order (f.X instead of .Xf): {spec}',
                                'type': 'wrong_order'
                            })
                        
                        if format_part.endswith('f.') or format_part.endswith('d.'):
                            problems.append({
                                'file': file_path,
                                'line': line_num,
                                'text': line.rstrip(),
                                'problem': f'Format spec ends with dot: {spec}',
                                'type': 'trailing_dot'
                            })
        
        # Pattern 2: .format() calls
        if '.format(' in line:
            # Look for format strings with problematic patterns
            format_matches = re.finditer(r'["\'][^"\']*\{[^}]*:[^}]*\}[^"\']*["\']\.format\(', line)
            for match in format_matches:
                format_call = match.group()
                format_specs = re.findall(r'\{[^}]*:[^}]*\}', format_call)
                for spec in format_specs:
                    spec_content = spec[1:-1]
                    if ':' in spec_content:
                        parts = spec_content.split(':', 1)
                        if len(parts) > 1:
                            format_part = parts[1]
                            if ' .' in format_part or ', .' in format_part:
                                problems.append({
                                    'file': file_path,
                                    'line': line_num,
                                    'text': line.rstrip(),
                                    'problem': f'Space before decimal in .format(): {spec}',
                                    'type': 'format_space_decimal'
                                })
        
        # Pattern 3: % formatting
        if ' % ' in line and (':%' in line or '%.%' in line):
            problems.append({
                'file': file_path,
                'line': line_num,
                'text': line.rstrip(),
                'problem': 'Potentially problematic % formatting',
                'type': 'percent_formatting'
            })
    
    return problems

def scan_directory(directory):
    """Scan all Python files in directory for format problems"""
    all_problems = []
    
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                problems = find_format_problems(file_path)
                all_problems.extend(problems)
    
    return all_problems

def main():
    if len(sys.argv) > 1:
        target = sys.argv[1]
    else:
        target = 'core'  # Default to core directory
    
    print(f"🔍 Scanning {target} for format specifier problems...")
    
    if os.path.isfile(target):
        problems = find_format_problems(target)
    else:
        problems = scan_directory(target)
    
    if problems:
        print(f"\n🚨 Found {len(problems)} potential format specifier problems:")
        print("=" * 80)
        
        for i, problem in enumerate(problems, 1):
            print(f"\n{i}. {problem['type'].upper()}")
            print(f"   File: {problem['file']}:{problem['line']}")
            print(f"   Problem: {problem['problem']}")
            print(f"   Code: {problem['text']}")
            print("-" * 60)
    else:
        print("✅ No obvious format specifier problems found")
        print("The error might be in:")
        print("  - Dynamic format strings")
        print("  - Complex pandas operations")
        print("  - Third-party library calls")

if __name__ == '__main__':
    main()