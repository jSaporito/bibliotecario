#!/usr/bin/env python3
"""
Comprehensive debugging script to identify format specifier issues
"""

import traceback
import re
import sys
import pandas as pd
from collections import defaultdict

def debug_format_specifier_issue():
    """Debug format specifier issues by testing common problematic patterns"""
    
    print("🔍 DEBUGGING FORMAT SPECIFIER ISSUES")
    print("=" * 60)
    
    # Test 1: Find the DataFrame dtype issue
    print("\n📊 Testing DataFrame dtype issue...")
    try:
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        
        # This will cause the error you're seeing
        print("Testing df.dtype (this should fail):")
        try:
            print(df.dtype)  # This should fail
        except AttributeError as e:
            print(f"❌ Error: {e}")
            print("✅ Solution: Use df.dtypes (plural) instead")
            print(f"✅ Correct: {df.dtypes}")
        
    except Exception as e:
        print(f"❌ DataFrame test failed: {e}")
    
    # Test 2: Common format specifier problems
    print("\n🔤 Testing format specifier patterns...")
    
    format_tests = [
        # Valid patterns
        ("f'Value: {42:.2f}'", lambda: f"Value: {42:.2f}"),
        ("'Value: {:.2f}'.format(42)", lambda: "Value: {:.2f}".format(42)),
        ("'Value: %.2f' % 42", lambda: "Value: %.2f" % 42),
        
        # Invalid patterns that cause errors
        ("f'Value: {42:.2f:.02}' (double format)", lambda: f"Value: {42:.2f:.02}"),
        ("'Value: {:.2f:.02}'.format(42)", lambda: "Value: {:.2f:.02}".format(42)),
        ("f'Value: {42:f.2}' (wrong order)", lambda: f"Value: {42:f.2}"),
        ("'Value: {:f.2}'.format(42)", lambda: "Value: {:f.2}".format(42)),
    ]
    
    for desc, test_func in format_tests:
        try:
            result = test_func()
            print(f"✅ {desc}: {result}")
        except ValueError as e:
            if "Invalid format specifier" in str(e):
                print(f"❌ {desc}: INVALID FORMAT SPECIFIER - {str(e)}")
            else:
                print(f"❓ {desc}: Other ValueError - {str(e)}")
        except Exception as e:
            print(f"❓ {desc}: Unexpected error - {str(e)}")
    
    # Test 3: Regex patterns that might cause issues
    print("\n🔍 Testing regex patterns in f-strings...")
    
    regex_tests = [
        # Safe patterns
        ("Simple regex", r'test[:\s]*(\d+)', "test: 123"),
        ("Escaped braces", r'test\{\d+\}', "test{123}"),
        
        # Potentially problematic patterns
        ("Unescaped braces", r'test{1,3}', "test123"),
        ("Complex character class", r'[:\s=\->\|]*', ":->|"),
    ]
    
    for desc, pattern, test_string in regex_tests:
        try:
            # Test the pattern itself
            matches = re.findall(pattern, test_string)
            print(f"✅ {desc}: Pattern works, found {len(matches)} matches")
            
            # Test in f-string context
            result = f"Pattern: {pattern} found {len(matches)} matches"
            print(f"✅ {desc}: F-string works: {result[:50]}...")
            
        except Exception as e:
            print(f"❌ {desc}: Error - {str(e)}")

def trace_format_specifier_error():
    """Set up error tracing to catch format specifier issues"""
    
    print("\n🔍 SETTING UP ERROR TRACING")
    print("=" * 60)
    
    # Override the default excepthook to catch format errors
    original_excepthook = sys.excepthook
    
    def debug_excepthook(exc_type, exc_value, exc_traceback):
        if exc_type == ValueError and "Invalid format specifier" in str(exc_value):
            print("\n🚨 CAUGHT INVALID FORMAT SPECIFIER ERROR!")
            print(f"Error: {exc_value}")
            print("\nFull traceback:")
            traceback.print_exception(exc_type, exc_value, exc_traceback)
            
            # Print local variables from the error frame
            if exc_traceback:
                frame = exc_traceback.tb_frame
                print(f"\nLocal variables in error frame:")
                for var_name, var_value in frame.f_locals.items():
                    try:
                        print(f"  {var_name}: {repr(var_value)[:100]}")
                    except:
                        print(f"  {var_name}: <unable to display>")
        else:
            original_excepthook(exc_type, exc_value, exc_traceback)
    
    sys.excepthook = debug_excepthook
    print("✅ Error tracing enabled")

def find_dtype_issues_in_code():
    """Look for common DataFrame.dtype issues"""
    
    print("\n🔍 CHECKING FOR DATAFRAME DTYPE ISSUES")
    print("=" * 60)
    
    # Common problematic patterns
    problematic_patterns = [
        r'\.dtype\s*(?!=)',  # .dtype not followed by =
        r'df\.dtype',
        r'dataframe\.dtype',
        r'data\.dtype',
    ]
    
    print("Common problematic patterns to look for in your code:")
    for i, pattern in enumerate(problematic_patterns, 1):
        print(f"{i}. {pattern}")
    
    print("\n💡 SOLUTIONS:")
    print("❌ Wrong: df.dtype")
    print("✅ Right: df.dtypes (for all columns)")
    print("✅ Right: df['column_name'].dtype (for specific column)")
    print("✅ Right: df.select_dtypes(include=[...]) (for filtering)")

def create_debug_wrapper():
    """Create a wrapper function to debug format string operations"""
    
    print("\n🔧 DEBUG WRAPPER CODE")
    print("=" * 60)
    
    wrapper_code = '''
def debug_format_operation(operation_name, func, *args, **kwargs):
    """Wrapper to debug format operations"""
    try:
        print(f"🔍 Executing {operation_name}")
        print(f"   Args: {args}")
        print(f"   Kwargs: {kwargs}")
        result = func(*args, **kwargs)
        print(f"✅ {operation_name} succeeded")
        return result
    except ValueError as e:
        if "Invalid format specifier" in str(e):
            print(f"❌ FORMAT SPECIFIER ERROR in {operation_name}")
            print(f"   Error: {e}")
            print(f"   Args: {args}")
            print(f"   Kwargs: {kwargs}")
            # Print the actual format string if available
            if args:
                print(f"   First argument (likely format string): {repr(args[0])}")
            raise
        else:
            raise
    except Exception as e:
        print(f"❌ OTHER ERROR in {operation_name}: {e}")
        raise

# Example usage:
# Instead of: f"Value: {num:.2f}"
# Use: debug_format_operation("f-string", lambda: f"Value: {num:.2f}")

# Instead of: "Value: {:.2f}".format(num)
# Use: debug_format_operation("format", str.format, "Value: {:.2f}", num)
'''
    
    print(wrapper_code)

def main():
    """Main debugging function"""
    
    print("🚀 COMPREHENSIVE FORMAT SPECIFIER DEBUGGER")
    print("=" * 80)
    
    # Set up error tracing
    trace_format_specifier_error()
    
    # Run all debug tests
    debug_format_specifier_issue()
    find_dtype_issues_in_code()
    create_debug_wrapper()
    
    print("\n" + "=" * 80)
    print("🎯 DEBUGGING COMPLETE")
    print("\n📋 NEXT STEPS:")
    print("1. Look for '.dtype' in your code and change to '.dtypes'")
    print("2. Search for format strings with double specifiers like ':.2f:.02'")
    print("3. Add the debug wrapper to your problematic functions")
    print("4. Run your Flask app with error tracing enabled")
    
    print("\n🔍 TO FIND THE EXACT ERROR:")
    print("Add this to your Flask app startup:")
    print("```python")
    print("import sys")
    print("def debug_excepthook(exc_type, exc_value, exc_traceback):")
    print("    if 'Invalid format specifier' in str(exc_value):")
    print("        print(f'FORMAT ERROR: {exc_value}')")
    print("        traceback.print_exception(exc_type, exc_value, exc_traceback)")
    print("    sys.__excepthook__(exc_type, exc_value, exc_traceback)")
    print("sys.excepthook = debug_excepthook")
    print("```")

if __name__ == "__main__":
    main()