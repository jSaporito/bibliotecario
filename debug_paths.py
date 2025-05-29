import os

print("Current working directory:", os.getcwd())
print("\nProject structure:")
for root, dirs, files in os.walk('.'):
    level = root.replace('.', '').count(os.sep)
    indent = ' ' * 2 * level  
    print(f"{indent}{os.path.basename(root)}/")
    subindent = ' ' * 2 * (level + 1)
    for file in files:
        if file.endswith(('.html', '.py')):
            print(f"{subindent}{file}")

print(f"\nTemplates folder exists: {os.path.exists('templates')}")
if os.path.exists('templates'):
    print("HTML files in templates:")
    for file in os.listdir('templates'):
        if file.endswith('.html'):
            print(f"  - {file}")