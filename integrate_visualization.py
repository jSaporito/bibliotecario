
import os
import shutil
from datetime import datetime

def backup_files():
    """Create backup of existing files"""
    backup_dir = f"backup_before_visualization_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    files_to_backup = [
        'app/routes.py',
        'templates/results.html',
        'requirements.txt'
    ]
    
    print(f"📁 Creating backup directory: {backup_dir}")
    os.makedirs(backup_dir, exist_ok=True)
    
    for file_path in files_to_backup:
        if os.path.exists(file_path):
            backup_file = os.path.join(backup_dir, file_path.replace('/', '_'))
            shutil.copy2(file_path, backup_file)
            print(f"✅ Backed up: {file_path}")
    
    return backup_dir

def add_visualization_route():
    """Add visualization route to existing routes.py"""
    routes_path = 'app/routes.py'
    
    if not os.path.exists(routes_path):
        print(f"❌ Routes file not found: {routes_path}")
        return False
    
    # Read existing routes
    with open(routes_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if visualization route already exists
    if 'data_visualization' in content:
        print("ℹ️  Visualization route already exists in routes.py")
        return True
    
    # Add the new route
    new_route = '''
@bp.route('/data-visualization/<session_id>')
def data_visualization(session_id):
    """Generate and display data visualization analysis"""
    if session_id not in processing_status:
        flash('Sessão não encontrada. Execute o processamento primeiro.', 'error')
        return redirect(url_for('main.upload'))
    
    try:
        config = processing_status[session_id]
        
        # Check if processing is completed
        if config.get('status') != 'completed':
            flash('Processamento ainda não foi concluído.', 'warning')
            return redirect(url_for('main.processing', session_id=session_id))
        
        # Check if we have the necessary data
        if 'results' not in config or 'dataframe' not in config['results']:
            flash('Dados processados não encontrados.', 'error')
            return redirect(url_for('main.results', session_id=session_id))
        
        # Load original and processed data
        original_df = load_original_dataframe(config['file_path'], config['obs_column'])
        processed_df = config['results']['dataframe']
        
        # Generate visualization report
        try:
            from core.data_visualizer import create_data_visualization_report
        except ImportError:
            flash('Módulo de visualização não encontrado. Instale as dependências necessárias.', 'error')
            return redirect(url_for('main.results', session_id=session_id))
        
        print(f"🎨 Generating visualization report for session {session_id}")
        visualization_report = create_data_visualization_report(
            original_df, 
            processed_df, 
            config['obs_column']
        )
        
        return render_template('data_visualization.html', 
                             report=visualization_report,
                             session_id=session_id)
        
    except Exception as e:
        flash(f'Erro ao gerar visualizações: {str(e)}', 'error')
        print(f"❌ Visualization error: {str(e)}")
        return redirect(url_for('main.results', session_id=session_id))

def load_original_dataframe(file_path, obs_column):
    """Load original dataframe for comparison"""
    try:
        import pandas as pd
        
        # Try multiple encodings
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                print(f"✅ Loaded original CSV with {encoding} encoding")
                return df
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        # If all encodings fail, try with python engine
        df = pd.read_csv(file_path, encoding='utf-8', engine='python', low_memory=False)
        print("✅ Loaded original CSV with python engine")
        return df
        
    except Exception as e:
        print(f"❌ Error loading original dataframe: {str(e)}")
        # Return empty dataframe as fallback
        return pd.DataFrame()
'''
    
    # Add the new route before the last few lines of the file
    # Find a good insertion point
    if 'if __name__ ==' in content:
        insertion_point = content.rfind('if __name__ ==')
    else:
        insertion_point = len(content)
    
    # Insert the new route
    updated_content = content[:insertion_point] + new_route + '\\n\\n' + content[insertion_point:]
    
    # Write back to file
    with open(routes_path, 'w', encoding='utf-8') as f:
        f.write(updated_content)
    
    print("✅ Added visualization route to routes.py")
    return True

def update_results_template():
    """Update results.html template to include visualization link"""
    results_path = 'templates/results.html'
    
    if not os.path.exists(results_path):
        print(f"❌ Results template not found: {results_path}")
        return False
    
    with open(results_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if visualization button already exists
    if 'data_visualization' in content:
        print("ℹ️  Visualization button already exists in results.html")
        return True
    
    # Find the actions section and add the visualization button
    visualization_button = '''                <a href="{{ url_for('main.data_visualization', session_id=session_id) }}" 
                   class="btn btn-success">
                    <i class="fas fa-chart-bar me-2"></i>
                    Ver Análise Visual
                </a>'''
    
    # Look for the "Process Another File" button to insert after it
    if 'Process Another File' in content or 'Processar Outro Arquivo' in content:
        # Find the button and add our visualization button after it
        import re
        pattern = r'(<a[^>]*class="btn btn-primary"[^>]*>[^<]*(?:Process Another File|Processar Outro Arquivo)[^<]*</a>)'
        match = re.search(pattern, content)
        
        if match:
            original_button = match.group(1)
            replacement = original_button + '\\n                ' + visualization_button
            content = content.replace(original_button, replacement)
        else:
            # Fallback: add at the end of the actions section
            actions_pattern = r'(</div>\\s*</div>\\s*</div>\\s*{% endblock %})'
            content = re.sub(actions_pattern, 
                           visualization_button + '\\n            \\1', 
                           content)
    
    # Write back to file
    with open(results_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print("✅ Added visualization button to results.html")
    return True

def update_requirements():
    """Update requirements.txt with visualization dependencies"""
    requirements_path = 'requirements.txt'
    
    new_requirements = [
        'matplotlib>=3.5.0',
        'seaborn>=0.11.0',
        'plotly>=5.0.0',
        'numpy>=1.21.0'
    ]
    
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r') as f:
            existing_requirements = f.read()
    else:
        existing_requirements = ""
    
    # Add new requirements if they don't exist
    updated_requirements = existing_requirements
    
    for req in new_requirements:
        package_name = req.split('>=')[0]
        if package_name not in existing_requirements:
            updated_requirements += f"\\n{req}"
            print(f"➕ Added requirement: {req}")
    
    # Write updated requirements
    with open(requirements_path, 'w') as f:
        f.write(updated_requirements.strip())
    
    print("✅ Updated requirements.txt")

def update_process_file_function():
    """Update the process_file_simple function to store dataframe"""
    routes_path = 'app/routes.py'
    
    with open(routes_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if dataframe is already being stored
    if "'dataframe': results['dataframe']" in content:
        print("ℹ️  Dataframe storage already exists in process_file_simple")
        return True
    
    # Find the results storage section and update it
    old_pattern = r"'results': {\\s*'stats': results\\['stats'\\],\\s*'download_info': exporter\\.create_download_info\\(export_results\\)\\s*}"
    new_pattern = """'results': {
                    'stats': results['stats'],
                    'dataframe': results['dataframe'],  # Store for visualization
                    'download_info': exporter.create_download_info(export_results)
                }"""
    
    import re
    if re.search(old_pattern, content):
        content = re.sub(old_pattern, new_pattern, content)
        
        with open(routes_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print("✅ Updated process_file_simple to store dataframe")
        return True
    else:
        print("⚠️  Could not automatically update process_file_simple function")
        print("   Please manually add 'dataframe': results['dataframe'] to the results dictionary")
        return False

def main():
    """Main integration function"""
    print("🚀 BIBLIOTECARIO VISUALIZATION INTEGRATION")
    print("=" * 50)
    
    # Check if core files exist
    required_files = ['app/routes.py', 'templates/results.html']
    missing_files = [f for f in required_files if not os.path.exists(f)]
    
    if missing_files:
        print(f"❌ Missing required files: {', '.join(missing_files)}")
        print("   Please run this script from your project root directory")
        return False
    
    # Create backup
    backup_dir = backup_files()
    print(f"📁 Backup created: {backup_dir}")
    
    try:
        # Integration steps
        steps = [
            ("Adding visualization route", add_visualization_route),
            ("Updating results template", update_results_template),
            ("Updating requirements", update_requirements),
            ("Updating process function", update_process_file_function)
        ]
        
        success_count = 0
        for step_name, step_function in steps:
            print(f"\\n🔄 {step_name}...")
            if step_function():
                success_count += 1
            else:
                print(f"❌ Failed: {step_name}")
        
        print(f"\\n📊 Integration Summary:")
        print(f"   ✅ Successful steps: {success_count}/{len(steps)}")
        print(f"   📁 Backup location: {backup_dir}")
        
        if success_count == len(steps):
            print("\\n🎉 Integration completed successfully!")
            print("\\nNext steps:")
            print("1. Install new requirements: pip install -r requirements.txt")
            print("2. Make sure core/data_visualizer.py is in place")
            print("3. Make sure templates/data_visualization.html is in place")
            print("4. Restart your Flask application")
            print("5. Process a CSV file and look for 'Ver Análise Visual' button in results")
        else:
            print("\\n⚠️  Integration partially completed. Please check the warnings above.")
        
        return success_count == len(steps)
        
    except Exception as e:
        print(f"\\n❌ Integration failed: {str(e)}")
        print(f"   Your original files are backed up in: {backup_dir}")
        return False

if __name__ == "__main__":
    main()