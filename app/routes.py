from flask import Blueprint, render_template, request, jsonify, send_file, flash, redirect, url_for, current_app
from werkzeug.utils import secure_filename
import os
import uuid
from datetime import datetime
import threading
import traceback
import pandas as pd

# Import core modules
from app.forms import UploadForm
from core.data_processor import GroupBasedDataProcessor
from core.export_handler import EnhancedExportHandler
from core.data_visualizer import GroupBasedDataVisualizer
from core.product_groups import product_group_manager

bp = Blueprint('main', __name__)
processing_status = {}

@bp.route('/')
def index():
    """Home page"""
    return render_template('index.html')

@bp.route('/upload', methods=['GET', 'POST'])
def upload():
    """File upload and processing"""
    form = UploadForm()
    
    if form.validate_on_submit():
        # Generate session ID
        session_id = str(uuid.uuid4())
        
        # Save uploaded file
        file = form.file.data
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{timestamp}_{filename}")
        file.save(upload_path)
        
        # Store processing config
        processing_status[session_id] = {
            'file_path': upload_path,
            'chunk_size': form.chunk_size.data,
            'export_formats': form.export_formats.data,
            'status': 'processing',
            'progress': 0,
            'message': 'Iniciando processamento...',
        }
        
        # Start background processing
        thread = threading.Thread(target=process_file, args=(current_app._get_current_object(), session_id))
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('main.processing', session_id=session_id))
    
    return render_template('upload.html', form=form)

@bp.route('/quick-analysis', methods=['POST'])
def quick_analysis():
    """Quick analysis of sample data"""
    form = UploadForm()
    
    if form.validate_on_submit():
        try:
            # Save file temporarily
            file = form.file.data
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"temp_{timestamp}_{filename}")
            file.save(temp_path)
            
            # Analyze sample
            analysis = analyze_sample(temp_path)
            
            # Clean up temp file
            try:
                os.remove(temp_path)
            except:
                pass
            
            if 'error' in analysis:
                flash(f'Análise falhou: {analysis["error"]}', 'error')
                return redirect(url_for('main.upload'))
            
            return render_template('analysis_preview.html', 
                                 analysis=analysis, 
                                 form=form,
                                 filename=filename)
            
        except Exception as e:
            flash(f'Análise falhou: {str(e)}', 'error')
            return redirect(url_for('main.upload'))
    
    return render_template('upload.html', form=form)

@bp.route('/processing/<session_id>')
def processing(session_id):
    """Processing status page"""
    if session_id not in processing_status:
        flash('Sessão inválida', 'error')
        return redirect(url_for('main.index'))
    return render_template('processing.html', session_id=session_id)

@bp.route('/api/status/<session_id>')
def get_status(session_id):
    """Get processing status"""
    if session_id not in processing_status:
        return jsonify({'error': 'Sessão não encontrada'}), 404
    
    status = processing_status[session_id]
    return jsonify({
        'status': status['status'],
        'progress': status['progress'],
        'message': status['message']
    })

@bp.route('/results/<session_id>')
def results(session_id):
    """Results page"""
    if session_id not in processing_status:
        return redirect(url_for('main.index'))
    
    status = processing_status[session_id]
    if status['status'] != 'completed':
        return redirect(url_for('main.processing', session_id=session_id))
    
    return render_template('results.html', 
                         session_id=session_id, 
                         results=status.get('results'),
                         has_product_groups=status.get('results', {}).get('has_product_groups', False))

@bp.route('/extraction-analysis/<session_id>')
def extraction_analysis(session_id):
    """Generate extraction analysis charts"""
    if session_id not in processing_status:
        flash('Sessão não encontrada.', 'error')
        return redirect(url_for('main.upload'))
    
    try:
        config = processing_status[session_id]
        
        if config.get('status') != 'completed':
            flash('Processamento não foi concluído.', 'warning')
            return redirect(url_for('main.processing', session_id=session_id))
        
        if 'results' not in config or 'dataframe' not in config['results']:
            flash('Dados processados não encontrados.', 'error')
            return redirect(url_for('main.results', session_id=session_id))
        
        # Generate extraction analysis
        processed_df = config['results']['dataframe']
        visualizer = GroupBasedDataVisualizer(product_group_manager)
        report = visualizer.generate_extraction_report(processed_df, 'product_group')
        
        if 'error' in report:
            flash(f'Erro ao gerar análise: {report["error"]}', 'error')
            return redirect(url_for('main.results', session_id=session_id))
        
        return render_template('extraction_analysis.html', 
                             report=report,
                             session_id=session_id)
        
    except Exception as e:
        flash(f'Erro ao gerar análise de extração: {str(e)}', 'error')
        return redirect(url_for('main.results', session_id=session_id))

@bp.route('/download/<session_id>/<format>')
def download(session_id, format):
    """Download processed files"""
    if session_id not in processing_status:
        return jsonify({'error': 'Sessão não encontrada'}), 404
    
    status = processing_status[session_id]
    if status['status'] != 'completed':
        return jsonify({'error': 'Processamento não concluído'}), 400
    
    # Find and serve the requested file
    if 'results' in status and 'download_info' in status['results']:
        for file_info in status['results']['download_info']['files']:
            if file_info['format'].lower() == format.lower():
                file_path = file_info['path']
                if os.path.exists(file_path):
                    return send_file(file_path, as_attachment=True)
    
    return jsonify({'error': 'Arquivo não encontrado'}), 404
# Add this new route after the existing download route in app/routes.py

@bp.route('/download/<session_id>/<format>/<group_key>')
def download_group(session_id, format, group_key):
    """Download files for specific product group"""
    if session_id not in processing_status:
        return jsonify({'error': 'Sessão não encontrada'}), 404
    
    status = processing_status[session_id]
    if status['status'] != 'completed':
        return jsonify({'error': 'Processamento não concluído'}), 400
    
    # Find and serve the requested group file
    if 'results' in status and 'download_info' in status['results']:
        for file_info in status['results']['download_info']['files']:
            if (file_info['format'].lower() == format.lower() and 
                file_info.get('product_group') == group_key):
                file_path = file_info['path']
                if os.path.exists(file_path):
                    return send_file(file_path, as_attachment=True)
    
    return jsonify({'error': 'Arquivo do grupo não encontrado'}), 404

# ===============================
# PROCESSING FUNCTIONS
# ===============================

def process_file(app, session_id):
    """Background file processing with product group support"""
    config = processing_status[session_id]
    
    def update_progress(message, progress=None):
        config['message'] = message
        if progress is not None:
            config['progress'] = progress
        print(f"📊 {message}")
    
    with app.app_context():
        try:
            update_progress("Carregando arquivo...", 10)
            
            # Initialize processor
            processor = GroupBasedDataProcessor(chunk_size=config['chunk_size'])
            
            # Process CSV by groups
            results = processor.process_csv_by_groups(
                config['file_path'],
                obs_column='obs',
                product_group_column='product_group',
                enable_cleaning=True,
                enable_extraction=True,
                progress_callback=update_progress
            )
            
            if not results['success']:
                raise Exception(f"Processamento falhou: {'; '.join(results.get('errors', []))}")
            
            update_progress("Exportando arquivos...", 85)
            
            # Export files with product group support
            download_folder = current_app.config['DOWNLOAD_FOLDER']
            exporter = EnhancedExportHandler()
            filename_base = f"extracted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Handle export formats
            export_formats = []
            if config['export_formats'] == 'both':
                export_formats = ['csv', 'excel']
            elif config['export_formats'] == 'all':
                export_formats = ['csv', 'excel', 'json']
            else:
                export_formats = [config['export_formats']]
            
            # Export with product group manager - THIS IS THE KEY CHANGE
            export_results = exporter.export_data(
                dataframe=results['dataframe'],
                output_dir=download_folder,
                filename_base=filename_base,
                formats=export_formats,
                product_group_manager=processor.group_manager  # Pass the group manager
            )
            
            if not export_results['success']:
                raise Exception(f"Exportação falhou: {'; '.join(export_results.get('errors', []))}")
            
            update_progress("Processamento concluído!", 100)
            
            # Store results with enhanced download info
            config.update({
                'status': 'completed',
                'progress': 100,
                'message': 'Processamento concluído com sucesso!',
                'results': {
                    'stats': results['stats'],
                    'dataframe': results['dataframe'],
                    'download_info': exporter.create_download_info(export_results),
                    'has_product_groups': 'product_group' in results['dataframe'].columns and processor.group_manager is not None
                }
            })
            
            # Clean up uploaded file
            try:
                os.remove(config['file_path'])
            except:
                pass
                
        except Exception as e:
            error_msg = f"Erro: {str(e)}"
            print(f"❌ {error_msg}")
            config.update({
                'status': 'error',
                'message': error_msg
            })

def analyze_sample(file_path, sample_size=5000):
    """Analyze sample data for preview"""
    import re
    
    try:
        # Read file info
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1
        except:
            total_rows = 0
        
        # Read sample
        read_size = min(sample_size * 2, total_rows)
        df = pd.read_csv(file_path, nrows=read_size, low_memory=False)
        
        if 'obs' not in df.columns:
            return {
                'error': f"Coluna 'obs' não encontrada. Colunas disponíveis: {', '.join(df.columns[:10])}"
            }
        
        # Check for product groups
        has_product_groups = 'product_group' in df.columns
        product_groups_info = {}
        
        if has_product_groups:
            for group_key in df['product_group'].dropna().unique():
                if product_group_manager.is_valid_group(group_key):
                    group_info = product_group_manager.get_group_info(group_key)
                    group_data = df[df['product_group'] == group_key]
                    mandatory_fields = product_group_manager.get_mandatory_fields(group_key)
                    
                    product_groups_info[group_key] = {
                        'name': group_info['name'],
                        'record_count': len(group_data),
                        'mandatory_fields': mandatory_fields,
                        'mandatory_field_count': len(mandatory_fields),
                        'category': group_info.get('category', 'unknown')
                    }
        
        # Basic text analysis
        text_data = df['obs'].astype(str)
        text_stats = {
            'total_mb': round(text_data.str.len().sum() / (1024 * 1024), 2),
            'avg_length': int(text_data.str.len().mean()),
            'max_length': int(text_data.str.len().max())
        }
        
        # Quick field detection in 'obs' column
        field_analysis = {}
        sample_texts = text_data.head(100)
        
        # Look for patterns that match the existing CSV columns
        field_patterns = {
            'ip_management': r'IP\s*(?:CPE|management)[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            'gateway': r'GTW[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})',
            'ip_block': r'BLOCO\s*IP[:\s]*([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}/\d+)',
            'vlan': r'VLAN\s*:?\s*(\d+)',
            'serial_code': r'SN[:\s]*([A-Za-z0-9]+)',
            'wifi_ssid': r'SSID[:\s]*([A-Za-z0-9_-]+)',
            'wifi_passcode': r'password[:\s]*([A-Za-z0-9@#$%^&*()_+-=]+)',
            'asn': r'AS\s*Cliente[:\s]*(\d+)',
            'mac': r'([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})',
            'cpe': r'(OLT-[A-Z0-9-]+)',
            'model_onu': r'ONU[:\s]*([A-Za-z0-9-]+)'
        }
        
        for text in sample_texts:
            if pd.notna(text) and isinstance(text, str):
                for field_name, pattern in field_patterns.items():
                    try:
                        if re.search(pattern, text, re.IGNORECASE):
                            field_analysis[field_name] = field_analysis.get(field_name, 0) + 1
                    except:
                        continue
        
        # Analyze existing column completeness
        existing_completeness = {}
        for col in ['ip_management', 'gateway', 'ip_block', 'vlan', 'serial_code', 'wifi_ssid', 'wifi_passcode', 'asn', 'mac', 'cpe', 'model_onu']:
            if col in df.columns:
                filled_count = df[col].notna().sum()
                total_count = len(df)
                existing_completeness[col] = {
                    'filled': filled_count,
                    'total': total_count,
                    'percentage': round((filled_count / total_count) * 100, 1) if total_count > 0 else 0
                }
        
        return {
            'sample_info': {
                'actual_size': len(df),
                'total_file_rows': total_rows,
                'column_name': 'obs',
                'has_product_groups': has_product_groups,
                'product_groups_info': product_groups_info
            },
            'text_stats': text_stats,
            'field_analysis': field_analysis,
            'existing_completeness': existing_completeness,
            'extraction_potential': {
                'total_fields_found': sum(field_analysis.values()),
                'estimated_processing_time': max(1, int((total_rows / 1000) * 0.5)),
                'improvement_potential': sum(1 for comp in existing_completeness.values() if comp['percentage'] < 80)
            }
        }
        
    except Exception as e:
        return {'error': f"Análise falhou: {str(e)}"}