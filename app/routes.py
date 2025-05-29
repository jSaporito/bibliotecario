from flask import Blueprint, render_template, request, jsonify, send_file, flash, redirect, url_for, current_app
from werkzeug.utils import secure_filename
import os
import uuid
from datetime import datetime
import threading

try:
    from .forms import UploadForm
except ImportError:
    from app.forms import UploadForm

from core.data_processor import DataProcessor
from core.export_handler import ExportHandler

bp = Blueprint('main', __name__)
processing_status = {}

@bp.route('/')
def index():
    return render_template('index.html')

@bp.route('/upload', methods=['GET', 'POST'])
def upload():
    form = UploadForm()
    
    if form.validate_on_submit():
        session_id = str(uuid.uuid4())
        
        # Save file
        file = form.file.data
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"{timestamp}_{filename}")
        file.save(upload_path)
        
        # Store config
        config = {
            'session_id': session_id,
            'file_path': upload_path,
            'obs_column': form.obs_column.data,
            'chunk_size': form.chunk_size.data,
            'export_formats': form.export_formats.data,
            'status': 'processing',
            'progress': 0,
            'message': 'Iniciando processamento...',
            'start_time': datetime.now()
        }
        processing_status[session_id] = config
        
        # Start processing
        thread = threading.Thread(target=process_file, args=(current_app._get_current_object(), session_id,))
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('main.processing', session_id=session_id))
    
    return render_template('upload.html', form=form)

@bp.route('/processing/<session_id>')
def processing(session_id):
    if session_id not in processing_status:
        flash('Sessão inválida', 'error')
        return redirect(url_for('main.index'))
    return render_template('processing.html', session_id=session_id)

@bp.route('/api/status/<session_id>')
def get_status(session_id):
    if session_id not in processing_status:
        return jsonify({'error': 'Sessão não encontrada'}), 404
    
    status = processing_status[session_id]
    return jsonify({
        'status': status['status'],
        'progress': status['progress'],
        'message': status['message'],
        'start_time': status['start_time'].isoformat()
    })

@bp.route('/results/<session_id>')
def results(session_id):
    if session_id not in processing_status:
        flash('Sessão inválida', 'error')
        return redirect(url_for('main.index'))
    
    status = processing_status[session_id]
    if status['status'] != 'completed':
        return redirect(url_for('main.processing', session_id=session_id))
    
    return render_template('results.html', session_id=session_id, results=status.get('results'))

@bp.route('/download/<session_id>/<format>')
def download(session_id, format):
    if session_id not in processing_status:
        return jsonify({'error': 'Sessão não encontrada'}), 404
    
    status = processing_status[session_id]
    if status['status'] != 'completed':
        return jsonify({'error': 'Processamento não concluído'}), 400
    
    # Find file
    download_info = status['results'].get('download_info', {})
    for file_info in download_info.get('files', []):
        if file_info['format'].lower() == format.lower():
            if os.path.exists(file_info['path']):
                return send_file(file_info['path'], as_attachment=True, download_name=file_info['filename'])
    
    return jsonify({'error': 'Arquivo não encontrado'}), 404

from flask import current_app

def process_file(session_id):
    with current_app.app_context():
        config = processing_status[session_id]

        def update_progress(message, progress=None):
            if session_id in processing_status:
                processing_status[session_id]['message'] = message
                if progress is not None:
                    processing_status[session_id]['progress'] = progress

        try:
            update_progress("Processando arquivo...", 10)

            # Process
            processor = DataProcessor(chunk_size=config['chunk_size'])
            results = processor.process_csv(
                config['file_path'],
                obs_column=config['obs_column'],
                progress_callback=lambda msg, prog=None: update_progress(msg, prog or 50)
            )

            if not results['success']:
                raise Exception(f"Processamento falhou: {'; '.join(results.get('errors', []))}")

            update_progress("Exportando arquivos...", 80)

            # Export
            exporter = ExportHandler()
            formats = []
            if config['export_formats'] == 'csv':
                formats = ['csv']
            elif config['export_formats'] == 'excel':
                formats = ['excel']
            elif config['export_formats'] == 'both':
                formats = ['csv', 'excel']
            else:
                formats = ['csv', 'excel']

            filename_base = f"processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            export_results = exporter.export_data(
                results['dataframe'],
                current_app.config['DOWNLOAD_FOLDER'],
                filename_base,
                formats
            )

            if not export_results['success']:
                raise Exception(f"Exportação falhou: {'; '.join(export_results.get('errors', []))}")

            # Success
            processing_status[session_id].update({
                'status': 'completed',
                'progress': 100,
                'message': 'Processamento concluído!',
                'end_time': datetime.now(),
                'results': {
                    'stats': results.get('stats', {}),
                    'download_info': exporter.create_download_info(export_results)
                }
            })

            # Cleanup
            try:
                os.remove(config['file_path'])
            except:
                pass

        except Exception as e:
            processing_status[session_id].update({
                'status': 'error',
                'message': f"Erro: {str(e)}",
                'end_time': datetime.now()
            })

@bp.errorhandler(404)
def not_found(error):
    return render_template('error.html', error_code=404, error_message="Página não encontrada"), 404

from flask import current_app

@bp.errorhandler(500)
def internal_server_error(error):
    import traceback
    trace = traceback.format_exc()
    current_app.logger.error(f"Internal server error: {error}\n{trace}")
    return "Internal server error occurred.", 500

@bp.errorhandler(413)
def file_too_large(error):
    return render_template('error.html', error_code=413, error_message="Arquivo muito grande"), 413