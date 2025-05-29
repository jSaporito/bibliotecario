from flask import Blueprint, render_template, request, jsonify, send_file, flash, redirect, url_for, current_app
from werkzeug.utils import secure_filename
import os
import uuid
from datetime import datetime
import threading
import traceback

from .forms import UploadForm
from core.data_processor import EnhancedTelecomDataProcessor
from core.export_handler import ExportHandler

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
        
        print(f"DEBUG: File saved to {upload_path}")
        
        # Store processing config with app config values
        processing_status[session_id] = {
            'file_path': upload_path,
            'obs_column': form.obs_column.data,
            'chunk_size': form.chunk_size.data,
            'export_formats': form.export_formats.data,
            'status': 'processing',
            'progress': 0,
            'message': 'Starting...',
            'debug_info': [],
            # ✅ Pass config values directly to avoid context issues
            'download_folder': current_app.config['DOWNLOAD_FOLDER']
        }
        
        # Start background processing with app context
        thread = threading.Thread(target=process_file_simple, args=(current_app._get_current_object(), session_id))
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('main.processing', session_id=session_id))
    
    return render_template('upload.html', form=form)

@bp.route('/processing/<session_id>')
def processing(session_id):
    """Processing status page"""
    if session_id not in processing_status:
        flash('Invalid session', 'error')
        return redirect(url_for('main.index'))
    return render_template('processing.html', session_id=session_id)

@bp.route('/api/status/<session_id>')
def get_status(session_id):
    """Get processing status"""
    if session_id not in processing_status:
        return jsonify({'error': 'Session not found'}), 404
    
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
    
    return render_template('results.html', session_id=session_id, results=status.get('results'))

@bp.route('/download/<session_id>/<format>')
def download(session_id, format):
    """Download processed file"""
    if session_id not in processing_status:
        return jsonify({'error': 'Session not found'}), 404
    
    status = processing_status[session_id]
    if status['status'] != 'completed':
        return jsonify({'error': 'Processing not completed'}), 400
    
    # Find and return file
    if 'results' in status and 'download_info' in status['results']:
        for file_info in status['results']['download_info']['files']:
            if file_info['format'].lower() == format.lower():
                if os.path.exists(file_info['path']):
                    return send_file(file_info['path'], as_attachment=True)
    
    return jsonify({'error': 'File not found'}), 404

def process_file_simple(app, session_id):
    """Simple background processing with app context"""
    config = processing_status[session_id]
    
    def update_progress(message, progress=None):
        config['message'] = message
        if progress is not None:
            config['progress'] = progress
        print(f"DEBUG: [{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    # ✅ Use the passed app object with app context
    with app.app_context():
        try:
            update_progress("Starting processing...", 10)
            
            # Process file
            processor = EnhancedTelecomDataProcessor(chunk_size=config['chunk_size'])
            results = processor.process_csv(
                config['file_path'],
                obs_column=config['obs_column'],
                progress_callback=update_progress
            )
            
            if not results['success']:
                error_details = '; '.join(results.get('errors', ['Unknown error']))
                raise Exception(f"Processing failed: {error_details}")
            
            update_progress("Processing completed, starting export...", 80)
            
            # Export files using stored config
            exporter = ExportHandler()
            filename_base = f"processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Use stored download folder instead of current_app
            download_folder = config['download_folder']
            
            # Handle export formats
            if config['export_formats'] == 'both':
                export_formats = ['csv', 'excel']
            else:
                export_formats = [config['export_formats']]
            
            export_results = exporter.export_data(
                results['dataframe'],
                download_folder,
                filename_base,
                export_formats
            )
            
            if not export_results['success']:
                error_details = '; '.join(export_results.get('errors', ['Export failed']))
                raise Exception(f"Export failed: {error_details}")
            
            # Success
            config.update({
                'status': 'completed',
                'progress': 100,
                'message': 'Processing completed successfully!',
                'results': {
                    'stats': results['stats'],
                    'download_info': exporter.create_download_info(export_results)
                }
            })
            
            update_progress("All processing completed!", 100)
            
            # Cleanup
            try:
                os.remove(config['file_path'])
            except Exception as e:
                print(f"Cleanup warning: {str(e)}")
                
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            print(f"Processing error: {error_msg}")
            print(traceback.format_exc())
            
            config.update({
                'status': 'error',
                'message': error_msg
            })

# Simple debug endpoint
@bp.route('/debug/<session_id>')
def debug_info(session_id):
    """Simple debug information"""
    if session_id not in processing_status:
        return "Session not found", 404
    
    status = processing_status[session_id]
    return f"""
    <html>
    <head><title>Debug - {session_id}</title></head>
    <body style="font-family: monospace; padding: 20px;">
    <h2>Status: {status.get('status', 'unknown')}</h2>
    <h3>Progress: {status.get('progress', 0)}%</h3>
    <h3>Message: {status.get('message', 'No message')}</h3>
    <pre>{status}</pre>
    <p><a href="/">Back to Home</a></p>
    </body>
    </html>
    """