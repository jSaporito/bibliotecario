from flask import Blueprint, render_template, request, jsonify, send_file, flash, redirect, url_for, current_app
from werkzeug.utils import secure_filename
import os
import uuid
from datetime import datetime
import threading
import traceback
import re

from .forms import EnhancedUploadForm as UploadForm
from core.data_processor import EnhancedTelecomDataProcessor
from core.export_handler import EnhancedExportHandler as ExportHandler
from core.text_cleaner import EnhancedTelecomTextCleaner

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
        thread = threading.Thread(target=process_file_simple_enhanced, args=(current_app._get_current_object(), session_id))
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
    if session_id not in processing_status:
        return jsonify({'error': 'Session not found'}), 404
    
    status = processing_status[session_id]
    if status['status'] != 'completed':
        return jsonify({'error': 'Processing not completed'}), 400
    
    print(f"🔍 Download request for session {session_id}, format {format}")
    
    # Find the requested file
    if 'results' in status and 'download_info' in status['results']:
        for file_info in status['results']['download_info']['files']:
            if file_info['format'].lower() == format.lower():
                stored_path = file_info['path']
                
                print(f"🔍 Stored path: {stored_path}")
                print(f"🔍 File exists at stored path: {os.path.exists(stored_path)}")
                
                # 🔧 FIX: Try the configured download folder first
                filename = os.path.basename(stored_path)
                correct_path = os.path.join(current_app.config['DOWNLOAD_FOLDER'], filename)
                
                print(f"🔍 Correct path: {correct_path}")
                print(f"🔍 File exists at correct path: {os.path.exists(correct_path)}")
                
                if os.path.exists(correct_path):
                    print(f"✅ Sending file: {correct_path}")
                    return send_file(correct_path, as_attachment=True)
                elif os.path.exists(stored_path):
                    print(f"✅ Sending file from stored path: {stored_path}")
                    return send_file(stored_path, as_attachment=True)
                
                # Debug: List files in download folder
                download_folder = current_app.config['DOWNLOAD_FOLDER']
                print(f"📁 Files in {download_folder}:")
                try:
                    for f in os.listdir(download_folder):
                        if f.endswith(('.xlsx', '.csv')):
                            print(f"  - {f}")
                except Exception as e:
                    print(f"  Error listing files: {e}")
    
    print(f"❌ File not found for session {session_id}, format {format}")
    return jsonify({'error': 'File not found'}), 404

def process_file_simple_enhanced(app, session_id):
    """Enhanced background processing with improved text cleaning and extraction"""
    config = processing_status[session_id]
    
    def update_progress(message, progress=None):
        config['message'] = message
        if progress is not None:
            config['progress'] = progress
        print(f"DEBUG: [{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    with app.app_context():
        try:
            update_progress("Starting enhanced processing...", 10)
            
            # Use enhanced processor with text cleaning and extraction
            processor = EnhancedTelecomDataProcessor(chunk_size=config['chunk_size'])
            results = processor.process_csv(
                config['file_path'],
                obs_column=config['obs_column'],
                enable_cleaning=True,      # ✅ NEW: Enable text cleaning
                enable_extraction=True,    # ✅ NEW: Enable field extraction
                progress_callback=update_progress
            )
            
            if not results['success']:
                error_details = '; '.join(results.get('errors', ['Unknown error']))
                raise Exception(f"Enhanced processing failed: {error_details}")
            
            update_progress("Processing completed, starting enhanced export...", 80)
            
            # Use enhanced exporter
            from core.export_handler import EnhancedExportHandler
            exporter = EnhancedExportHandler()
            
            download_folder = current_app.config['DOWNLOAD_FOLDER']
            filename_base = f"enhanced_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Handle export formats
            if config['export_formats'] == 'both':
                export_formats = ['csv', 'excel', 'json']  # ✅ NEW: Added JSON support
            else:
                export_formats = [config['export_formats']]
            
            # Enhanced export with detailed statistics
            export_results = exporter.export_data(
                results['dataframe'],
                download_folder,
                filename_base,
                export_formats
            )
            
            if not export_results['success']:
                error_details = '; '.join(export_results.get('errors', ['Export failed']))
                raise Exception(f"Enhanced export failed: {error_details}")
            
            # Success - with enhanced statistics
            config.update({
                'status': 'completed',
                'progress': 100,
                'message': 'Enhanced processing completed successfully!',
                'results': {
                    'stats': {
                        **results['stats'],
                        'enhancement_info': {
                            'text_cleaning_applied': True,
                            'field_extraction_applied': True,
                            'export_formats': len(export_formats)
                        }
                    },
                    'download_info': exporter.create_download_info(export_results)
                }
            })
            
            update_progress("Enhanced processing completed!", 100)
            
            # Print enhanced results summary
            print("🎉 Enhanced Processing Results:")
            print(f"  - Text cleaning: {results['stats'].get('cleaning_stats', {})}")
            print(f"  - Field extraction: {results['stats'].get('extraction_stats', {})}")
            print(f"  - Export formats: {len(export_formats)}")
            
            # Cleanup
            try:
                os.remove(config['file_path'])
                print(f"🗑️ Cleaned up uploaded file: {config['file_path']}")
            except Exception as e:
                print(f"⚠️ Cleanup warning: {str(e)}")
                
        except Exception as e:
            error_msg = f"Enhanced processing error: {str(e)}"
            print(f"❌ {error_msg}")
            print(traceback.format_exc())
            
            config.update({
                'status': 'error',
                'message': error_msg
            })
            
@bp.route('/quick-analysis', methods=['POST'])
def quick_analysis():
    """Quick analysis of uploaded file before full processing"""
    form = UploadForm()
    
    if form.validate_on_submit():
        try:
            # Save uploaded file temporarily
            file = form.file.data
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"temp_analysis_{timestamp}_{filename}")
            file.save(temp_path)
            
            print(f"🔍 Starting quick analysis of: {filename}")
            
            # Quick analysis
            analysis = analyze_large_sample_quick(
                temp_path, 
                form.obs_column.data, 
                sample_size=5000  # Analyze 5K rows for good representation
            )
            
            # Clean up temp file
            try:
                os.remove(temp_path)
                print(f"🗑️ Cleaned up temp file: {temp_path}")
            except Exception as e:
                print(f"⚠️ Could not remove temp file: {e}")
            
            if 'error' in analysis:
                flash(f'Analysis failed: {analysis["error"]}', 'error')
                return redirect(url_for('main.upload'))
            
            return render_template('analysis_preview.html', 
                                 analysis=analysis, 
                                 form=form,
                                 filename=filename)
            
        except Exception as e:
            flash(f'Analysis failed: {str(e)}', 'error')
            print(f"❌ Analysis error: {str(e)}")
            return redirect(url_for('main.upload'))
    
    # If form validation fails
    flash('Please fix the form errors and try again', 'error')
    return render_template('upload.html', form=form)

def analyze_large_sample_quick(file_path, obs_column='obs', sample_size=5000):
    """Quick analysis of large sample for optimization"""
    import pandas as pd
    import re
    from collections import Counter
    
    try:
        print(f"🔍 Analyzing up to {sample_size:,} rows for pattern optimization...")
        
        # Read file info first
        try:
            # Get total rows efficiently
            with open(file_path, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1  # -1 for header
        except:
            total_rows = 0
        
        # Read a sample for analysis
        read_size = min(sample_size * 2, total_rows)  # Read extra to ensure enough data
        df = pd.read_csv(file_path, nrows=read_size, low_memory=False)
        
        print(f"📖 Read {len(df):,} rows from file")
        
        # Check if obs column exists
        if obs_column not in df.columns:
            return {
                'error': f"Column '{obs_column}' not found. Available columns: {', '.join(df.columns[:10])}"
            }
        
        # Filter to rows with obs data
        df_obs = df[df[obs_column].notna() & (df[obs_column] != '')].head(sample_size)
        
        if len(df_obs) == 0:
            return {
                'error': f"No valid data found in column '{obs_column}'"
            }
        
        analysis = {
            'sample_info': {
                'requested_size': sample_size,
                'actual_size': len(df_obs),
                'total_file_rows': total_rows,
                'column_name': obs_column
            },
            'text_stats': {},
            'noise_analysis': {},
            'field_analysis': {},
            'recommendations': {}
        }
        
        obs_texts = df_obs[obs_column].astype(str)
        print(f"📊 Analyzing {len(obs_texts):,} text entries...")
        
        # Text statistics
        lengths = obs_texts.str.len()
        lines = obs_texts.str.split('\n').str.len()
        
        analysis['text_stats'] = {
            'avg_length': int(lengths.mean()),
            'median_length': int(lengths.median()),
            'max_length': int(lengths.max()),
            'min_length': int(lengths.min()),
            'total_mb': round(lengths.sum() / (1024 * 1024), 2),
            'avg_lines': round(lines.mean(), 1),
        }
        
        # Sample subset for pattern analysis (for performance)
        sample_for_patterns = obs_texts.head(min(1000, len(obs_texts)))
        print(f"🔍 Analyzing patterns in {len(sample_for_patterns):,} entries...")
        
        # Noise pattern analysis
        noise_counts = {
            'separator_lines': 0,
            'empty_lines': 0,
            'debug_lines': 0,
            'html_tags': 0,
            'command_noise': 0
        }
        
        noise_patterns = {
            'separator_lines': r'^[-=_~*+#]{5,}',
            'empty_lines': r'^\s*$',
            'debug_lines': r'(DEBUG|INFO|WARNING|ERROR):',
            'html_tags': r'<[^>]+>',
            'command_noise': r'^\s*(quit|exit|end|return)\s*$'
        }
        
        # Field pattern analysis
        field_counts = {
            'ip_addresses': 0,
            'vlan_ids': 0,
            'serial_numbers': 0,
            'ticket_numbers': 0,
            'equipment_names': 0,
            'service_codes': 0,
            'asn_numbers': 0,
            'optical_power': 0,
            'mac_addresses': 0,
            'ipv6_addresses': 0
        }
        
        field_patterns = {
            'ip_addresses': r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
            'vlan_ids': r'VLAN\s*:?\s*\d+',
            'serial_numbers': r'SN[:\s]*[A-Za-z0-9]+',
            'ticket_numbers': r'#\d{8}-\d+',
            'equipment_names': r'OLT-[A-Z0-9-]+',
            'service_codes': r'[A-Z]{3,}/[A-Z]{2,}/\d+',
            'asn_numbers': r'AS\s*Cliente[:\s]*\d+',
            'optical_power': r'Rx\s*Optical\s*Power.*?=\s*[-\d.]+',
            'mac_addresses': r'([0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}',
            'ipv6_addresses': r'[0-9a-fA-F:]+::/\d{1,3}'
        }
        
        # Count patterns
        for text in sample_for_patterns:
            try:
                # Count noise
                for name, pattern in noise_patterns.items():
                    matches = len(re.findall(pattern, text, re.MULTILINE | re.IGNORECASE))
                    noise_counts[name] += matches
                
                # Count fields
                for name, pattern in field_patterns.items():
                    matches = len(re.findall(pattern, text, re.IGNORECASE))
                    field_counts[name] += matches
            except Exception as e:
                continue  # Skip problematic texts
        
        analysis['noise_analysis'] = noise_counts
        analysis['field_analysis'] = field_counts
        
        # Calculate potential impact
        total_noise = sum(noise_counts.values())
        total_fields = sum(field_counts.values())
        
        # Estimate cleaning impact
        sample_for_cleaning = '\n'.join(sample_for_patterns.head(50))
        cleaned_sample = simulate_text_cleaning(sample_for_cleaning)
        reduction_percent = round(100 * (1 - len(cleaned_sample) / len(sample_for_cleaning)), 1) if len(sample_for_cleaning) > 0 else 0
        
        # Generate recommendations
        analysis['recommendations'] = {
            'cleaning_impact': {
                'reduction_percent': reduction_percent,
                'estimated_mb_saved': round(analysis['text_stats']['total_mb'] * (reduction_percent / 100), 2)
            },
            'extraction_potential': {
                'total_fields_found': total_fields,
                'fields_per_record': round(total_fields / len(sample_for_patterns), 2) if len(sample_for_patterns) > 0 else 0,
                'most_common_fields': sorted([(k, v) for k, v in field_counts.items() if v > 0], 
                                           key=lambda x: x[1], reverse=True)[:5]
            },
            'processing_suggestions': {
                'recommended_chunk_size': 2000 if total_rows > 20000 else 5000,
                'estimated_processing_time_minutes': round(total_rows / 1000, 1),
                'memory_usage_warning': total_rows > 25000
            }
        }
        
        print(f"✅ Analysis completed:")
        print(f"   - Sample size: {len(df_obs):,} rows")
        print(f"   - Noise items found: {total_noise:,}")
        print(f"   - Fields found: {total_fields:,}")
        print(f"   - Text reduction potential: {reduction_percent}%")
        
        return analysis
        
    except Exception as e:
        error_msg = f"Sample analysis failed: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(traceback.format_exc())
        return {'error': error_msg}

def simulate_text_cleaning(text):
    """Quick simulation of text cleaning"""
    if not text:
        return text
    
    cleaned = text
    
    # Remove common noise patterns
    cleaned = re.sub(r'^[-=_~*+#]{5,}.*$', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'^#+\s*$', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    cleaned = re.sub(r'[ \t]{3,}', ' ', cleaned)
    cleaned = re.sub(r'^\s+$', '', cleaned, flags=re.MULTILINE)
    
    return cleaned.strip()        

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