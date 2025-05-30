from flask import Blueprint, render_template, request, jsonify, send_file, flash, redirect, url_for, current_app
from werkzeug.utils import secure_filename
import os
import uuid
from datetime import datetime
import threading
import traceback
import re
import pandas as pd  # ← Adicione esta linha se não estiver

# Import your form
from app.forms import UploadForm

# Import your core modules
from core.data_processor import EnhancedTelecomDataProcessor
from core.export_handler import EnhancedExportHandler
# Import your form
from app.forms import UploadForm

# Import your core modules
from core.data_processor import EnhancedTelecomDataProcessor
from core.export_handler import EnhancedExportHandler

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
        
        # Store processing config
        processing_status[session_id] = {
            'file_path': upload_path,
            'obs_column': form.obs_column.data,
            'chunk_size': form.chunk_size.data,
            'export_formats': form.export_formats.data,
            'status': 'processing',
            'progress': 0,
            'message': 'Iniciando...',
            'download_folder': current_app.config['DOWNLOAD_FOLDER']
        }
        
        # Start background processing
        thread = threading.Thread(target=process_file_simple, args=(current_app._get_current_object(), session_id))
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('main.processing', session_id=session_id))
    
    return render_template('upload.html', form=form)

@bp.route('/quick-analysis', methods=['POST'])
def quick_analysis():
    """Quick analysis of uploaded file before full processing"""
    form = UploadForm()
    
    if form.validate_on_submit():
        try:
            # Generate session ID for this analysis
            session_id = str(uuid.uuid4())
            
            # Save uploaded file temporarily
            file = form.file.data
            filename = secure_filename(file.filename)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            temp_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"temp_analysis_{timestamp}_{filename}")
            file.save(temp_path)
            
            print(f"🔍 Starting analysis of: {filename}")
            
            # Quick analysis
            analysis = analyze_large_sample_quick(
                temp_path, 
                form.obs_column.data, 
                sample_size=5000
            )
            
            # Store analysis data for visual metrics
            processing_status[session_id] = {
                'file_path': temp_path,
                'obs_column': form.obs_column.data,
                'analysis_data': analysis,
                'filename': filename,
                'timestamp': datetime.now().isoformat()
            }
            
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
                                 filename=filename,
                                 session_id=session_id)
            
        except Exception as e:
            flash(f'Analysis failed: {str(e)}', 'error')
            print(f"❌ Analysis error: {str(e)}")
            return redirect(url_for('main.upload'))
    
    flash('Corrija os erros do formulário e tente novamente', 'error')
    return render_template('upload.html', form=form)

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
            
            # Initialize variables
            original_df = None
            processed_df = None
            
            # Load processed data first (this should always exist)
            try:
                processed_df = config['results']['dataframe']
                print(f"✅ Loaded processed dataframe: {processed_df.shape}")
            except Exception as e:
                flash(f'Erro ao carregar dados processados: {str(e)}', 'error')
                return redirect(url_for('main.results', session_id=session_id))
            
            # Load original data for comparison
            try:
                original_df = load_original_dataframe(config['file_path'], config['obs_column'])
                print(f"✅ Loaded original dataframe: {original_df.shape}")
            except Exception as e:
                print(f"⚠️  Warning: Could not load original dataframe: {str(e)}")
                # Create a minimal original dataframe for comparison
                original_df = pd.DataFrame({config['obs_column']: [''] * len(processed_df)})
            
            # Verify we have valid dataframes
            if original_df is None or original_df.empty:
                print("⚠️  Creating dummy original dataframe")
                original_df = pd.DataFrame({config['obs_column']: [''] * max(1, len(processed_df))})
            
            if processed_df is None or processed_df.empty:
                flash('Dados processados estão vazios.', 'error')
                return redirect(url_for('main.results', session_id=session_id))
            
            # Debug info
            print(f"🔍 Final check - Original: {original_df.shape}, Processed: {processed_df.shape}")
            
            # Generate visualization report
            try:
                from core.data_visualizer import create_data_visualization_report
            except ImportError as e:
                flash(f'Módulo de visualização não encontrado: {str(e)}', 'error')
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
            import traceback
            print(traceback.format_exc())
            return redirect(url_for('main.results', session_id=session_id))

def load_original_dataframe(file_path, obs_column):
    """Load original dataframe for comparison"""
    import pandas as pd
    
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"⚠️  Original file not found: {file_path}")
            return pd.DataFrame()
        
        # Try multiple encodings
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                print(f"✅ Loaded original CSV with {encoding} encoding")
                return df
            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                print(f"⚠️  Error with {encoding}: {e}")
                continue
        
        # If all encodings fail, try with python engine
        try:
            df = pd.read_csv(file_path, encoding='utf-8', engine='python', low_memory=False)
            print("✅ Loaded original CSV with python engine")
            return df
        except Exception as e:
            print(f"⚠️  Python engine failed: {e}")
        
        # Final fallback - return empty dataframe
        print("❌ All methods failed, returning empty dataframe")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ Error loading original dataframe: {str(e)}")
        return pd.DataFrame()

@bp.route('/visual-analysis/<session_id>')
def visual_analysis(session_id):
    """Generate visual metrics analysis"""
    if session_id not in processing_status:
        flash('Sessão não encontrada. Execute a análise primeiro.', 'error')
        return redirect(url_for('main.upload'))
    
    try:
        config = processing_status[session_id]
        
        # Check if we have analysis data stored
        if 'analysis_data' not in config:
            flash('Execute a análise primeiro antes de gerar métricas visuais', 'warning')
            return redirect(url_for('main.upload'))
        
        analysis_data = config['analysis_data']
        
        # Create mock metrics for demonstration
        mock_metrics = create_mock_visual_metrics(analysis_data)
        executive_summary = create_mock_executive_summary(analysis_data)
        
        return render_template('visual_metrics.html', 
                             metrics=mock_metrics,
                             executive_summary=executive_summary,
                             analysis_data=analysis_data,
                             session_id=session_id)
        
    except Exception as e:
        flash(f'Visual analysis failed: {str(e)}', 'error')
        print(f"❌ Visual analysis error: {str(e)}")
        return redirect(url_for('main.upload'))

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
    
    return render_template('results.html', session_id=session_id, results=status.get('results'))

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

# HELPER FUNCTIONS

def analyze_large_sample_quick(file_path, obs_column='obs', sample_size=5000):
    """Quick analysis of large sample for optimization"""
    import pandas as pd
    
    try:
        print(f"🔍 Analyzing up to {sample_size:,} rows for pattern optimization...")
        
        # Read file info first
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1
        except:
            total_rows = 0
        
        # Read a sample for analysis
        read_size = min(sample_size * 2, total_rows)
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
        
        # Pattern analysis
        sample_for_patterns = obs_texts.head(min(1000, len(obs_texts)))
        
        # Noise patterns
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
        
        # Field patterns
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
            except Exception:
                continue
        
        analysis['noise_analysis'] = noise_counts
        analysis['field_analysis'] = field_counts
        
        # Calculate recommendations
        total_fields = sum(field_counts.values())
        
        # Estimate cleaning impact
        sample_for_cleaning = '\n'.join(sample_for_patterns.head(50))
        cleaned_sample = simulate_text_cleaning(sample_for_cleaning)
        reduction_percent = round(100 * (1 - len(cleaned_sample) / len(sample_for_cleaning)), 1) if len(sample_for_cleaning) > 0 else 0
        
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
        
        print(f"✅ Analysis completed successfully")
        return analysis
        
    except Exception as e:
        error_msg = f"Sample analysis failed: {str(e)}"
        print(f"❌ {error_msg}")
        return {'error': error_msg}

def simulate_text_cleaning(text):
    """Quick simulation of text cleaning"""
    if not text:
        return text
    
    cleaned = text
    cleaned = re.sub(r'^[-=_~*+#]{5,}.*$', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'^#+\s*$', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    cleaned = re.sub(r'[ \t]{3,}', ' ', cleaned)
    cleaned = re.sub(r'^\s+$', '', cleaned, flags=re.MULTILINE)
    
    return cleaned.strip()

def create_mock_visual_metrics(analysis_data):
    """Create mock visual metrics for demonstration"""
    return {
        'charts': {
            'data_quality': 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==',
            'processing_efficiency': 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==',
            'field_extraction': 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==',
            'cost_savings': 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==',
            'time_comparison': 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==',
            'storage_optimization': 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=='
        },
        'business_impact': {
            'time_savings': {
                'hours_per_process': 40.0,
                'days_per_process': 5.0,
                'annual_hours_saved': 480
            },
            'cost_savings': {
                'per_process': 2000,
                'annually': 24000,
                'roi_months': 3
            },
            'quality_improvements': {
                'accuracy_increase': '25%',
                'fields_extracted': '15,000',
                'data_reduction': f"{analysis_data['recommendations']['cleaning_impact']['reduction_percent']}%"
            },
            'operational_benefits': {
                'processing_speed': '50x faster',
                'consistency': '100% consistent results',
                'scalability': 'Handles any dataset size'
            }
        },
        'roi_analysis': {
            'investment': {
                'software_annual': 2000,
                'training_onetime': 1000,
                'setup_onetime': 500,
                'total_first_year': 3500
            },
            'returns': {
                'annual_savings': 24000,
                'roi_percentage': 585,
                'payback_months': 2.6,
                'net_benefit_year1': 20500
            },
            'break_even_analysis': {
                'break_even_month': 3,
                'monthly_savings': 2000,
                'cumulative_benefit_3_years': 68500
            }
        },
        'efficiency_gains': {
            'processing_speed': '50x faster than manual',
            'accuracy_improvement': '25% higher accuracy',
            'consistency': '100% consistent results',
            'scalability': f"Can process {analysis_data['sample_info']['total_file_rows']:,} rows without additional staff",
            'error_reduction': '90% reduction in human errors',
            'availability': '24/7 processing capability'
        }
    }

def create_mock_executive_summary(analysis_data):
    """Create mock executive summary"""
    return {
        'headline_benefits': [
            "Save 5+ days per processing cycle",
            "Reduce costs by $24,000 annually",
            "Achieve 585% ROI in first year",
            "Extract 15,000+ structured data fields"
        ],
        'key_metrics': {
            'time_reduction': '50x faster',
            'cost_savings': '$24,000/year',
            'roi': '585%',
            'payback': '2.6 months'
        },
        'competitive_advantage': [
            'Tempo mais rápido para insights em decisões empresariais',
            'Maior qualidade e consistência dos dados',
            'Solução escalável para volumes crescentes de dados',
            'Dependência reduzida de processos manuais'
        ]
    }

def process_file_simple(app, session_id):
    """Background processing function"""
    config = processing_status[session_id]
    
    def update_progress(message, progress=None):
        config['message'] = message
        if progress is not None:
            config['progress'] = progress
        print(f"DEBUG: [{datetime.now().strftime('%H:%M:%S')}] {message}")
    
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
            
            # Export files
            download_folder = current_app.config['DOWNLOAD_FOLDER']
            exporter = EnhancedExportHandler()
            filename_base = f"processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
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
                    'dataframe': results['dataframe'],  # Adicione esta linha
                    'download_info': exporter.create_download_info(export_results)
                }
            })
            update_progress("All processing completed!", 100)
            try:
                os.remove(config['file_path'])
                print(f"🗑️ Cleaned up uploaded file: {config['file_path']}")
            except Exception as e:
                print(f"⚠️ Cleanup warning: {str(e)}")
                
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            print(f"❌ Processing error: {error_msg}")
            print(traceback.format_exc())
            
            config.update({
                'status': 'error',
                'message': error_msg
            })