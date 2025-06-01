from flask import Blueprint, render_template, request, jsonify, send_file, flash, redirect, url_for, current_app
from werkzeug.utils import secure_filename
import os
import uuid
from datetime import datetime
import threading
import traceback
import re
import pandas as pd

# Import forms and core modules
from app.forms import UploadForm
from core.data_processor import EnhancedTelecomDataProcessor
from core.export_handler import EnhancedExportHandler
from core.product_groups import product_group_manager

bp = Blueprint('main', __name__)
processing_status = {}

import sys
def debug_excepthook(exc_type, exc_value, exc_traceback):
    if 'Invalid format specifier' in str(exc_value):
        print(f'FORMAT ERROR: {exc_value}')
        traceback.print_exception(exc_type, exc_value, exc_traceback)
    sys.__excepthook__(exc_type, exc_value, exc_traceback)
sys.excepthook = debug_excepthook    

@bp.route('/')
def index():
    """Home page"""
    return render_template('index.html')

@bp.route('/upload', methods=['GET', 'POST'])
def upload():
    """File upload and processing with product group support"""
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
        
        print(f"✅ File saved to {upload_path}")
        
        # Store processing config
        processing_status[session_id] = {
            'file_path': upload_path,
            'obs_column': form.obs_column.data,
            'chunk_size': form.chunk_size.data,
            'export_formats': form.export_formats.data,
            'enable_cleaning': getattr(form, 'enable_cleaning', True),
            'enable_extraction': getattr(form, 'enable_extraction', True),
            'status': 'processing',
            'progress': 0,
            'message': 'Iniciando processamento avançado...',
            'download_folder': current_app.config['DOWNLOAD_FOLDER']
        }
        
        # Start background processing with product group support
        thread = threading.Thread(target=process_file_enhanced, args=(current_app._get_current_object(), session_id))
        thread.daemon = True
        thread.start()
        
        return redirect(url_for('main.processing', session_id=session_id))
    
    return render_template('upload.html', form=form)

@bp.route('/quick-analysis', methods=['POST'])
def quick_analysis():
    """Enhanced quick analysis with product group detection"""
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
            
            print(f"🔍 Starting enhanced analysis of: {filename}")
            
            # Enhanced analysis with product group detection
            analysis = analyze_large_sample_enhanced(
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
                flash(f'Análise falhou: {analysis["error"]}', 'error')
                return redirect(url_for('main.upload'))
            
            return render_template('analysis_preview.html', 
                                 analysis=analysis, 
                                 form=form,
                                 filename=filename,
                                 session_id=session_id)
            
        except Exception as e:
            flash(f'Análise falhou: {str(e)}', 'error')
            print(f"❌ Analysis error: {str(e)}")
            return redirect(url_for('main.upload'))
    
    flash('Corrija os erros do formulário e tente novamente', 'error')
    return render_template('upload.html', form=form)

@bp.route('/product-group-analysis/<session_id>')
def product_group_analysis(session_id):
    """Product group specific analysis and completeness report"""
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
        
        # Load processed data
        processed_df = config['results']['dataframe']
        
        # Generate product group analysis
        group_analysis = analyze_product_group_completeness(processed_df)
        
        # Get processing summary if available
        processing_summary = config['results'].get('processing_summary', {})
        group_completeness = processing_summary.get('group_completeness', {})
        
        return render_template('product_group_analysis.html', 
                             group_analysis=group_analysis,
                             group_completeness=group_completeness,
                             processing_summary=processing_summary,
                             session_id=session_id)
        
    except Exception as e:
        flash(f'Erro ao gerar análise de grupos de produtos: {str(e)}', 'error')
        print(f"❌ Product group analysis error: {str(e)}")
        return redirect(url_for('main.results', session_id=session_id))

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
        
        # Load processed data
        processed_df = config['results']['dataframe']
        print(f"✅ Loaded processed dataframe: {processed_df.shape}")
        
        # Load original data for comparison
        try:
            original_df = load_original_dataframe(config['file_path'], config['obs_column'])
            print(f"✅ Loaded original dataframe: {original_df.shape}")
        except Exception as e:
            print(f"⚠️ Warning: Could not load original dataframe: {str(e)}")
            # Create a minimal original dataframe for comparison
            original_df = pd.DataFrame({config['obs_column']: [''] * len(processed_df)})
        
        # Verify we have valid dataframes
        if original_df is None or original_df.empty:
            print("⚠️ Creating dummy original dataframe")
            original_df = pd.DataFrame({config['obs_column']: [''] * max(1, len(processed_df))})
        
        if processed_df is None or processed_df.empty:
            flash('Dados processados estão vazios.', 'error')
            return redirect(url_for('main.results', session_id=session_id))
        
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
        
        enhanced_metrics = create_enhanced_visual_metrics(analysis_data)
        executive_summary = create_enhanced_executive_summary(analysis_data)
        
        return render_template('visual_metrics.html', 
                             metrics=enhanced_metrics,
                             executive_summary=executive_summary,
                             analysis_data=analysis_data,
                             session_id=session_id)
        
    except Exception as e:
        flash(f'Análise visual falhou: {str(e)}', 'error')
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
    """Results page with product group support"""
    if session_id not in processing_status:
        return redirect(url_for('main.index'))
    
    status = processing_status[session_id]
    if status['status'] != 'completed':
        return redirect(url_for('main.processing', session_id=session_id))
    
    # Check if product groups were processed
    has_product_groups = status.get('results', {}).get('has_product_groups', False)
    
    return render_template('results.html', 
                         session_id=session_id, 
                         results=status.get('results'),
                         has_product_groups=has_product_groups)

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

# ===============================
# ENHANCED PROCESSING FUNCTIONS
# ===============================

def process_file_enhanced(app, session_id):
    """Enhanced background processing function with product group support"""
    config = processing_status[session_id]
    
    def update_progress(message, progress=None):
        config['message'] = message
        if progress is not None:
            config['progress'] = progress
        print(f"📊 [{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    with app.app_context():
        try:
            update_progress("Iniciando processamento avançado com suporte a grupos de produto...", 5)
            
            # Check if CSV has product group column
            temp_df = pd.read_csv(config['file_path'], nrows=5)
            has_product_groups = 'product_group' in temp_df.columns
            
            if has_product_groups:
                update_progress("✅ Grupos de produto detectados, otimizando extração...", 10)
                print(f"🏷️ Product groups found in CSV")
            else:
                update_progress("⚠️ Nenhum grupo de produto encontrado, usando processamento genérico...", 10)
                print(f"⚠️ No product groups found, using generic processing")
            
            # Process file with enhanced processor
            processor = EnhancedTelecomDataProcessor(chunk_size=config['chunk_size'])
            
            results = processor.process_csv(
                config['file_path'],
                obs_column=config['obs_column'],
                product_group_column='product_group',
                enable_cleaning=config.get('enable_cleaning', True),
                enable_extraction=config.get('enable_extraction', True),
                progress_callback=update_progress
            )
            
            if not results['success']:
                error_details = '; '.join(results.get('errors', ['Erro desconhecido']))
                raise Exception(f"Processamento falhou: {error_details}")
            
            update_progress("Processamento concluído, iniciando exportação...", 80)
            
            # Export files
            download_folder = current_app.config['DOWNLOAD_FOLDER']
            exporter = EnhancedExportHandler()
            filename_base = f"bibliotecario_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Handle export formats
            export_formats = []
            if config['export_formats'] == 'both':
                export_formats = ['csv', 'excel']
            elif config['export_formats'] == 'all':
                export_formats = ['csv', 'excel', 'json']
            else:
                export_formats = [config['export_formats']]
            
            export_results = exporter.export_data(
                results['dataframe'],
                download_folder,
                filename_base,
                export_formats
            )
            
            if not export_results['success']:
                error_details = '; '.join(export_results.get('errors', ['Exportação falhou']))
                raise Exception(f"Exportação falhou: {error_details}")
            
            update_progress("Gerando relatórios de completude...", 90)
            
            # Generate processing summary
            processing_summary = processor.get_processing_summary()
            
            # Generate mandatory field report if product groups exist
            mandatory_report = {}
            if has_product_groups:
                try:
                    mandatory_report = processor.get_mandatory_field_report()
                except Exception as e:
                    print(f"⚠️ Warning: Could not generate mandatory field report: {e}")
            
            # Success - store comprehensive results
            config.update({
                'status': 'completed',
                'progress': 100,
                'message': 'Processamento avançado concluído com sucesso!',
                'results': {
                    'stats': results['stats'],
                    'dataframe': results['dataframe'],
                    'download_info': exporter.create_download_info(export_results),
                    'has_product_groups': has_product_groups,
                    'processing_summary': processing_summary,
                    'mandatory_report': mandatory_report
                }
            })
            
            update_progress("Todos os processamentos concluídos!", 100)
            
            # Clean up uploaded file
            try:
                os.remove(config['file_path'])
                print(f"🗑️ Arquivo temporário removido: {config['file_path']}")
            except Exception as e:
                print(f"⚠️ Aviso de limpeza: {str(e)}")
                
        except Exception as e:
            error_msg = f"Erro: {str(e)}"
            print(f"❌ Erro de processamento: {error_msg}")
            print(traceback.format_exc())
            
            config.update({
                'status': 'error',
                'message': error_msg
            })

def analyze_large_sample_enhanced(file_path, obs_column='obs', sample_size=5000):
    """Enhanced quick analysis with product group detection"""
    import pandas as pd
    
    try:
        print(f"🔍 Analisando até {sample_size:,} linhas com detecção de grupos de produto...")
        
        # Read file info first
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                total_rows = sum(1 for _ in f) - 1
        except:
            total_rows = 0
        
        # Read a sample for analysis
        read_size = min(sample_size * 2, total_rows)
        df = pd.read_csv(file_path, nrows=read_size, low_memory=False)
        
        print(f"📖 Lidas {len(df):,} linhas do arquivo")
        
        # Check if obs column exists
        if obs_column not in df.columns:
            return {
                'error': f"Coluna '{obs_column}' não encontrada. Colunas disponíveis: {', '.join(df.columns[:10])}"
            }
        
        # Check for product group column
        has_product_groups = 'product_group' in df.columns
        product_groups_info = {}
        
        if has_product_groups:
            print(f"🏷️ Coluna de grupos de produto encontrada!")
            
            # Analyze product groups in sample
            group_counts = df['product_group'].value_counts()
            print(f"📊 Grupos de produto encontrados na amostra:")
            
            for group, count in group_counts.items():
                if pd.notna(group):
                    group_info = product_group_manager.get_group_info(group)
                    group_name = group_info['name'] if group_info else group
                    mandatory_fields = product_group_manager.get_mandatory_fields(group) if group_info else []
                    
                    print(f"   {group_name}: {count} registros ({len(mandatory_fields)} campos obrigatórios)")
                    
                    product_groups_info[group] = {
                        'name': group_name,
                        'record_count': int(count),
                        'percentage': round((count / len(df)) * 100, 2),
                        'mandatory_fields': mandatory_fields,
                        'mandatory_field_count': len(mandatory_fields)
                    }
        else:
            print(f"⚠️ Nenhuma coluna de grupo de produto encontrada")
        
        # Filter to rows with obs data
        df_obs = df[df[obs_column].notna() & (df[obs_column] != '')].head(sample_size)
        
        if len(df_obs) == 0:
            return {
                'error': f"Nenhum dado válido encontrado na coluna '{obs_column}'"
            }
        
        analysis = {
            'sample_info': {
                'requested_size': sample_size,
                'actual_size': len(df_obs),
                'total_file_rows': total_rows,
                'column_name': obs_column,
                'has_product_groups': has_product_groups,
                'product_groups_info': product_groups_info
            },
            'text_stats': {},
            'noise_analysis': {},
            'field_analysis': {},
            'product_group_analysis': {},
            'recommendations': {}
        }
        
        obs_texts = df_obs[obs_column].astype(str)
        print(f"📊 Analisando {len(obs_texts):,} entradas de texto...")
        
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
        
        # Enhanced pattern analysis
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
        
        # Enhanced field patterns
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
            'ipv6_addresses': 0,
            'wifi_credentials': 0,
            'client_types': 0,
            'technology_ids': 0
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
            'ipv6_addresses': r'[0-9a-fA-F:]+::/\d{1,3}',
            'wifi_credentials': r'(SSID|password)[:\s]*[A-Za-z0-9@#$%^&*()_+-=]+',
            'client_types': r'(RESIDENCIAL|EMPRESARIAL|CORPORATIVO)',
            'technology_ids': r'(GPON|EPON|ETHERNET|MPLS|P2P)'
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
        
        # Product group specific analysis
        if has_product_groups:
            analysis['product_group_analysis'] = analyze_mandatory_field_potential(
                df_obs, obs_column, sample_for_patterns, product_groups_info
            )
        
        # Calculate enhanced recommendations
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
                'memory_usage_warning': total_rows > 25000,
                'product_group_optimization': has_product_groups
            },
            'product_group_recommendations': generate_product_group_recommendations(product_groups_info) if has_product_groups else {}
        }
        
        print(f"✅ Análise avançada concluída com sucesso")
        return analysis
        
    except Exception as e:
        error_msg = f"Análise da amostra falhou: {str(e)}"
        print(f"❌ {error_msg}")
        return {'error': error_msg}

# ===============================
# HELPER FUNCTIONS
# ===============================

def load_original_dataframe(file_path, obs_column):
    """Load original dataframe for comparison"""
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"⚠️ Arquivo original não encontrado: {file_path}")
            return pd.DataFrame()
        
        # Try multiple encodings
        encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                print(f"✅ CSV original carregado com codificação {encoding}")
                return df
            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                print(f"⚠️ Erro com {encoding}: {e}")
                continue
        
        # If all encodings fail, try with python engine
        try:
            df = pd.read_csv(file_path, encoding='utf-8', engine='python', low_memory=False)
            print("✅ CSV original carregado com engine python")
            return df
        except Exception as e:
            print(f"⚠️ Engine python falhou: {e}")
        
        # Final fallback - return empty dataframe
        print("❌ Todos os métodos falharam, retornando dataframe vazio")
        return pd.DataFrame()
        
    except Exception as e:
        print(f"❌ Erro ao carregar dataframe original: {str(e)}")
        return pd.DataFrame()

def analyze_product_group_completeness(df):
    """Analyze product group completeness in processed DataFrame"""
    try:
        if 'product_group' not in df.columns:
            return {'error': 'Nenhuma coluna de grupo de produto encontrada nos dados processados'}
        
        analysis = {}
        
        # Get unique product groups
        unique_groups = df['product_group'].dropna().unique()
        
        for group in unique_groups:
            group_data = df[df['product_group'] == group]
            group_info = product_group_manager.get_group_info(group)
            
            if not group_info:
                continue
            
            # Get mandatory fields for this group
            mandatory_fields = product_group_manager.get_mandatory_fields(group)
            field_mapping = product_group_manager.get_extracted_field_mapping(group)
            
            # Calculate completeness for each mandatory field
            field_completeness = {}
            total_completeness = 0
            
            for business_field in mandatory_fields:
                # Map to extracted field name
                if business_field in field_mapping:
                    extracted_field = field_mapping[business_field].replace('extracted_', '')
                    column_name = f'extracted_{extracted_field}'
                else:
                    column_name = f'extracted_{business_field}'
                
                if column_name in group_data.columns:
                    filled_count = group_data[column_name].notna().sum()
                    total_count = len(group_data)
                    completeness_rate = (filled_count / total_count * 100) if total_count > 0 else 0
                    
                    field_completeness[business_field] = {
                        'completeness_rate': round(completeness_rate, 2),
                        'filled_count': int(filled_count),
                        'total_count': int(total_count),
                        'column_name': column_name,
                        'status': get_completeness_status(completeness_rate)
                    }
                    
                    total_completeness += completeness_rate
                else:
                    field_completeness[business_field] = {
                        'completeness_rate': 0.0,
                        'filled_count': 0,
                        'total_count': len(group_data),
                        'column_name': column_name,
                        'status': 'missing'
                    }
            
            # Calculate overall completeness
            overall_completeness = (total_completeness / len(mandatory_fields)) if mandatory_fields else 0
            
            analysis[group] = {
                'group_name': group_info['name'],
                'total_records': len(group_data),
                'mandatory_fields': mandatory_fields,
                'field_completeness': field_completeness,
                'overall_completeness': round(overall_completeness, 2),
                'overall_status': get_completeness_status(overall_completeness),
                'recommendations': generate_recommendations(field_completeness, overall_completeness)
            }
        
        return analysis
        
    except Exception as e:
        print(f"❌ Erro ao analisar completude dos grupos de produto: {str(e)}")
        return {'error': str(e)}

def analyze_mandatory_field_potential(df_sample, obs_column, text_samples, product_groups_info):
    """Analyze potential for extracting mandatory fields by product group"""
    try:
        group_analysis = {}
        
        for group_key, group_info in product_groups_info.items():
            mandatory_fields = group_info['mandatory_fields']
            field_mapping = product_group_manager.get_extracted_field_mapping(group_key)
            
            # Get sample texts for this product group
            group_data = df_sample[df_sample['product_group'] == group_key] if 'product_group' in df_sample.columns else df_sample
            group_texts = group_data[obs_column].astype(str).head(min(100, len(group_data)))
            
            field_potential = {}
            
            for business_field in mandatory_fields:
                # Map to extraction patterns
                extracted_field = field_mapping.get(business_field, business_field)
                if extracted_field.startswith('extracted_'):
                    extracted_field = extracted_field.replace('extracted_', '')
                
                # Count potential matches in sample texts
                match_count = 0
                total_texts = len(group_texts)
                
                # Use simple heuristics to estimate extraction potential
                pattern = get_field_pattern(business_field)
                
                # Count matches
                try:
                    for text in group_texts:
                        if re.search(pattern, str(text), re.IGNORECASE):
                            match_count += 1
                except:
                    pass
                
                extraction_potential = (match_count / total_texts * 100) if total_texts > 0 else 0
                
                field_potential[business_field] = {
                    'extraction_potential': round(extraction_potential, 2),
                    'sample_matches': match_count,
                    'sample_total': total_texts,
                    'is_mandatory': True,
                    'mapped_field': extracted_field
                }
            
            # Calculate overall potential for this group
            avg_potential = sum([fp['extraction_potential'] for fp in field_potential.values()]) / len(field_potential) if field_potential else 0
            
            group_analysis[group_key] = {
                'group_name': group_info['name'],
                'record_count': group_info['record_count'],
                'mandatory_field_potential': field_potential,
                'overall_extraction_potential': round(avg_potential, 2),
                'completeness_forecast': get_completeness_forecast(avg_potential)
            }
        
        return group_analysis
        
    except Exception as e:
        print(f"❌ Erro ao analisar potencial de campos obrigatórios: {str(e)}")
        return {}

def get_field_pattern(business_field):
    """Get regex pattern for business field"""
    field_patterns = {
        'ip_management': r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
        'ip_cpe': r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
        'vlan': r'VLAN\s*:?\s*\d+',
        'serial_code': r'SN[:\s]*[A-Za-z0-9]+',
        'serial_number': r'SN[:\s]*[A-Za-z0-9]+',
        'asn': r'AS\s*Cliente[:\s]*\d+',
        'provider_id': r'AS\s*Cliente[:\s]*\d+',
        'wifi_ssid': r'SSID[:\s]*[A-Za-z0-9_-]+',
        'wifi_passcode': r'password[:\s]*[A-Za-z0-9@#$%^&*()_+-=]+',
        'wifi_password': r'password[:\s]*[A-Za-z0-9@#$%^&*()_+-=]+',
        'technology_id': r'(GPON|EPON|ETHERNET|MPLS|P2P)',
        'client_type': r'(RESIDENCIAL|EMPRESARIAL|CORPORATIVO)',
        'cpe': r'OLT-[A-Z0-9-]+',
        'model_onu': r'ONU[:\s]*[A-Za-z0-9-]+',
        'gateway': r'GTW[:\s]*[0-9\.]+',
        'interface_1': r'interface\s+[A-Za-z0-9/]+',
        'pop_description': r'br\.[a-z]{2}\.[a-z]{2,}\.[a-z]{2,}\.pe\.\d+',
        'login_pppoe': r'login\s*pppoe[:\s]*[A-Za-z0-9@._-]+',
        'ip_block': r'BLOCO\s*IP[:\s]*[0-9\.]+/\d+',
        'prefixes': r'prefixo[:\s]*[0-9\.]+/\d+'
    }
    
    return field_patterns.get(business_field, rf'{business_field.replace("_", ".*?")}[:\s]*[A-Za-z0-9_-]+')

def get_completeness_status(completeness_rate):
    """Get status based on completeness rate"""
    if completeness_rate >= 95:
        return 'excellent'
    elif completeness_rate >= 80:
        return 'good'
    elif completeness_rate >= 60:
        return 'fair'
    elif completeness_rate >= 30:
        return 'poor'
    else:
        return 'critical'

def get_completeness_forecast(avg_potential):
    """Get forecast based on extraction potential"""
    if avg_potential >= 80:
        return 'excellent'
    elif avg_potential >= 60:
        return 'good'
    elif avg_potential >= 40:
        return 'fair'
    elif avg_potential >= 20:
        return 'poor'
    else:
        return 'critical'

def generate_recommendations(field_completeness, overall_completeness):
    """Generate recommendations based on completeness analysis"""
    recommendations = []
    
    # Overall recommendations
    if overall_completeness < 50:
        recommendations.append({
            'type': 'critical',
            'message': f'Completude geral muito baixa ({overall_completeness:.1f}%). Considere revisar a qualidade dos dados e padrões de extração.'
        })
    elif overall_completeness < 80:
        recommendations.append({
            'type': 'improvement',
            'message': f'Completude geral moderada ({overall_completeness:.1f}%). Foque em melhorar campos críticos.'
        })
    
    # Field-specific recommendations
    critical_fields = [field for field, stats in field_completeness.items() 
                      if stats['completeness_rate'] < 30]
    
    if critical_fields:
        recommendations.append({
            'type': 'critical',
            'message': f'Campos críticos com completude muito baixa: {", ".join(critical_fields)}'
        })
    
    # Missing fields
    missing_fields = [field for field, stats in field_completeness.items() 
                     if stats['status'] == 'missing']
    
    if missing_fields:
        recommendations.append({
            'type': 'missing',
            'message': f'Campos não encontrados nos dados: {", ".join(missing_fields)}. Verifique os padrões de extração.'
        })
    
    # Success cases
    excellent_fields = [field for field, stats in field_completeness.items() 
                       if stats['completeness_rate'] >= 95]
    
    if excellent_fields:
        recommendations.append({
            'type': 'success',
            'message': f'Excelente completude em: {", ".join(excellent_fields)}'
        })
    
    return recommendations

def generate_product_group_recommendations(product_groups_info):
    """Generate recommendations based on product group analysis"""
    recommendations = {}
    
    for group_key, group_info in product_groups_info.items():
        group_recs = []
        
        # Record count recommendations
        if group_info['record_count'] < 10:
            group_recs.append({
                'type': 'warning',
                'message': f"Amostra pequena ({group_info['record_count']} registros). Resultados podem não ser representativos."
            })
        elif group_info['record_count'] > 1000:
            group_recs.append({
                'type': 'info',
                'message': f"Dataset grande ({group_info['record_count']} registros). Considere processamento em lotes."
            })
        
        # Mandatory field recommendations
        field_count = group_info['mandatory_field_count']
        if field_count > 8:
            group_recs.append({
                'type': 'info',
                'message': f"Grupo complexo com {field_count} campos obrigatórios. Foque primeiro nos campos críticos."
            })
        elif field_count < 3:
            group_recs.append({
                'type': 'success',
                'message': f"Grupo simples com {field_count} campos obrigatórios. Alta completude esperada."
            })
        
        recommendations[group_key] = {
            'group_name': group_info['name'],
            'recommendations': group_recs
        }
    
    return recommendations

def simulate_text_cleaning(text):
    """Quick simulation of text cleaning"""
    if not text:
        return text
    
    cleaned = text
    cleaned = re.sub(r'^[-=_~*+#]{5,}.*', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'^#+\s*', '', cleaned, flags=re.MULTILINE)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    cleaned = re.sub(r'[ \t]{3,}', ' ', cleaned)
    cleaned = re.sub(r'^\s+', '', cleaned, flags=re.MULTILINE)
    
    return cleaned.strip()

