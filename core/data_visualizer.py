import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

import io
import base64
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class DataVisualizationAnalyzer:
    """
    Create visual analytics to show before/after data extraction impact
    Focuses on blank fields vs extracted data comparison
    """
    
    def __init__(self):
        # Set up professional styling
        plt.style.use('default')
        sns.set_palette("Set2")
        self.setup_plot_style()
        
    def setup_plot_style(self):
        """Setup professional plot styling"""
        sns.set_style("whitegrid")
        plt.rcParams.update({
            'figure.figsize': (10, 6),
            'font.size': 10,
            'axes.titlesize': 12,
            'axes.labelsize': 10,
            'xtick.labelsize': 8,
            'ytick.labelsize': 8,
            'legend.fontsize': 8,
            'font.family': 'sans-serif',
            'axes.grid': True,
            'grid.alpha': 0.3
        })
    
    def analyze_data_completeness(self, original_df, processed_df, obs_column='obs'):
        """
        Analyze data completeness before and after processing
        """
        analysis = {
            'original_stats': {},
            'processed_stats': {},
            'extraction_impact': {},
            'field_by_field_analysis': {},
            'completeness_metrics': {}
        }
        
        # Original data analysis
        total_rows = len(original_df)
        
        # Handle empty dataframe
        if total_rows == 0:
            print("⚠️  Warning: Original dataframe is empty")
            return self._create_empty_analysis()
        
        original_columns = original_df.columns.tolist()
        
        # Count blank/null fields in original data
        original_blanks = {}
        for col in original_columns:
            try:
                blank_count = (
                    original_df[col].isna().sum() + 
                    (original_df[col] == '').sum() + 
                    (original_df[col] == ' ').sum() +
                    (original_df[col].astype(str).str.strip() == '').sum()
                )
                original_blanks[col] = {
                    'blank_count': int(blank_count),
                    'filled_count': int(total_rows - blank_count),
                    'completeness_percent': round((total_rows - blank_count) / total_rows * 100, 2) if total_rows > 0 else 0
                }
            except Exception as e:
                print(f"⚠️  Warning: Error processing column {col}: {e}")
                original_blanks[col] = {
                    'blank_count': total_rows,
                    'filled_count': 0,
                    'completeness_percent': 0
                }
        
        # Calculate overall completeness safely
        total_filled = sum([stats['filled_count'] for stats in original_blanks.values()])
        total_cells = total_rows * len(original_columns)
        overall_completeness = round(total_filled / total_cells * 100, 2) if total_cells > 0 else 0
        
        analysis['original_stats'] = {
            'total_rows': total_rows,
            'total_columns': len(original_columns),
            'blank_analysis': original_blanks,
            'overall_completeness': overall_completeness
        }
        
        # Processed data analysis
        processed_columns = processed_df.columns.tolist()
        extracted_columns = [col for col in processed_columns if col.startswith('extracted_')]
        
        # Count extracted vs blank fields
        processed_blanks = {}
        extraction_success = {}
        
        for col in processed_columns:
            try:
                blank_count = (
                    processed_df[col].isna().sum() + 
                    (processed_df[col] == '').sum() + 
                    (processed_df[col] == ' ').sum() +
                    (processed_df[col].astype(str).str.strip() == '').sum()
                )
                processed_blanks[col] = {
                    'blank_count': int(blank_count),
                    'filled_count': int(total_rows - blank_count),
                    'completeness_percent': round((total_rows - blank_count) / total_rows * 100, 2) if total_rows > 0 else 0
                }
                
                # Special analysis for extracted fields
                if col.startswith('extracted_'):
                    field_name = col.replace('extracted_', '')
                    extraction_success[field_name] = {
                        'extracted_count': int(total_rows - blank_count),
                        'extraction_rate': round((total_rows - blank_count) / total_rows * 100, 2) if total_rows > 0 else 0,
                        'blank_count': int(blank_count)
                    }
            except Exception as e:
                print(f"⚠️  Warning: Error processing column {col}: {e}")
                processed_blanks[col] = {
                    'blank_count': total_rows,
                    'filled_count': 0,
                    'completeness_percent': 0
                }
        
        # Calculate processed completeness safely
        processed_filled = sum([stats['filled_count'] for stats in processed_blanks.values()])
        processed_total_cells = total_rows * len(processed_columns)
        processed_completeness = round(processed_filled / processed_total_cells * 100, 2) if processed_total_cells > 0 else 0
        
        analysis['processed_stats'] = {
            'total_rows': total_rows,
            'total_columns': len(processed_columns),
            'extracted_columns': len(extracted_columns),
            'blank_analysis': processed_blanks,
            'extraction_success': extraction_success,
            'overall_completeness': processed_completeness
        }
        
        # Calculate extraction impact safely
        original_filled_cells = sum([stats['filled_count'] for stats in original_blanks.values()])
        processed_filled_cells = sum([stats['filled_count'] for stats in processed_blanks.values()])
        
        new_data_points = processed_filled_cells - original_filled_cells
        improvement_percent = round(
            ((processed_filled_cells - original_filled_cells) / original_filled_cells) * 100, 2
        ) if original_filled_cells > 0 else 0
        
        data_density_improvement = 0
        if total_rows > 0 and len(original_columns) > 0 and len(processed_columns) > 0:
            original_density = original_filled_cells / (total_rows * len(original_columns))
            processed_density = processed_filled_cells / (total_rows * len(processed_columns))
            data_density_improvement = round((processed_density - original_density) * 100, 4)
        
        analysis['extraction_impact'] = {
            'original_filled_cells': original_filled_cells,
            'processed_filled_cells': processed_filled_cells,
            'new_data_points': new_data_points,
            'improvement_percent': improvement_percent,
            'data_density_improvement': data_density_improvement
        }
    
        return analysis
    
    def create_completeness_comparison_chart(self, analysis):
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))
        
        # Before - Original Data Completeness
        original_stats = analysis['original_stats']
        total_original_cells = original_stats['total_rows'] * original_stats['total_columns']
        original_filled = sum([stats['filled_count'] for stats in original_stats['blank_analysis'].values()])
        original_blank = total_original_cells - original_filled
        
        # After - Processed Data Completeness
        processed_stats = analysis['processed_stats']
        total_processed_cells = processed_stats['total_rows'] * processed_stats['total_columns']
        processed_filled = sum([stats['filled_count'] for stats in processed_stats['blank_analysis'].values()])
        processed_blank = total_processed_cells - processed_filled
        
        # Create pie charts
        colors_before = ['#ff6b6b', '#e0e0e0']
        colors_after = ['#2ed573', '#b8e6b8']
        
        # Before pie chart
        before_data = [original_blank, original_filled]
        before_labels = [f'Campos Vazios\n({original_blank:,})', f'Dados Originais\n({original_filled:,})']
        
        wedges1, texts1, autotexts1 = ax1.pie(before_data, labels=before_labels, autopct='%1.1f%%', 
                                              colors=colors_before, startangle=90, textprops={'fontsize': 10})
        ax1.set_title('ANTES: Dados Originais\n(CSV Importado)', fontsize=14, fontweight='bold', 
                     color='#d63031', pad=20)
        
        # After pie chart
        after_data = [processed_blank, processed_filled]
        after_labels = [f'Campos Vazios\n({processed_blank:,})', f'Dados Extraídos\n({processed_filled:,})']
        
        wedges2, texts2, autotexts2 = ax2.pie(after_data, labels=after_labels, autopct='%1.1f%%', 
                                              colors=colors_after, startangle=90, textprops={'fontsize': 10})
        ax2.set_title('DEPOIS: Dados Processados\n(Bibliotecario)', fontsize=14, fontweight='bold', 
                     color='#00b894', pad=20)
        
        # Add improvement annotation
        improvement = analysis['extraction_impact']['improvement_percent']
        fig.suptitle(f'Comparação de Completude dos Dados\nMelhoria: +{improvement}% mais dados extraídos', 
                    fontsize=16, fontweight='bold', y=0.95)
        
        plt.tight_layout()
        return self.fig_to_base64(fig)
    
    def create_field_extraction_comparison(self, analysis):
        try:
            extraction_data = analysis['processed_stats']['extraction_success']
            
            if not extraction_data or len(extraction_data) == 0:
                print("⚠️  Warning: No extraction data available")
                return None
            
            # Prepare data for visualization
            field_names = list(extraction_data.keys())
            extraction_rates = [data['extraction_rate'] for data in extraction_data.values()]
            extracted_counts = [data['extracted_count'] for data in extraction_data.values()]
            
            # Filter out fields with no extractions for cleaner visualization
            non_zero_data = [(name, rate, count) for name, rate, count in 
                            zip(field_names, extraction_rates, extracted_counts) if count > 0]
            
            if not non_zero_data or len(non_zero_data) == 0:
                print("⚠️  Warning: No successful extractions found")
                return None
            
            field_names_filtered, extraction_rates_filtered, extracted_counts_filtered = zip(*non_zero_data)
            
            # Check if we have valid data
            if len(field_names_filtered) == 0 or max(extraction_rates_filtered) == 0:
                print("⚠️  Warning: No valid extraction rates")
                return None
            
            # Create horizontal bar chart
            fig, ax = plt.subplots(figsize=(6, max(3, len(field_names_filtered) * 0.3)))
            
            # Create color gradient based on extraction rate - safe version
            max_rate = max(extraction_rates_filtered) if extraction_rates_filtered else 1
            normalized_rates = [rate/max_rate for rate in extraction_rates_filtered]
            colors = plt.cm.RdYlGn(normalized_rates)
            
            bars = ax.barh(field_names_filtered, extraction_rates_filtered, color=colors)
            
            # Customize chart
            ax.set_xlabel('Taxa de Extração (%)', fontsize=12, fontweight='bold')
            ax.set_title('Taxa de Extração por Campo\n(Porcentagem de registros com dados extraídos)', 
                        fontsize=14, fontweight='bold', pad=20)
            ax.set_xlim(0, max(100, max(extraction_rates_filtered) * 1.1))
            
            # Add value labels on bars
            for i, (bar, count) in enumerate(zip(bars, extracted_counts_filtered)):
                width = bar.get_width()
                ax.text(width + max(extraction_rates_filtered) * 0.01, bar.get_y() + bar.get_height()/2, 
                    f'{width:.1f}% ({count:,})', 
                    ha='left', va='center', fontweight='bold', fontsize=10)
            
            # Add grid for better readability
            ax.grid(True, alpha=0.3, axis='x')
            ax.set_axisbelow(True)
            
            plt.tight_layout()
            return self.fig_to_base64(fig)
        except Exception as e:
            print(f"⚠️  Warning: Could not create field extraction comparison chart: {e}")
            return None
        
    def create_data_density_heatmap(self, original_df, processed_df, sample_size=1000):
        """Create heatmap showing data density before and after"""
        try:
            # Check if dataframes are empty
            if original_df.empty or processed_df.empty:
                print("⚠️  Warning: One or both dataframes are empty, skipping heatmap")
                return None
            
            # Check if we have enough data
            if len(original_df) < 2:
                print("⚠️  Warning: Not enough data for heatmap")
                return None
            
            # Sample data if too large
            if len(original_df) > sample_size:
                sample_indices = np.random.choice(len(original_df), min(sample_size, len(original_df)), replace=False)
                original_sample = original_df.iloc[sample_indices]
                processed_sample = processed_df.iloc[sample_indices]
            else:
                original_sample = original_df.copy()
                processed_sample = processed_df.copy()
            
            # Check if samples are valid
            if original_sample.empty or len(original_sample.columns) == 0:
                print("⚠️  Warning: Original sample is empty")
                return None
            
            fig, axes = plt.subplots(2, 1, figsize=(12, 8))
            ax1, ax2 = axes
            
            # Original data heatmap
            try:
                # Convert to string first to handle mixed types
                original_sample_str = original_sample.astype(str)
                original_mask = (original_sample_str == 'nan') | (original_sample_str == '') | (original_sample_str == ' ') | (original_sample_str == 'None')
                
                # Convert to numeric (1 for data, 0 for missing)
                original_numeric = (~original_mask).astype(int)
                
                # Only create heatmap if we have data
                if original_numeric.size > 0 and original_numeric.shape[0] > 0 and original_numeric.shape[1] > 0:
                    sns.heatmap(original_numeric.T, cmap='RdYlGn', cbar_kws={'label': 'Dados Presentes'},
                            ax=ax1, xticklabels=False, yticklabels=True)
                    ax1.set_title('ANTES: Densidade dos Dados Originais\n(Verde = Dados, Vermelho = Vazio)', 
                                fontsize=12, fontweight='bold')
                    ax1.set_ylabel('Colunas Originais', fontsize=10)
                else:
                    ax1.text(0.5, 0.5, 'Dados Originais\nInsuficientes', ha='center', va='center', 
                            transform=ax1.transAxes, fontsize=14)
            except Exception as e:
                print(f"⚠️  Warning: Error creating original heatmap: {e}")
                ax1.text(0.5, 0.5, 'Erro nos Dados\nOriginais', ha='center', va='center', 
                        transform=ax1.transAxes, fontsize=14)
            
            # Processed data heatmap (focus on extracted fields)
            try:
                extracted_cols = [col for col in processed_sample.columns if col.startswith('extracted_')]
                
                if extracted_cols and len(extracted_cols) > 0:
                    processed_extracted = processed_sample[extracted_cols]
                    
                    # Convert to string first
                    processed_extracted_str = processed_extracted.astype(str)
                    processed_mask = (processed_extracted_str == 'nan') | (processed_extracted_str == '') | (processed_extracted_str == ' ') | (processed_extracted_str == 'None')
                    processed_numeric = (~processed_mask).astype(int)
                    
                    # Only create heatmap if we have data
                    if processed_numeric.size > 0 and processed_numeric.shape[0] > 0 and processed_numeric.shape[1] > 0:
                        sns.heatmap(processed_numeric.T, cmap='RdYlGn', cbar_kws={'label': 'Dados Extraídos'},
                                ax=ax2, xticklabels=False, yticklabels=True)
                        ax2.set_title('DEPOIS: Densidade dos Dados Extraídos\n(Verde = Extraído, Vermelho = Não Encontrado)', 
                                    fontsize=12, fontweight='bold')
                        ax2.set_ylabel('Campos Extraídos', fontsize=10)
                        ax2.set_xlabel(f'Registros (Amostra de {len(original_sample)})', fontsize=10)
                    else:
                        ax2.text(0.5, 0.5, 'Nenhum Campo\nExtraído', ha='center', va='center', 
                                transform=ax2.transAxes, fontsize=14)
                else:
                    ax2.text(0.5, 0.5, 'Nenhum Campo\nExtraído', ha='center', va='center', 
                            transform=ax2.transAxes, fontsize=14)
                    
            except Exception as e:
                print(f"⚠️  Warning: Error creating processed heatmap: {e}")
                ax2.text(0.5, 0.5, 'Erro nos Dados\nProcessados', ha='center', va='center', 
                        transform=ax2.transAxes, fontsize=14)
            
            plt.tight_layout()
            return self.fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️  Warning: Could not create heatmap: {e}")
            return None
    
    def create_extraction_summary_metrics(self, analysis):
        """Create summary metrics visualization"""
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(12, 8))
        
        # 1. Data Volume Comparison
        original_filled = analysis['original_stats']['total_rows'] * analysis['original_stats']['total_columns']
        processed_filled = analysis['processed_stats']['total_rows'] * analysis['processed_stats']['total_columns']
        
        volumes = [analysis['extraction_impact']['original_filled_cells'], 
                  analysis['extraction_impact']['processed_filled_cells']]
        labels = ['Dados Originais', 'Dados Processados']
        colors = ['#3498db', '#2ecc71']
        
        bars1 = ax1.bar(labels, volumes, color=colors, alpha=0.8)
        ax1.set_title('Volume Total de Dados\n(Células Preenchidas)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Número de Células com Dados')
        
        # Add value labels
        for bar, volume in zip(bars1, volumes):
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{volume:,}', ha='center', va='bottom', fontweight='bold')
        
        # Add improvement annotation
        improvement = analysis['extraction_impact']['new_data_points']
        ax1.text(0.5, max(volumes) * 0.8, f'+{improvement:,}\nNovos Dados', 
                ha='center', va='center', fontsize=14, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", facecolor='yellow', alpha=0.7))
        
        # 2. Completeness Percentage
        original_completeness = analysis['original_stats']['overall_completeness']
        processed_completeness = analysis['processed_stats']['overall_completeness']
        
        completeness = [original_completeness, processed_completeness]
        
        bars2 = ax2.bar(labels, completeness, color=colors, alpha=0.8)
        ax2.set_title('Taxa de Completude Geral\n(% de Células Preenchidas)', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Porcentagem (%)')
        ax2.set_ylim(0, 100)
        
        # Add value labels
        for bar, comp in zip(bars2, completeness):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 2,
                    f'{comp:.1f}%', ha='center', va='bottom', fontweight='bold')
        
        # 3. Field Count Comparison
        original_fields = analysis['original_stats']['total_columns']
        extracted_fields = analysis['processed_stats']['extracted_columns']
        total_processed_fields = analysis['processed_stats']['total_columns']
        
        field_counts = [original_fields, extracted_fields, total_processed_fields]
        field_labels = ['Campos\nOriginais', 'Campos\nExtraídos', 'Total\nProcessados']
        field_colors = ['#e74c3c', '#f39c12', '#2ecc71']
        
        bars3 = ax3.bar(field_labels, field_counts, color=field_colors, alpha=0.8)
        ax3.set_title('Comparação de Campos\n(Número de Colunas)', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Número de Campos')
        
        # Add value labels
        for bar, count in zip(bars3, field_counts):
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{count}', ha='center', va='bottom', fontweight='bold')
        
        # 4. Top Extracted Fields
        extraction_success = analysis['processed_stats']['extraction_success']
        if extraction_success:
            # Get top 8 most successful extractions
            top_extractions = sorted(extraction_success.items(), 
                                   key=lambda x: x[1]['extracted_count'], reverse=True)[:8]
            
            if top_extractions:
                field_names = [item[0].replace('_', ' ').title() for item, _ in zip(*top_extractions)]
                extraction_counts = [data['extracted_count'] for _, data in top_extractions]
                
                bars4 = ax4.barh(field_names, extraction_counts, color='#9b59b6', alpha=0.8)
                ax4.set_title('Top Campos Extraídos\n(Número de Extrações)', fontsize=12, fontweight='bold')
                ax4.set_xlabel('Número de Registros Extraídos')
                
                # Add value labels
                for bar, count in zip(bars4, extraction_counts):
                    width = bar.get_width()
                    ax4.text(width + max(extraction_counts)*0.01, bar.get_y() + bar.get_height()/2,
                            f'{count:,}', ha='left', va='center', fontweight='bold', fontsize=9)
        
        plt.tight_layout()
        return self.fig_to_base64(fig)
    
    def create_interactive_extraction_dashboard(self, analysis):
        """Create interactive Plotly dashboard"""
        extraction_data = analysis['processed_stats']['extraction_success']
        
        if not extraction_data:
            return None
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Taxa de Extração por Campo', 'Volume de Dados Extraídos',
                          'Comparação Antes/Depois', 'Distribuição de Completude'),
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "pie"}, {"type": "histogram"}]]
        )
        
        # 1. Extraction Rate by Field
        field_names = list(extraction_data.keys())
        extraction_rates = [data['extraction_rate'] for data in extraction_data.values()]
        
        fig.add_trace(
            go.Bar(x=field_names, y=extraction_rates, 
                  name='Taxa de Extração (%)',
                  marker_color='lightblue',
                  text=[f'{rate:.1f}%' for rate in extraction_rates],
                  textposition='outside'),
            row=1, col=1
        )
        
        # 2. Volume of Extracted Data
        extracted_counts = [data['extracted_count'] for data in extraction_data.values()]
        
        fig.add_trace(
            go.Bar(x=field_names, y=extracted_counts,
                  name='Registros Extraídos',
                  marker_color='lightgreen',
                  text=[f'{count:,}' for count in extracted_counts],
                  textposition='outside'),
            row=1, col=2
        )
        
        # 3. Before/After Comparison
        comparison_data = [
            analysis['extraction_impact']['original_filled_cells'],
            analysis['extraction_impact']['processed_filled_cells']
        ]
        comparison_labels = ['Dados Originais', 'Dados Processados']
        
        fig.add_trace(
            go.Pie(labels=comparison_labels, values=comparison_data,
                  name="Comparação",
                  marker_colors=['lightcoral', 'lightgreen']),
            row=2, col=1
        )
        
        # 4. Completeness Distribution
        completeness_values = [data['extraction_rate'] for data in extraction_data.values() if data['extraction_rate'] > 0]
        
        fig.add_trace(
            go.Histogram(x=completeness_values,
                        name='Distribuição de Completude',
                        marker_color='lightpurple',
                        nbinsx=10),
            row=2, col=2
        )
        
        # Update layout
        fig.update_layout(
            title_text="Dashboard de Análise de Extração de Dados",
            title_x=0.5,
            showlegend=False,
            height=800
        )
        
        # Convert to HTML
        return fig.to_html(include_plotlyjs='cdn')
    
    def fig_to_base64(self, fig):
        """Convert matplotlib figure to base64 string"""
        img_buffer = io.BytesIO()
        fig.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        img_buffer.seek(0)
        img_string = base64.b64encode(img_buffer.read()).decode()
        plt.close(fig)
        return f"data:image/png;base64,{img_string}"
    
    def generate_comprehensive_report(self, original_df, processed_df, obs_column='obs'):
        """Generate comprehensive visual report"""
        print("🎨 Generating visual analytics report...")
        
        # Check if we have valid dataframes
        if original_df.empty:
            print("⚠️  Warning: Original dataframe is empty")
            original_df = pd.DataFrame({'dummy': [0]})  # Create dummy dataframe
        
        if processed_df.empty:
            print("⚠️  Warning: Processed dataframe is empty")
            return {
                'analysis_data': self._create_empty_analysis(),
                'visualizations': {
                    'completeness_comparison': None,
                    'field_extraction_comparison': None,
                    'data_density_heatmap': None,
                    'summary_metrics': None,
                    'interactive_dashboard': None
                },
                'summary_insights': [],
                'generated_at': datetime.now().isoformat()
            }
        
        try:
            # Analyze data completeness
            analysis = self.analyze_data_completeness(original_df, processed_df, obs_column)
            
            # Generate visualizations safely
            visualizations = {}
            
            try:
                visualizations['completeness_comparison'] = self.create_completeness_comparison_chart(analysis)
            except Exception as e:
                print(f"⚠️  Warning: Could not create completeness chart: {e}")
                visualizations['completeness_comparison'] = None
            
            try:
                visualizations['field_extraction_comparison'] = self.create_field_extraction_comparison(analysis)
            except Exception as e:
                print(f"⚠️  Warning: Could not create field extraction chart: {e}")
                visualizations['field_extraction_comparison'] = None
            
            try:
                visualizations['data_density_heatmap'] = self.create_data_density_heatmap(original_df, processed_df)
            except Exception as e:
                print(f"⚠️  Warning: Could not create heatmap: {e}")
                visualizations['data_density_heatmap'] = None
            
            try:
                visualizations['summary_metrics'] = self.create_extraction_summary_metrics(analysis)
            except Exception as e:
                print(f"⚠️  Warning: Could not create summary metrics: {e}")
                visualizations['summary_metrics'] = None
            
            visualizations['interactive_dashboard'] = None  # Temporarily disabled
            
            # Compile report data
            report = {
                'analysis_data': analysis,
                'visualizations': visualizations,
                'summary_insights': self.generate_insights(analysis),
                'generated_at': datetime.now().isoformat()
            }
            
            print("✅ Visual analytics report generated successfully!")
            return report
            
        except Exception as e:
            print(f"❌ Error generating comprehensive report: {e}")
            import traceback
            print(traceback.format_exc())
            
            # Return minimal report on error
            return {
                'analysis_data': self._create_empty_analysis(),
                'visualizations': {
                    'completeness_comparison': None,
                    'field_extraction_comparison': None,
                    'data_density_heatmap': None,
                    'summary_metrics': None,
                    'interactive_dashboard': None
                },
                'summary_insights': [],
                'generated_at': datetime.now().isoformat()
            }
    
    def generate_insights(self, analysis):
        """Generate key insights from the analysis"""
        insights = []
        
        # Data volume improvement
        improvement_percent = analysis['extraction_impact']['improvement_percent']
        new_data_points = analysis['extraction_impact']['new_data_points']
        
        insights.append({
            'type': 'improvement',
            'title': 'Melhoria no Volume de Dados',
            'description': f'O processamento resultou em {improvement_percent:.1f}% mais dados, '
                          f'adicionando {new_data_points:,} novos pontos de dados.',
            'metric': f'+{improvement_percent:.1f}%'
        })
        
        # Completeness improvement
        original_completeness = analysis['original_stats']['overall_completeness']
        processed_completeness = analysis['processed_stats']['overall_completeness']
        completeness_gain = processed_completeness - original_completeness
        
        insights.append({
            'type': 'completeness',
            'title': 'Aumento da Completude',
            'description': f'A completude geral dos dados aumentou de {original_completeness:.1f}% '
                          f'para {processed_completeness:.1f}%, um ganho de {completeness_gain:.1f}%.',
            'metric': f'+{completeness_gain:.1f}%'
        })
        
        # Field extraction success
        extraction_success = analysis['processed_stats']['extraction_success']
        successful_fields = len([f for f in extraction_success.values() if f['extraction_rate'] > 10])
        total_fields = len(extraction_success)
        
        insights.append({
            'type': 'extraction',
            'title': 'Sucesso na Extração de Campos',
            'description': f'{successful_fields} de {total_fields} campos foram extraídos com sucesso '
                          f'(>10% de taxa de extração).',
            'metric': f'{successful_fields}/{total_fields}'
        })
        
        # Best performing field
        if extraction_success:
            best_field = max(extraction_success.items(), key=lambda x: x[1]['extraction_rate'])
            insights.append({
                'type': 'best_field',
                'title': 'Campo Mais Bem Extraído',
                'description': f'O campo "{best_field[0].replace("_", " ").title()}" teve a melhor '
                              f'taxa de extração com {best_field[1]["extraction_rate"]:.1f}% '
                              f'({best_field[1]["extracted_count"]:,} registros).',
                'metric': f'{best_field[1]["extraction_rate"]:.1f}%'
            })
        
        return insights

    def _create_empty_analysis(self):
        """Create empty analysis for edge cases"""
        return {
            'original_stats': {
                'total_rows': 0,
                'total_columns': 0,
                'blank_analysis': {},
                'overall_completeness': 0
            },
            'processed_stats': {
                'total_rows': 0,
                'total_columns': 0,
                'extracted_columns': 0,
                'blank_analysis': {},
                'extraction_success': {},
                'overall_completeness': 0
            },
            'extraction_impact': {
                'original_filled_cells': 0,
                'processed_filled_cells': 0,
                'new_data_points': 0,
                'improvement_percent': 0,
                'data_density_improvement': 0
            },
            'field_by_field_analysis': {},
            'completeness_metrics': {}
        }
            
    def create_completeness_comparison_chart(self, analysis):
        """Create before/after data completeness comparison"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 4))
        
        # Before - Original Data Completeness
        original_stats = analysis['original_stats']
        total_original_cells = max(1, original_stats['total_rows'] * original_stats['total_columns'])  # Avoid division by zero
        original_filled = sum([stats['filled_count'] for stats in original_stats['blank_analysis'].values()])
        original_blank = max(0, total_original_cells - original_filled)
        
        # After - Processed Data Completeness
        processed_stats = analysis['processed_stats']
        total_processed_cells = max(1, processed_stats['total_rows'] * processed_stats['total_columns'])  # Avoid division by zero
        processed_filled = sum([stats['filled_count'] for stats in processed_stats['blank_analysis'].values()])
        processed_blank = max(0, total_processed_cells - processed_filled)
        
        # Create pie charts - handle zero values
        colors_before = ['#ff6b6b', '#e0e0e0']
        colors_after = ['#2ed573', '#b8e6b8']
        
        # Before pie chart
        before_data = [max(1, original_blank), max(1, original_filled)]  # Ensure no zero values
        before_labels = [f'Campos Vazios\n({original_blank:,})', f'Dados Originais\n({original_filled:,})']
        
        # Only create pie chart if we have meaningful data
        if original_blank > 0 or original_filled > 0:
            wedges1, texts1, autotexts1 = ax1.pie(before_data, labels=before_labels, autopct='%1.1f%%', 
                                                colors=colors_before, startangle=90, textprops={'fontsize': 10})
        else:
            ax1.text(0.5, 0.5, 'Sem Dados\nOriginais', ha='center', va='center', transform=ax1.transAxes, fontsize=14)
        
        ax1.set_title('ANTES: Dados Originais\n(CSV Importado)', fontsize=14, fontweight='bold', 
                    color='#d63031', pad=20)
        
        # After pie chart
        after_data = [max(1, processed_blank), max(1, processed_filled)]  # Ensure no zero values
        after_labels = [f'Campos Vazios\n({processed_blank:,})', f'Dados Extraídos\n({processed_filled:,})']
        
        if processed_blank > 0 or processed_filled > 0:
            wedges2, texts2, autotexts2 = ax2.pie(after_data, labels=after_labels, autopct='%1.1f%%', 
                                                colors=colors_after, startangle=90, textprops={'fontsize': 10})
        else:
            ax2.text(0.5, 0.5, 'Sem Dados\nProcessados', ha='center', va='center', transform=ax2.transAxes, fontsize=14)
        
        ax2.set_title('DEPOIS: Dados Processados\n(Bibliotecario)', fontsize=14, fontweight='bold', 
                    color='#00b894', pad=20)
        
        # Add improvement annotation
        improvement = analysis['extraction_impact']['improvement_percent']
        fig.suptitle(f'Comparação de Completude dos Dados\nMelhoria: +{improvement}% mais dados extraídos', 
                    fontsize=16, fontweight='bold', y=0.95)
        
        plt.tight_layout()
        return self.fig_to_base64(fig)    


# Integration helper function
def create_data_visualization_report(original_df, processed_df, obs_column='obs'):
    """
    Helper function to create visualization report
    Can be called from routes.py
    """
    visualizer = DataVisualizationAnalyzer()
    return visualizer.generate_comprehensive_report(original_df, processed_df, obs_column)