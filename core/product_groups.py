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

# Import the existing product group manager
from core.product_groups import ProductGroupManager

class GroupBasedDataVisualizer:
    """
    Enhanced data visualizer that considers product groups and mandatory fields
    Creates separate visualizations for each product group to remove outliers
    """
    
    def __init__(self):
        self.group_manager = ProductGroupManager()
        self.setup_plot_style()
        
    def setup_plot_style(self):
        """Setup professional plot styling"""
        sns.set_style("whitegrid")
        plt.rcParams.update({
            'figure.figsize': (12, 8),
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
    
    def analyze_group_completeness_enhanced(self, processed_df, group_column='product_group'):
        """
        Enhanced analysis focusing ONLY on mandatory fields for each group
        This removes outliers by filtering non-mandatory fields
        """
        if group_column not in processed_df.columns:
            print(f"⚠️  Column '{group_column}' not found. Available columns: {list(processed_df.columns)}")
            return {}
        
        # Get unique groups in the data
        unique_groups = processed_df[group_column].unique()
        valid_groups = [g for g in unique_groups if g in self.group_manager.get_all_groups()]
        
        print(f"📊 Found groups in data: {unique_groups}")
        print(f"✅ Valid groups for analysis: {valid_groups}")
        
        group_analysis = {}
        
        for group_key in valid_groups:
            # Filter data for this specific group
            group_data = processed_df[processed_df[group_column] == group_key].copy()
            
            if len(group_data) == 0:
                continue
            
            # Get ONLY mandatory fields for this group
            mandatory_fields = self.group_manager.get_mandatory_fields(group_key)
            field_mapping = self.group_manager.get_extracted_field_mapping(group_key)
            
            print(f"🔍 Group '{group_key}': {len(mandatory_fields)} mandatory fields")
            
            # Calculate completeness ONLY for mandatory fields
            completeness_stats = {}
            total_mandatory_filled = 0
            total_mandatory_possible = len(mandatory_fields) * len(group_data)
            
            for business_field in mandatory_fields:
                extracted_field = field_mapping.get(business_field, f"extracted_{business_field}")
                
                if extracted_field in group_data.columns:
                    total_records = len(group_data)
                    # Count non-null, non-empty values
                    filled_records = group_data[extracted_field].notna().sum()
                    filled_records += (group_data[extracted_field].astype(str).str.strip() != '').sum()
                    filled_records = min(filled_records, total_records)  # Cap at total records
                    
                    completeness_rate = (filled_records / total_records * 100) if total_records > 0 else 0
                    total_mandatory_filled += filled_records
                    
                    completeness_stats[business_field] = {
                        'extracted_field': extracted_field,
                        'total_records': total_records,
                        'filled_records': int(filled_records),
                        'completeness_rate': round(completeness_rate, 2),
                        'is_mandatory': True,
                        'business_critical': True
                    }
                else:
                    completeness_stats[business_field] = {
                        'extracted_field': extracted_field,
                        'total_records': len(group_data),
                        'filled_records': 0,
                        'completeness_rate': 0.0,
                        'is_mandatory': True,
                        'field_missing': True,
                        'business_critical': True
                    }
            
            # Calculate overall completeness for mandatory fields only
            overall_completeness = (total_mandatory_filled / total_mandatory_possible * 100) if total_mandatory_possible > 0 else 0
            
            group_analysis[group_key] = {
                'name': self.group_manager.get_group_display_name(group_key),
                'group_key': group_key,
                'total_records': len(group_data),
                'mandatory_fields': mandatory_fields,
                'mandatory_field_count': len(mandatory_fields),
                'completeness_stats': completeness_stats,
                'overall_completeness': round(overall_completeness, 2),
                'total_mandatory_filled': total_mandatory_filled,
                'total_mandatory_possible': total_mandatory_possible,
                'data_quality_score': self._calculate_data_quality_score(completeness_stats)
            }
        
        return group_analysis
    
    def _calculate_data_quality_score(self, completeness_stats):
        """Calculate a data quality score based on mandatory field completeness"""
        if not completeness_stats:
            return 0
        
        rates = [stat['completeness_rate'] for stat in completeness_stats.values()]
        avg_rate = sum(rates) / len(rates)
        
        # Penalize missing fields more heavily
        missing_fields = len([stat for stat in completeness_stats.values() if stat.get('field_missing', False)])
        total_fields = len(completeness_stats)
        
        penalty = (missing_fields / total_fields) * 30  # Up to 30% penalty
        
        return max(0, round(avg_rate - penalty, 2))
    
    def create_group_overview_dashboard(self, group_analysis):
        """Create comprehensive group overview dashboard"""
        if not group_analysis:
            return None
        
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
        
        # 1. Group Completeness Overview (Top Left)
        group_names = [info['name'][:20] + '...' if len(info['name']) > 20 else info['name'] 
                      for info in group_analysis.values()]
        completeness_rates = [info['overall_completeness'] for info in group_analysis.values()]
        record_counts = [info['total_records'] for info in group_analysis.values()]
        
        colors = plt.cm.RdYlGn([rate/100 for rate in completeness_rates])
        bars1 = ax1.barh(group_names, completeness_rates, color=colors)
        ax1.set_xlabel('Taxa de Completude - Campos Obrigatórios (%)', fontweight='bold')
        ax1.set_title('Completude por Grupo de Produto\n(Apenas Campos Obrigatórios)', fontweight='bold', fontsize=14)
        ax1.set_xlim(0, 100)
        
        # Add value labels
        for bar, rate in zip(bars1, completeness_rates):
            width = bar.get_width()
            ax1.text(width + 1, bar.get_y() + bar.get_height()/2, 
                    f'{rate:.1f}%', ha='left', va='center', fontweight='bold')
        
        # 2. Records vs Completeness Scatter (Top Right)
        data_quality_scores = [info['data_quality_score'] for info in group_analysis.values()]
        scatter = ax2.scatter(record_counts, completeness_rates, 
                            s=np.array(data_quality_scores)*5 + 50,  # Size based on quality score
                            c=completeness_rates, cmap='RdYlGn', alpha=0.7, edgecolors='black')
        
        ax2.set_xlabel('Número de Registros por Grupo', fontweight='bold')
        ax2.set_ylabel('Taxa de Completude (%)', fontweight='bold')
        ax2.set_title('Volume vs Qualidade por Grupo', fontweight='bold', fontsize=14)
        ax2.grid(True, alpha=0.3)
        
        # Add group labels to scatter points
        for i, (name, count, rate) in enumerate(zip(group_names, record_counts, completeness_rates)):
            ax2.annotate(f'{i+1}', (count, rate), xytext=(5, 5), 
                        textcoords='offset points', fontsize=10, fontweight='bold')
        
        plt.colorbar(scatter, ax=ax2, label='Taxa de Completude (%)')
        
        # 3. Mandatory Fields Distribution (Bottom Left)
        field_counts = [info['mandatory_field_count'] for info in group_analysis.values()]
        ax3.bar(range(len(group_names)), field_counts, color='steelblue', alpha=0.8)
        ax3.set_xlabel('Grupos de Produto', fontweight='bold')
        ax3.set_ylabel('Número de Campos Obrigatórios', fontweight='bold')
        ax3.set_title('Campos Obrigatórios por Grupo', fontweight='bold', fontsize=14)
        ax3.set_xticks(range(len(group_names)))
        ax3.set_xticklabels([f'G{i+1}' for i in range(len(group_names))], rotation=45)
        
        # Add value labels
        for i, count in enumerate(field_counts):
            ax3.text(i, count + 0.1, str(count), ha='center', va='bottom', fontweight='bold')
        
        # 4. Data Quality Score Distribution (Bottom Right)
        quality_colors = ['#d63031' if score < 50 else '#fdcb6e' if score < 75 else '#00b894' 
                         for score in data_quality_scores]
        bars4 = ax4.bar(range(len(group_names)), data_quality_scores, color=quality_colors, alpha=0.8)
        ax4.set_xlabel('Grupos de Produto', fontweight='bold')
        ax4.set_ylabel('Score de Qualidade dos Dados', fontweight='bold')
        ax4.set_title('Qualidade dos Dados por Grupo', fontweight='bold', fontsize=14)
        ax4.set_xticks(range(len(group_names)))
        ax4.set_xticklabels([f'G{i+1}' for i in range(len(group_names))], rotation=45)
        ax4.set_ylim(0, 100)
        
        # Add quality threshold lines
        ax4.axhline(y=75, color='green', linestyle='--', alpha=0.7, label='Boa Qualidade')
        ax4.axhline(y=50, color='orange', linestyle='--', alpha=0.7, label='Qualidade Média')
        ax4.legend()
        
        # Add value labels
        for i, (score, color) in enumerate(zip(data_quality_scores, quality_colors)):
            ax4.text(i, score + 1, f'{score:.1f}', ha='center', va='bottom', fontweight='bold')
        
        plt.suptitle('Dashboard de Análise por Grupos de Produto\n(Foco em Campos Obrigatórios)', 
                    fontsize=16, fontweight='bold', y=0.98)
        plt.tight_layout()
        
        return self.fig_to_base64(fig)
    
    def create_group_detailed_analysis(self, group_analysis, top_groups=None):
        """Create detailed analysis for each group showing only mandatory fields"""
        if not group_analysis:
            return None
        
        # Sort groups by data quality score and completeness
        sorted_groups = sorted(group_analysis.items(), 
                             key=lambda x: (x[1]['data_quality_score'], x[1]['overall_completeness']), 
                             reverse=True)
        
        if top_groups:
            sorted_groups = sorted_groups[:top_groups]
        
        fig, axes = plt.subplots(len(sorted_groups), 1, figsize=(14, 5 * len(sorted_groups)))
        if len(sorted_groups) == 1:
            axes = [axes]
        
        for idx, (group_key, group_info) in enumerate(sorted_groups):
            ax = axes[idx]
            
            # Get mandatory fields data only
            fields = list(group_info['completeness_stats'].keys())
            rates = [group_info['completeness_stats'][field]['completeness_rate'] 
                    for field in fields]
            filled_counts = [group_info['completeness_stats'][field]['filled_records'] 
                           for field in fields]
            
            # Color code based on completeness rate
            colors = ['#d63031' if rate < 25 else '#fdcb6e' if rate < 50 else 
                     '#74b9ff' if rate < 75 else '#00b894' for rate in rates]
            
            # Create horizontal bar chart
            bars = ax.barh(fields, rates, color=colors)
            
            # Customize chart
            ax.set_xlabel('Taxa de Completude (%)', fontweight='bold')
            ax.set_title(f'{group_info["name"]}\n'
                        f'({group_info["total_records"]} registros • '
                        f'Qualidade: {group_info["data_quality_score"]:.1f}/100)', 
                        fontweight='bold', fontsize=12)
            ax.set_xlim(0, 100)
            ax.grid(True, alpha=0.3)
            
            # Add value labels
            for bar, rate, count in zip(bars, rates, filled_counts):
                width = bar.get_width()
                label = f'{rate:.1f}% ({count}/{group_info["total_records"]})'
                ax.text(width + 1, bar.get_y() + bar.get_height()/2, 
                       label, ha='left', va='center', fontweight='bold', fontsize=9)
            
            # Add overall completeness annotation
            overall = group_info['overall_completeness']
            quality_color = '#d63031' if group_info['data_quality_score'] < 50 else \
                          '#fdcb6e' if group_info['data_quality_score'] < 75 else '#00b894'
            
            ax.text(0.02, 0.98, f'Completude Geral: {overall:.1f}%\n'
                               f'Campos Obrigatórios: {len(fields)}', 
                   transform=ax.transAxes, ha='left', va='top',
                   bbox=dict(boxstyle="round,pad=0.3", facecolor=quality_color, alpha=0.7),
                   fontweight='bold', color='white')
        
        plt.suptitle('Análise Detalhada por Grupo de Produto\n(Apenas Campos Obrigatórios)', 
                    fontsize=16, fontweight='bold', y=0.98)
        plt.tight_layout()
        return self.fig_to_base64(fig)
    
    def create_mandatory_vs_optional_comparison(self, processed_df, group_column='product_group'):
        """Compare mandatory vs optional field extraction rates"""
        group_analysis = self.analyze_group_completeness_enhanced(processed_df, group_column)
        
        if not group_analysis:
            return None
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Collect data for comparison
        group_names = []
        mandatory_rates = []
        optional_rates = []
        
        for group_key, group_info in group_analysis.items():
            group_names.append(group_info['name'][:15] + '...' if len(group_info['name']) > 15 
                             else group_info['name'])
            
            # Mandatory field rate (this is what we calculated)
            mandatory_rate = group_info['overall_completeness']
            mandatory_rates.append(mandatory_rate)
            
            # Optional field rate (all other extracted fields)
            group_data = processed_df[processed_df[group_column] == group_key]
            mandatory_extracted_fields = set()
            
            for field_mapping in group_info['completeness_stats'].values():
                mandatory_extracted_fields.add(field_mapping['extracted_field'])
            
            # Find all extracted fields that are NOT mandatory for this group
            all_extracted_fields = [col for col in processed_df.columns if col.startswith('extracted_')]
            optional_extracted_fields = [field for field in all_extracted_fields 
                                       if field not in mandatory_extracted_fields]
            
            if len(group_data) > 0 and optional_extracted_fields:
                optional_field_rates = []
                for field in optional_extracted_fields:
                    filled = group_data[field].notna().sum()
                    rate = (filled / len(group_data)) * 100
                    optional_field_rates.append(rate)
                
                optional_rate = np.mean(optional_field_rates) if optional_field_rates else 0
            else:
                optional_rate = 0
            
            optional_rates.append(optional_rate)
        
        # Create comparison bar chart
        x = np.arange(len(group_names))
        width = 0.35
        
        bars1 = ax1.bar(x - width/2, mandatory_rates, width, 
                       label='Campos Obrigatórios (Críticos)', 
                       color='#e74c3c', alpha=0.8)
        bars2 = ax1.bar(x + width/2, optional_rates, width, 
                       label='Campos Opcionais (Extras)', 
                       color='#95a5a6', alpha=0.8)
        
        ax1.set_xlabel('Grupos de Produto', fontweight='bold')
        ax1.set_ylabel('Taxa de Completude (%)', fontweight='bold')
        ax1.set_title('Campos Obrigatórios vs Opcionais\n(Foco na Criticidade Empresarial)', 
                     fontweight='bold', fontsize=14)
        ax1.set_xticks(x)
        ax1.set_xticklabels(group_names, rotation=45, ha='right')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.set_ylim(0, 100)
        
        # Add value labels
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{height:.1f}%', ha='center', va='bottom', fontsize=8, fontweight='bold')
        
        # Create impact analysis scatter plot
        data_quality_scores = [group_info['data_quality_score'] for group_info in group_analysis.values()]
        record_counts = [group_info['total_records'] for group_info in group_analysis.values()]
        
        scatter = ax2.scatter(mandatory_rates, data_quality_scores, 
                            s=np.array(record_counts)/max(record_counts)*500 + 50,
                            c=mandatory_rates, cmap='RdYlGn', alpha=0.7, edgecolors='black')
        
        ax2.set_xlabel('Taxa de Completude - Campos Obrigatórios (%)', fontweight='bold')
        ax2.set_ylabel('Score de Qualidade dos Dados', fontweight='bold')
        ax2.set_title('Impacto da Completude Obrigatória\nna Qualidade Geral', fontweight='bold', fontsize=14)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(0, 100)
        ax2.set_ylim(0, 100)
        
        # Add reference lines
        ax2.axhline(y=75, color='green', linestyle='--', alpha=0.5, label='Qualidade Alvo')
        ax2.axvline(x=75, color='green', linestyle='--', alpha=0.5)
        ax2.legend()
        
        # Add group labels
        for i, (name, x, y) in enumerate(zip(group_names, mandatory_rates, data_quality_scores)):
            ax2.annotate(f'G{i+1}', (x, y), xytext=(5, 5), 
                        textcoords='offset points', fontsize=8, fontweight='bold')
        
        plt.colorbar(scatter, ax=ax2, label='Taxa de Completude (%)')
        plt.tight_layout()
        
        return self.fig_to_base64(fig)
    
    def create_business_critical_heatmap(self, group_analysis):
        """Create heatmap showing mandatory field completeness across groups"""
        if not group_analysis:
            return None
        
        # Collect all unique mandatory fields across all groups
        all_mandatory_fields = set()
        for group_info in group_analysis.values():
            all_mandatory_fields.update(group_info['mandatory_fields'])
        
        all_mandatory_fields = sorted(list(all_mandatory_fields))
        group_keys = list(group_analysis.keys())
        
        # Create matrix: groups x mandatory fields
        matrix_data = []
        group_labels = []
        
        for group_key in group_keys:
            group_info = group_analysis[group_key]
            group_labels.append(f"{group_info['name'][:15]}...\n({group_info['total_records']} rec)")
            
            row = []
            group_mandatory = set(group_info['mandatory_fields'])
            
            for field in all_mandatory_fields:
                if field in group_mandatory:
                    # Get completeness rate for this mandatory field in this group
                    completeness = group_info['completeness_stats'].get(field, {}).get('completeness_rate', 0)
                    row.append(completeness)
                else:
                    row.append(-5)  # Not required for this group (different from missing)
            
            matrix_data.append(row)
        
        # Create heatmap
        fig, ax = plt.subplots(figsize=(max(12, len(all_mandatory_fields) * 0.8), 
                                      max(6, len(group_keys) * 0.6)))
        
        # Custom colormap: gray for not required (-5), red to green for completeness (0-100)
        import matplotlib.colors as mcolors
        from matplotlib.colors import LinearSegmentedColormap
        
        # Create custom colormap
        colors = ['lightgray', '#d63031', '#fdcb6e', '#74b9ff', '#00b894']
        n_bins = 100
        cmap = LinearSegmentedColormap.from_list('custom', colors, N=n_bins)
        
        # Normalize data: -5 maps to special gray, 0-100 maps to color scale
        masked_data = np.array(matrix_data)
        
        im = ax.imshow(masked_data, cmap=cmap, aspect='auto', vmin=-5, vmax=100)
        
        # Set ticks and labels
        ax.set_xticks(range(len(all_mandatory_fields)))
        ax.set_xticklabels([field.replace('_', '\n') for field in all_mandatory_fields], 
                          rotation=45, ha='right', fontsize=9)
        ax.set_yticks(range(len(group_keys)))
        ax.set_yticklabels(group_labels, fontsize=9)
        
        # Add text annotations
        for i in range(len(group_keys)):
            for j in range(len(all_mandatory_fields)):
                value = masked_data[i, j]
                if value == -5:
                    text = 'N/R'  # Not Required
                    color = 'black'
                    fontweight = 'normal'
                elif value == 0:
                    text = '0%'
                    color = 'white'
                    fontweight = 'bold'
                else:
                    text = f'{value:.0f}%'
                    color = 'white' if value < 50 else 'black'
                    fontweight = 'bold'
                
                ax.text(j, i, text, ha='center', va='center', color=color, 
                       fontweight=fontweight, fontsize=8)
        
        ax.set_title('Mapa de Calor: Completude de Campos Obrigatórios por Grupo\n'
                    '(Cinza = Não Obrigatório para este Grupo | Verde = Alta Completude | Vermelho = Baixa Completude)', 
                    fontweight='bold', fontsize=14, pad=20)
        
        # Add colorbar with custom labels
        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label('Taxa de Completude (%)', fontweight='bold')
        
        plt.tight_layout()
        return self.fig_to_base64(fig)
    
    def fig_to_base64(self, fig):
        """Convert matplotlib figure to base64 string"""
        img_buffer = io.BytesIO()
        fig.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        img_buffer.seek(0)
        img_string = base64.b64encode(img_buffer.read()).decode()
        plt.close(fig)
        return f"data:image/png;base64,{img_string}"
    
    def generate_group_based_report(self, processed_df, group_column='product_group'):
        """Generate comprehensive group-based analysis report"""
        print("🎨 Generating group-based visual analytics report...")
        
        if group_column not in processed_df.columns:
            print(f"❌ Group column '{group_column}' not found in data")
            return None
        
        try:
            # Analyze group completeness (focusing on mandatory fields only)
            group_analysis = self.analyze_group_completeness_enhanced(processed_df, group_column)
            
            if not group_analysis:
                print("❌ No valid groups found for analysis")
                return None
            
            # Generate visualizations
            visualizations = {}
            
            try:
                visualizations['group_overview'] = self.create_group_overview_dashboard(group_analysis)
                print("✅ Group overview dashboard created")
            except Exception as e:
                print(f"⚠️  Warning: Could not create group overview: {e}")
                visualizations['group_overview'] = None
            
            try:
                visualizations['detailed_analysis'] = self.create_group_detailed_analysis(group_analysis)
                print("✅ Detailed group analysis created")
            except Exception as e:
                print(f"⚠️  Warning: Could not create detailed analysis: {e}")
                visualizations['detailed_analysis'] = None
            
            try:
                visualizations['mandatory_vs_optional'] = self.create_mandatory_vs_optional_comparison(
                    processed_df, group_column)
                print("✅ Mandatory vs optional comparison created")
            except Exception as e:
                print(f"⚠️  Warning: Could not create comparison chart: {e}")
                visualizations['mandatory_vs_optional'] = None
            
            try:
                visualizations['critical_heatmap'] = self.create_business_critical_heatmap(group_analysis)
                print("✅ Business critical heatmap created")
            except Exception as e:
                print(f"⚠️  Warning: Could not create heatmap: {e}")
                visualizations['critical_heatmap'] = None
            
            # Generate insights
            insights = self.generate_group_insights(group_analysis)
            
            # Compile comprehensive report
            report = {
                'group_analysis': group_analysis,
                'visualizations': visualizations,
                'insights': insights,
                'summary': self.generate_executive_summary(group_analysis),
                'generated_at': datetime.now().isoformat(),
                'total_groups_analyzed': len(group_analysis),
                'focus': 'mandatory_fields_only'
            }
            
            print("✅ Group-based visual analytics report generated successfully!")
            return report
            
        except Exception as e:
            print(f"❌ Error generating group-based report: {e}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def generate_group_insights(self, group_analysis):
        """Generate business insights from group analysis"""
        insights = []
        
        if not group_analysis:
            return insights
        
        # Overall performance insight
        avg_completeness = np.mean([info['overall_completeness'] for info in group_analysis.values()])
        avg_quality = np.mean([info['data_quality_score'] for info in group_analysis.values()])
        
        insights.append({
            'type': 'performance',
            'title': 'Performance Geral dos Grupos',
            'description': f'Completude média de campos obrigatórios: {avg_completeness:.1f}%. '
                          f'Score médio de qualidade: {avg_quality:.1f}/100.',
            'metric': f'{avg_completeness:.1f}%',
            'business_impact': 'high' if avg_completeness > 75 else 'medium' if avg_completeness > 50 else 'low'
        })
        
        # Best performing group
        best_group = max(group_analysis.items(), key=lambda x: x[1]['data_quality_score'])
        insights.append({
            'type': 'best_performer',
            'title': 'Melhor Grupo de Produto',
            'description': f'{best_group[1]["name"]} tem o melhor score de qualidade '
                          f'({best_group[1]["data_quality_score"]:.1f}/100) com '
                          f'{best_group[1]["overall_completeness"]:.1f}% de completude.',
            'metric': f'{best_group[1]["data_quality_score"]:.1f}/100',
            'business_impact': 'high'
        })
        
        # Worst performing group (needs attention)
        worst_group = min(group_analysis.items(), key=lambda x: x[1]['data_quality_score'])
        insights.append({
            'type': 'needs_attention',
            'title': worst_group[1]['name'] + ' - Grupo com Menor Qualidade',
            'description': f'{worst_group[1]["name"]} apresenta o menor score de qualidade '
                          f'({worst_group[1]["data_quality_score"]:.1f}/100) e completude de '
                          f'{worst_group[1]["overall_completeness"]:.1f}%. Recomendamos ações corretivas.',
            'metric': f'{worst_group[1]["data_quality_score"]:.1f}/100',
            'business_impact': 'high'
        })
        # Critical fields missing    