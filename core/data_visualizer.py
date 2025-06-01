import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class GroupBasedDataVisualizer:
    """
    Enhanced data visualizer focused on product group analysis
    Creates real visualizations with actual data - no mock data
    """
    
    def __init__(self, product_group_manager):
        self.group_manager = product_group_manager
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
    
    def create_group_completeness_analysis(self, processed_df, group_column='product_group'):
        """
        Create comprehensive analysis of mandatory field completeness by group
        """
        if group_column not in processed_df.columns:
            return {'error': f"Column '{group_column}' not found in processed data"}
        
        # Analyze completeness using the group manager
        completeness_data = self.group_manager.analyze_group_completeness(processed_df, group_column)
        
        if not completeness_data or 'error' in completeness_data:
            return {'error': 'Could not analyze group completeness'}
        
        visualizations = {}
        
        # 1. Group Overview Chart
        visualizations['group_overview'] = self._create_group_overview_chart(completeness_data)
        
        # 2. Mandatory Field Heatmap
        visualizations['mandatory_heatmap'] = self._create_mandatory_field_heatmap(completeness_data)
        
        # 3. Category Performance Chart
        visualizations['category_performance'] = self._create_category_performance_chart(completeness_data)
        
        # 4. Priority vs Quality Scatter
        visualizations['priority_quality_scatter'] = self._create_priority_quality_scatter(completeness_data)
        
        # 5. Field-specific Analysis
        visualizations['field_analysis'] = self._create_field_specific_analysis(completeness_data)
        
        return {
            'visualizations': visualizations,
            'analysis_data': completeness_data,
            'summary_insights': self._generate_group_insights(completeness_data),
            'generated_at': datetime.now().isoformat()
        }
    
    def _create_group_overview_chart(self, completeness_data):
        """
        Create overview chart showing completeness by product group
        """
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
            
            # Prepare data
            groups = []
            completeness_rates = []
            record_counts = []
            categories = []
            
            for group_key, data in completeness_data.items():
                if 'error' in data:
                    continue
                groups.append(data['name'][:25])  # Truncate long names
                completeness_rates.append(data['overall_completeness'])
                record_counts.append(data['total_records'])
                categories.append(self.group_manager.get_group_category(group_key))
            
            if not groups:
                return None
            
            # Chart 1: Completeness rates by group
            colors = plt.cm.RdYlGn([rate/100 for rate in completeness_rates])
            bars1 = ax1.barh(groups, completeness_rates, color=colors)
            ax1.set_xlabel('Taxa de Completude de Campos Obrigatórios (%)', fontweight='bold')
            ax1.set_title('Completude por Grupo de Produto', fontweight='bold', fontsize=14)
            ax1.set_xlim(0, 100)
            
            # Add value labels
            for bar, rate in zip(bars1, completeness_rates):
                width = bar.get_width()
                ax1.text(width + 1, bar.get_y() + bar.get_height()/2, 
                        f'{rate:.1f}%', ha='left', va='center', fontweight='bold')
            
            # Chart 2: Record count vs completeness scatter
            unique_categories = list(set(categories))
            category_colors = plt.cm.Set3(np.linspace(0, 1, len(unique_categories)))
            category_color_map = dict(zip(unique_categories, category_colors))
            
            scatter_colors = [category_color_map[cat] for cat in categories]
            
            scatter = ax2.scatter(record_counts, completeness_rates, 
                                c=scatter_colors, s=100, alpha=0.7, edgecolors='black')
            
            ax2.set_xlabel('Número de Registros', fontweight='bold')
            ax2.set_ylabel('Taxa de Completude (%)', fontweight='bold')
            ax2.set_title('Volume vs Qualidade por Grupo', fontweight='bold', fontsize=14)
            ax2.grid(True, alpha=0.3)
            
            # Add group labels to scatter points
            for i, (x, y, group) in enumerate(zip(record_counts, completeness_rates, groups)):
                ax2.annotate(f'{i+1}', (x, y), xytext=(5, 5), 
                            textcoords='offset points', fontsize=9, fontweight='bold')
            
            # Add legend for categories
            legend_elements = [plt.scatter([], [], c=color, s=100, label=cat.replace('_', ' ').title()) 
                             for cat, color in category_color_map.items()]
            ax2.legend(handles=legend_elements, title='Categorias', loc='lower right')
            
            plt.suptitle('Análise de Completude por Grupo de Produto', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️ Error creating group overview chart: {e}")
            return None
    
    def _create_mandatory_field_heatmap(self, completeness_data):
        """
        Create heatmap showing mandatory field completeness across groups
        """
        try:
            # Collect all unique mandatory fields across groups
            all_mandatory_fields = set()
            for group_data in completeness_data.values():
                if 'error' not in group_data:
                    all_mandatory_fields.update(group_data['mandatory_fields'])
            
            if not all_mandatory_fields:
                return None
            
            all_mandatory_fields = sorted(list(all_mandatory_fields))
            group_names = []
            group_keys = []
            
            # Build matrix data
            matrix_data = []
            for group_key, group_data in completeness_data.items():
                if 'error' in group_data:
                    continue
                    
                group_names.append(group_data['name'][:20])  # Truncate for display
                group_keys.append(group_key)
                
                row = []
                group_mandatory = set(group_data['mandatory_fields'])
                
                for field in all_mandatory_fields:
                    if field in group_mandatory:
                        # Get completeness rate for this mandatory field
                        completeness = group_data['completeness_stats'].get(field, {}).get('completeness_rate', 0)
                        row.append(completeness)
                    else:
                        row.append(-10)  # Not required for this group
                
                matrix_data.append(row)
            
            if not matrix_data:
                return None
            
            # Create heatmap
            fig, ax = plt.subplots(figsize=(max(12, len(all_mandatory_fields) * 0.7), 
                                          max(6, len(group_names) * 0.5)))
            
            # Custom colormap: gray for not required, red to green for completeness
            from matplotlib.colors import LinearSegmentedColormap
            colors = ['lightgray', '#d63031', '#fdcb6e', '#74b9ff', '#00b894']
            n_bins = 100
            cmap = LinearSegmentedColormap.from_list('custom', colors, N=n_bins)
            
            matrix_array = np.array(matrix_data)
            im = ax.imshow(matrix_array, cmap=cmap, aspect='auto', vmin=-10, vmax=100)
            
            # Set ticks and labels
            ax.set_xticks(range(len(all_mandatory_fields)))
            ax.set_xticklabels([field.replace('_', '\n') for field in all_mandatory_fields], 
                              rotation=45, ha='right', fontsize=9)
            ax.set_yticks(range(len(group_names)))
            ax.set_yticklabels(group_names, fontsize=9)
            
            # Add text annotations
            for i in range(len(group_names)):
                for j in range(len(all_mandatory_fields)):
                    value = matrix_array[i, j]
                    if value == -10:
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
            
            ax.set_title('Completude de Campos Obrigatórios por Grupo\n(Cinza = Não Obrigatório | Verde = Alta | Vermelho = Baixa)', 
                        fontweight='bold', fontsize=14, pad=20)
            
            # Add colorbar
            cbar = plt.colorbar(im, ax=ax, shrink=0.8)
            cbar.set_label('Taxa de Completude (%)', fontweight='bold')
            
            plt.tight_layout()
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️ Error creating mandatory field heatmap: {e}")
            return None
    
    def _create_category_performance_chart(self, completeness_data):
        """
        Create chart showing performance by product category
        """
        try:
            # Group data by category
            category_data = {}
            for group_key, group_data in completeness_data.items():
                if 'error' in group_data:
                    continue
                    
                category = self.group_manager.get_group_category(group_key)
                if category not in category_data:
                    category_data[category] = {
                        'completeness_rates': [],
                        'record_counts': [],
                        'group_names': []
                    }
                
                category_data[category]['completeness_rates'].append(group_data['overall_completeness'])
                category_data[category]['record_counts'].append(group_data['total_records'])
                category_data[category]['group_names'].append(group_data['name'])
            
            if not category_data:
                return None
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
            
            # Chart 1: Average completeness by category
            categories = list(category_data.keys())
            avg_completeness = [np.mean(data['completeness_rates']) for data in category_data.values()]
            total_records = [sum(data['record_counts']) for data in category_data.values()]
            
            colors = plt.cm.Set2(np.linspace(0, 1, len(categories)))
            bars = ax1.bar(categories, avg_completeness, color=colors, alpha=0.8)
            
            ax1.set_ylabel('Completude Média (%)', fontweight='bold')
            ax1.set_title('Completude Média por Categoria', fontweight='bold', fontsize=14)
            ax1.set_ylim(0, 100)
            
            # Add value labels and record counts
            for bar, avg_comp, records in zip(bars, avg_completeness, total_records):
                height = bar.get_height()
                ax1.text(bar.get_x() + bar.get_width()/2., height + 1,
                        f'{avg_comp:.1f}%\n({records:,} reg)', 
                        ha='center', va='bottom', fontweight='bold', fontsize=9)
            
            # Rotate category labels if needed
            ax1.tick_params(axis='x', rotation=45)
            
            # Chart 2: Distribution within categories
            category_names_short = [cat.replace('_', '\n') for cat in categories]
            
            # Create box plot data
            box_data = []
            for category in categories:
                box_data.append(category_data[category]['completeness_rates'])
            
            bp = ax2.boxplot(box_data, labels=category_names_short, patch_artist=True)
            
            # Color the box plots
            for patch, color in zip(bp['boxes'], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)
            
            ax2.set_ylabel('Taxa de Completude (%)', fontweight='bold')
            ax2.set_title('Distribuição da Completude por Categoria', fontweight='bold', fontsize=14)
            ax2.set_ylim(0, 100)
            ax2.grid(True, alpha=0.3)
            
            plt.suptitle('Análise de Performance por Categoria', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️ Error creating category performance chart: {e}")
            return None
    
    def _create_priority_quality_scatter(self, completeness_data):
        """
        Create scatter plot showing relationship between priority level and quality
        """
        try:
            priority_mapping = {'critical': 3, 'high': 2, 'medium': 1, 'low': 0}
            
            # Prepare data
            priorities = []
            completeness_rates = []
            record_counts = []
            group_names = []
            categories = []
            
            for group_key, group_data in completeness_data.items():
                if 'error' in group_data:
                    continue
                    
                priority_level = self.group_manager.get_group_priority_level(group_key)
                priority_num = priority_mapping.get(priority_level, 1)
                
                priorities.append(priority_num)
                completeness_rates.append(group_data['overall_completeness'])
                record_counts.append(group_data['total_records'])
                group_names.append(group_data['name'][:15])
                categories.append(self.group_manager.get_group_category(group_key))
            
            if not priorities:
                return None
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            # Create scatter plot with size based on record count
            max_records = max(record_counts) if record_counts else 1
            sizes = [50 + (count/max_records)*300 for count in record_counts]
            
            # Color by category
            unique_categories = list(set(categories))
            category_colors = plt.cm.Set3(np.linspace(0, 1, len(unique_categories)))
            category_color_map = dict(zip(unique_categories, category_colors))
            colors = [category_color_map[cat] for cat in categories]
            
            scatter = ax.scatter(priorities, completeness_rates, s=sizes, c=colors, 
                               alpha=0.7, edgecolors='black', linewidth=1)
            
            ax.set_xlabel('Nível de Prioridade', fontweight='bold')
            ax.set_ylabel('Taxa de Completude (%)', fontweight='bold')
            ax.set_title('Prioridade vs Qualidade dos Dados\n(Tamanho = Volume de Registros)', 
                        fontweight='bold', fontsize=14)
            
            # Set x-axis labels
            ax.set_xticks([0, 1, 2, 3])
            ax.set_xticklabels(['Low', 'Medium', 'High', 'Critical'])
            ax.set_ylim(0, 100)
            ax.grid(True, alpha=0.3)
            
            # Add group labels
            for i, (x, y, name) in enumerate(zip(priorities, completeness_rates, group_names)):
                ax.annotate(name, (x, y), xytext=(5, 5), 
                           textcoords='offset points', fontsize=8, 
                           bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7))
            
            # Add reference lines
            ax.axhline(y=80, color='green', linestyle='--', alpha=0.5, label='Qualidade Alvo (80%)')
            ax.axhline(y=60, color='orange', linestyle='--', alpha=0.5, label='Qualidade Mínima (60%)')
            
            # Add legend for categories
            legend_elements = [plt.scatter([], [], c=color, s=100, label=cat.replace('_', ' ').title()) 
                             for cat, color in category_color_map.items()]
            ax.legend(handles=legend_elements, title='Categorias', loc='lower left')
            
            plt.tight_layout()
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️ Error creating priority quality scatter: {e}")
            return None
    
    def _create_field_specific_analysis(self, completeness_data):
        """
        Create analysis showing which fields are most/least complete across groups
        """
        try:
            # Collect field completeness data across all groups
            field_completeness = {}
            
            for group_key, group_data in completeness_data.items():
                if 'error' in group_data:
                    continue
                    
                for field, stats in group_data['completeness_stats'].items():
                    if field not in field_completeness:
                        field_completeness[field] = []
                    
                    if not stats.get('missing', False):
                        field_completeness[field].append(stats['completeness_rate'])
            
            if not field_completeness:
                return None
            
            # Calculate statistics for each field
            field_stats = {}
            for field, rates in field_completeness.items():
                if rates:  # Only include fields with data
                    field_stats[field] = {
                        'avg_completeness': np.mean(rates),
                        'min_completeness': np.min(rates),
                        'max_completeness': np.max(rates),
                        'std_completeness': np.std(rates),
                        'groups_count': len(rates)
                    }
            
            if not field_stats:
                return None
            
            # Sort fields by average completeness
            sorted_fields = sorted(field_stats.items(), key=lambda x: x[1]['avg_completeness'], reverse=True)
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 12))
            
            # Chart 1: Average completeness by field (top 15 fields)
            top_fields = sorted_fields[:15]
            field_names = [field.replace('_', ' ').title() for field, _ in top_fields]
            avg_rates = [stats['avg_completeness'] for _, stats in top_fields]
            
            colors = plt.cm.RdYlGn([rate/100 for rate in avg_rates])
            bars = ax1.barh(field_names, avg_rates, color=colors)
            
            ax1.set_xlabel('Completude Média (%)', fontweight='bold')
            ax1.set_title('Completude Média por Campo (Top 15)', fontweight='bold', fontsize=14)
            ax1.set_xlim(0, 100)
            
            # Add value labels
            for bar, rate in zip(bars, avg_rates):
                width = bar.get_width()
                ax1.text(width + 1, bar.get_y() + bar.get_height()/2, 
                        f'{rate:.1f}%', ha='left', va='center', fontweight='bold', fontsize=9)
            
            # Chart 2: Field completeness variability (showing consistency)
            bottom_fields = sorted_fields[-15:] if len(sorted_fields) > 15 else sorted_fields
            field_names_var = [field.replace('_', ' ').title() for field, _ in bottom_fields]
            std_deviations = [stats['std_completeness'] for _, stats in bottom_fields]
            avg_rates_var = [stats['avg_completeness'] for _, stats in bottom_fields]
            
            # Color by average rate but show variability
            colors_var = plt.cm.RdYlGn([rate/100 for rate in avg_rates_var])
            bars2 = ax2.barh(field_names_var, std_deviations, color=colors_var, alpha=0.7)
            
            ax2.set_xlabel('Desvio Padrão da Completude (%)', fontweight='bold')
            ax2.set_title('Variabilidade da Completude por Campo\n(Menor = Mais Consistente)', fontweight='bold', fontsize=14)
            
            # Add value labels
            for bar, std in zip(bars2, std_deviations):
                width = bar.get_width()
                ax2.text(width + 0.5, bar.get_y() + bar.get_height()/2, 
                        f'{std:.1f}', ha='left', va='center', fontweight='bold', fontsize=9)
            
            plt.suptitle('Análise Específica por Campo', fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️ Error creating field specific analysis: {e}")
            return None
    
    def create_business_impact_visualization(self, completeness_data, processing_stats=None):
        """
        Create business-focused visualization showing impact of group-based processing
        """
        try:
            fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
            
            # Extract data for analysis
            groups = []
            completeness_rates = []
            record_counts = []
            categories = []
            priorities = []
            
            for group_key, group_data in completeness_data.items():
                if 'error' in group_data:
                    continue
                groups.append(group_data['name'][:20])
                completeness_rates.append(group_data['overall_completeness'])
                record_counts.append(group_data['total_records'])
                categories.append(self.group_manager.get_group_category(group_key))
                priorities.append(self.group_manager.get_group_priority_level(group_key))
            
            # 1. Business Value by Group (Top left)
            # Calculate business value score (completeness * records * priority weight)
            priority_weights = {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}
            business_values = []
            
            for comp, records, priority in zip(completeness_rates, record_counts, priorities):
                weight = priority_weights.get(priority, 2)
                # Normalize and calculate business value
                value = (comp / 100) * np.log10(records + 1) * weight
                business_values.append(value)
            
            # Select top 10 for display
            top_indices = np.argsort(business_values)[-10:]
            top_groups = [groups[i] for i in top_indices]
            top_values = [business_values[i] for i in top_indices]
            
            bars1 = ax1.barh(top_groups, top_values, color=plt.cm.viridis(np.linspace(0, 1, len(top_values))))
            ax1.set_xlabel('Score de Valor Empresarial', fontweight='bold')
            ax1.set_title('Valor Empresarial por Grupo\n(Completude × Volume × Prioridade)', fontweight='bold')
            
            # 2. Data Quality Distribution (Top right)
            quality_bins = ['Crítico\n(<60%)', 'Regular\n(60-80%)', 'Bom\n(80-95%)', 'Excelente\n(>95%)']
            quality_counts = [
                sum(1 for rate in completeness_rates if rate < 60),
                sum(1 for rate in completeness_rates if 60 <= rate < 80),
                sum(1 for rate in completeness_rates if 80 <= rate < 95),
                sum(1 for rate in completeness_rates if rate >= 95)
            ]
            
            colors_quality = ['#d63031', '#fdcb6e', '#74b9ff', '#00b894']
            pie = ax2.pie(quality_counts, labels=quality_bins, autopct='%1.0f%%', 
                         colors=colors_quality, startangle=90)
            ax2.set_title('Distribuição da Qualidade dos Dados', fontweight='bold')
            
            # 3. Category Performance Matrix (Bottom left)
            category_performance = {}
            for cat, comp, records in zip(categories, completeness_rates, record_counts):
                if cat not in category_performance:
                    category_performance[cat] = {'completeness': [], 'records': []}
                category_performance[cat]['completeness'].append(comp)
                category_performance[cat]['records'].append(records)
            
            cat_names = list(category_performance.keys())
            cat_avg_comp = [np.mean(data['completeness']) for data in category_performance.values()]
            cat_total_records = [sum(data['records']) for data in category_performance.values()]
            
            # Bubble chart
            max_records = max(cat_total_records) if cat_total_records else 1
            bubble_sizes = [100 + (records/max_records)*400 for records in cat_total_records]
            
            scatter = ax3.scatter(range(len(cat_names)), cat_avg_comp, s=bubble_sizes, 
                                alpha=0.6, c=cat_avg_comp, cmap='RdYlGn', 
                                edgecolors='black', linewidth=1, vmin=0, vmax=100)
            
            ax3.set_xticks(range(len(cat_names)))
            ax3.set_xticklabels([cat.replace('_', '\n') for cat in cat_names], rotation=45)
            ax3.set_ylabel('Completude Média (%)', fontweight='bold')
            ax3.set_title('Performance por Categoria\n(Tamanho = Volume Total)', fontweight='bold')
            ax3.set_ylim(0, 100)
            ax3.grid(True, alpha=0.3)
            
            # Add value labels
            for i, (comp, records) in enumerate(zip(cat_avg_comp, cat_total_records)):
                ax3.text(i, comp + 2, f'{comp:.1f}%\n{records:,}', 
                        ha='center', va='bottom', fontsize=8, fontweight='bold')
            
            # 4. Improvement Opportunities (Bottom right)
            # Identify groups with high volume but low completeness
            improvement_scores = []
            for comp, records in zip(completeness_rates, record_counts):
                # High impact = high volume but low completeness
                if comp < 80:  # Only consider groups with room for improvement
                    impact_score = records * (100 - comp) / 100  # Volume weighted by improvement potential
                    improvement_scores.append(impact_score)
                else:
                    improvement_scores.append(0)
            
            # Get top improvement opportunities
            top_improvement_indices = np.argsort(improvement_scores)[-8:]
            improvement_groups = [groups[i] for i in top_improvement_indices if improvement_scores[i] > 0]
            improvement_values = [improvement_scores[i] for i in top_improvement_indices if improvement_scores[i] > 0]
            
            if improvement_groups:
                bars4 = ax4.barh(improvement_groups, improvement_values, 
                               color='orange', alpha=0.7)
                ax4.set_xlabel('Score de Oportunidade de Melhoria', fontweight='bold')
                ax4.set_title('Principais Oportunidades de Melhoria\n(Volume × Potencial)', fontweight='bold')
                
                # Add value labels
                for bar, value in zip(bars4, improvement_values):
                    width = bar.get_width()
                    ax4.text(width + width*0.01, bar.get_y() + bar.get_height()/2, 
                            f'{value:.0f}', ha='left', va='center', fontweight='bold', fontsize=9)
            else:
                ax4.text(0.5, 0.5, 'Todos os grupos\najá possuem\nalta qualidade!', 
                        ha='center', va='center', transform=ax4.transAxes, 
                        fontsize=14, fontweight='bold', color='green')
                ax4.set_title('Oportunidades de Melhoria', fontweight='bold')
            
            plt.suptitle('Análise de Impacto Empresarial - Processamento por Grupos', 
                        fontsize=16, fontweight='bold')
            plt.tight_layout()
            
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️ Error creating business impact visualization: {e}")
            return None
    
    def _generate_group_insights(self, completeness_data):
        """
        Generate business insights from group analysis
        """
        insights = []
        
        if not completeness_data:
            return insights
        
        # Calculate overall metrics
        completeness_rates = []
        record_counts = []
        priorities = []
        
        for group_key, group_data in completeness_data.items():
            if 'error' not in group_data:
                completeness_rates.append(group_data['overall_completeness'])
                record_counts.append(group_data['total_records'])
                priorities.append(self.group_manager.get_group_priority_level(group_key))
        
        if not completeness_rates:
            return insights
        
        avg_completeness = np.mean(completeness_rates)
        total_records = sum(record_counts)
        
        # Overall performance insight
        insights.append({
            'type': 'performance',
            'title': 'Performance Geral por Grupos',
            'description': f'Completude média de campos obrigatórios: {avg_completeness:.1f}% across {len(completeness_rates)} grupos de produto.',
            'metric': f'{avg_completeness:.1f}%',
            'impact': 'high' if avg_completeness > 80 else 'medium' if avg_completeness > 60 else 'low'
        })
        
        # Best performing group
        if completeness_rates:
            best_idx = np.argmax(completeness_rates)
            best_group_key = list(completeness_data.keys())[best_idx]
            best_group_data = completeness_data[best_group_key]
            
            insights.append({
                'type': 'best_performer',
                'title': 'Melhor Grupo de Produto',
                'description': f'{best_group_data["name"]} tem a melhor completude ({best_group_data["overall_completeness"]:.1f}%) com {best_group_data["total_records"]:,} registros.',
                'metric': f'{best_group_data["overall_completeness"]:.1f}%',
                'impact': 'high'
            })
        
        # Groups needing attention
        low_completeness_groups = [
            (group_key, group_data) for group_key, group_data in completeness_data.items()
            if 'error' not in group_data and group_data['overall_completeness'] < 60
        ]
        
        if low_completeness_groups:
            group_names = [data['name'] for _, data in low_completeness_groups]
            insights.append({
                'type': 'needs_attention',
                'title': 'Grupos Que Precisam de Atenção',
                'description': f'{len(low_completeness_groups)} grupos com completude < 60%: {", ".join(group_names[:3])}{"..." if len(group_names) > 3 else ""}',
                'metric': f'{len(low_completeness_groups)} grupos',
                'impact': 'high'
            })
        
        # Critical priority groups analysis
        critical_groups = [
            (group_key, group_data) for group_key, group_data in completeness_data.items()
            if 'error' not in group_data and self.group_manager.get_group_priority_level(group_key) == 'critical'
        ]
        
        if critical_groups:
            critical_avg = np.mean([data['overall_completeness'] for _, data in critical_groups])
            insights.append({
                'type': 'critical_analysis',
                'title': 'Análise de Grupos Críticos',
                'description': f'{len(critical_groups)} grupos críticos com completude média de {critical_avg:.1f}%. Qualidade {"adequada" if critical_avg > 80 else "precisa de melhoria"}.',
                'metric': f'{critical_avg:.1f}%',
                'impact': 'critical'
            })
        
        # Volume vs Quality insight
        if len(completeness_rates) > 1:
            high_volume_groups = [
                (rate, count) for rate, count in zip(completeness_rates, record_counts)
                if count > np.median(record_counts)
            ]
            
            if high_volume_groups:
                high_volume_avg_quality = np.mean([rate for rate, _ in high_volume_groups])
                insights.append({
                    'type': 'volume_quality',
                    'title': 'Qualidade vs Volume',
                    'description': f'Grupos com alto volume ({len(high_volume_groups)} grupos) mantêm completude média de {high_volume_avg_quality:.1f}%.',
                    'metric': f'{high_volume_avg_quality:.1f}%',
                    'impact': 'medium'
                })
        
        return insights
    
    def _fig_to_base64(self, fig):
        """Convert matplotlib figure to base64 string"""
        img_buffer = io.BytesIO()
        fig.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        img_buffer.seek(0)
        img_string = base64.b64encode(img_buffer.read()).decode()
        plt.close(fig)
        return f"data:image/png;base64,{img_string}"
    
    def generate_comprehensive_group_report(self, processed_df, group_column='product_group'):
        """
        Generate comprehensive group-based visual report
        """
        print("🎨 Generating group-based visual analytics report...")
        
        if group_column not in processed_df.columns:
            return {
                'error': f"Column '{group_column}' not found in processed data",
                'suggestion': 'Ensure your data has been processed with product group classification'
            }
        
        try:
            # Generate main group completeness analysis
            main_analysis = self.create_group_completeness_analysis(processed_df, group_column)
            
            if 'error' in main_analysis:
                return main_analysis
            
            # Generate business impact visualization
            business_viz = self.create_business_impact_visualization(
                main_analysis['analysis_data']
            )
            
            # Compile comprehensive report
            report = {
                'group_visualizations': main_analysis['visualizations'],
                'business_impact_visualization': business_viz,
                'analysis_data': main_analysis['analysis_data'],
                'insights': main_analysis['summary_insights'],
                'executive_summary': self._create_executive_summary(main_analysis['analysis_data']),
                'recommendations': self._generate_actionable_recommendations(main_analysis['analysis_data']),
                'generated_at': datetime.now().isoformat(),
                'total_groups_analyzed': len(main_analysis['analysis_data']),
                'focus': 'product_group_mandatory_fields'
            }
            
            print("✅ Group-based visual analytics report generated successfully!")
            return report
            
        except Exception as e:
            print(f"❌ Error generating group-based report: {e}")
            import traceback
            print(traceback.format_exc())
            
            return {
                'error': f"Failed to generate group report: {str(e)}",
                'suggestion': 'Check data format and ensure product groups are properly defined'
            }
    
    def _create_executive_summary(self, analysis_data):
        """
        Create executive summary for business stakeholders
        """
        if not analysis_data:
            return {}
        
        # Calculate key metrics
        total_groups = len(analysis_data)
        total_records = sum(data.get('total_records', 0) for data in analysis_data.values() if 'error' not in data)
        
        completeness_rates = [
            data['overall_completeness'] for data in analysis_data.values() 
            if 'error' not in data
        ]
        
        if not completeness_rates:
            return {'error': 'No valid data for executive summary'}
        
        avg_completeness = np.mean(completeness_rates)
        min_completeness = np.min(completeness_rates)
        max_completeness = np.max(completeness_rates)
        
        # Categorize groups by performance
        excellent_groups = sum(1 for rate in completeness_rates if rate >= 90)
        good_groups = sum(1 for rate in completeness_rates if 80 <= rate < 90)
        needs_improvement = sum(1 for rate in completeness_rates if 60 <= rate < 80)
        critical_groups = sum(1 for rate in completeness_rates if rate < 60)
        
        # Calculate business impact
        high_priority_groups = [
            data for group_key, data in analysis_data.items()
            if 'error' not in data and self.group_manager.get_group_priority_level(group_key) in ['critical', 'high']
        ]
        
        high_priority_avg = np.mean([
            data['overall_completeness'] for data in high_priority_groups
        ]) if high_priority_groups else 0
        
        return {
            'key_metrics': {
                'total_product_groups': total_groups,
                'total_records_analyzed': f"{total_records:,}",
                'average_completeness': f"{avg_completeness:.1f}%",
                'completeness_range': f"{min_completeness:.1f}% - {max_completeness:.1f}%"
            },
            'performance_distribution': {
                'excellent': {'count': excellent_groups, 'percentage': f"{(excellent_groups/total_groups)*100:.0f}%"},
                'good': {'count': good_groups, 'percentage': f"{(good_groups/total_groups)*100:.0f}%"},
                'needs_improvement': {'count': needs_improvement, 'percentage': f"{(needs_improvement/total_groups)*100:.0f}%"},
                'critical': {'count': critical_groups, 'percentage': f"{(critical_groups/total_groups)*100:.0f}%"}
            },
            'business_critical_status': {
                'high_priority_groups': len(high_priority_groups),
                'high_priority_avg_completeness': f"{high_priority_avg:.1f}%",
                'status': 'excellent' if high_priority_avg >= 85 else 'good' if high_priority_avg >= 70 else 'needs_attention'
            },
            'overall_assessment': self._get_overall_assessment(avg_completeness, critical_groups, total_groups)
        }
    
    def _get_overall_assessment(self, avg_completeness, critical_groups, total_groups):
        """
        Get overall data quality assessment
        """
        if avg_completeness >= 85 and critical_groups == 0:
            return {
                'grade': 'A',
                'status': 'Excelente',
                'message': 'Qualidade de dados excepcional com alta completude em todos os grupos.'
            }
        elif avg_completeness >= 75 and critical_groups <= total_groups * 0.1:
            return {
                'grade': 'B',
                'status': 'Bom',
                'message': 'Boa qualidade geral com alguns grupos precisando de otimização.'
            }
        elif avg_completeness >= 60 and critical_groups <= total_groups * 0.25:
            return {
                'grade': 'C',
                'status': 'Regular',
                'message': 'Qualidade moderada. Ações corretivas recomendadas para grupos críticos.'
            }
        else:
            return {
                'grade': 'D',
                'status': 'Crítico',
                'message': 'Qualidade de dados requer atenção imediata. Múltiplos grupos com baixa completude.'
            }
    
    def _generate_actionable_recommendations(self, analysis_data):
        """
        Generate specific, actionable recommendations
        """
        recommendations = []
        
        if not analysis_data:
            return recommendations
        
        # 1. Critical groups that need immediate attention
        critical_groups = [
            (group_key, data) for group_key, data in analysis_data.items()
            if 'error' not in data and data['overall_completeness'] < 60
        ]
        
        if critical_groups:
            group_names = [data['name'] for _, data in critical_groups[:3]]
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Data Quality',
                'title': 'Ação Imediata Necessária',
                'description': f'Grupos com completude crítica (<60%): {", ".join(group_names)}',
                'actions': [
                    'Revisar padrões de extração para estes grupos',
                    'Validar qualidade dos dados de origem',
                    'Considerar regras de limpeza específicas',
                    'Implementar monitoramento contínuo'
                ],
                'expected_impact': 'Alto',
                'estimated_effort': 'Médio'
            })
        
        # 2. High-priority groups with moderate completeness
        priority_groups_moderate = [
            (group_key, data) for group_key, data in analysis_data.items()
            if 'error' not in data 
            and self.group_manager.get_group_priority_level(group_key) in ['critical', 'high']
            and 60 <= data['overall_completeness'] < 85
        ]
        
        if priority_groups_moderate:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Business Critical',
                'title': 'Otimizar Grupos de Alta Prioridade',
                'description': f'{len(priority_groups_moderate)} grupos críticos/importantes com completude moderada',
                'actions': [
                    'Priorizar melhoria de campos obrigatórios',
                    'Implementar validação adicional na entrada',
                    'Treinar equipe em padrões específicos',
                    'Automatizar verificações de qualidade'
                ],
                'expected_impact': 'Alto',
                'estimated_effort': 'Alto'
            })
        
        # 3. Fields with consistently low completeness
        field_issues = {}
        for group_key, data in analysis_data.items():
            if 'error' in data:
                continue
            for field, stats in data['completeness_stats'].items():
                if stats.get('completeness_rate', 0) < 50:
                    if field not in field_issues:
                        field_issues[field] = 0
                    field_issues[field] += 1
        
        problematic_fields = [field for field, count in field_issues.items() if count >= len(analysis_data) * 0.3]
        
        if problematic_fields:
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Pattern Optimization',
                'title': 'Melhorar Padrões de Extração',
                'description': f'Campos com baixa completude em múltiplos grupos: {", ".join(problematic_fields[:5])}',
                'actions': [
                    'Revisar expressões regulares para estes campos',
                    'Analisar amostras de texto que não estão sendo extraídas',
                    'Considerar padrões alternativos de nomenclatura',
                    'Testar com datasets históricos'
                ],
                'expected_impact': 'Médio',
                'estimated_effort': 'Médio'
            })
        
        # 4. Data standardization opportunities
        inconsistent_groups = [
            data for data in analysis_data.values()
            if 'error' not in data and abs(data['overall_completeness'] - np.mean([d['overall_completeness'] for d in analysis_data.values() if 'error' not in d])) > 20
        ]
        
        if len(inconsistent_groups) > len(analysis_data) * 0.3:
            recommendations.append({
                'priority': 'LOW',
                'category': 'Standardization',
                'title': 'Padronizar Processamento Entre Grupos',
                'description': 'Grande variação na qualidade entre grupos sugere oportunidades de padronização',
                'actions': [
                    'Documentar melhores práticas dos grupos de alta performance',
                    'Criar templates padrão para novos grupos',
                    'Implementar processo de revisão por pares',
                    'Estabelecer métricas de qualidade uniformes'
                ],
                'expected_impact': 'Médio',
                'estimated_effort': 'Baixo'
            })
        
        # 5. Success stories to replicate
        excellent_groups = [
            (group_key, data) for group_key, data in analysis_data.items()
            if 'error' not in data and data['overall_completeness'] >= 90
        ]
        
        if excellent_groups:
            group_names = [data['name'] for _, data in excellent_groups[:3]]
            recommendations.append({
                'priority': 'LOW',
                'category': 'Best Practices',
                'title': 'Replicar Sucessos',
                'description': f'Grupos com excelente performance: {", ".join(group_names)}',
                'actions': [
                    'Documentar padrões e processos destes grupos',
                    'Aplicar mesmas técnicas aos grupos com baixa performance',
                    'Criar cases de sucesso para treinamento',
                    'Estabelecer estes grupos como referência'
                ],
                'expected_impact': 'Alto',
                'estimated_effort': 'Baixo'
            })
        
        return recommendations


# Integration function for routes.py
def create_group_based_visualization_report(processed_df, product_group_manager, group_column='product_group'):
    """
    Helper function to create group-based visualization report
    Can be called from routes.py
    """
    visualizer = GroupBasedDataVisualizer(product_group_manager)
    return visualizer.generate_comprehensive_group_report(processed_df, group_column)


# Backward compatibility
DataVisualizationAnalyzer = GroupBasedDataVisualizer