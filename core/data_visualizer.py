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
    Data visualizer focused on real product group extraction results
    Shows actual extraction success rates and field completeness
    """
    
    def __init__(self, product_group_manager):
        self.group_manager = product_group_manager
        self.setup_plot_style()
        
    def setup_plot_style(self):
        """Setup clean plot styling"""
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
    
    def create_extraction_analysis(self, processed_df, group_column='product_group'):
        """
        Create analysis of actual field extraction by product group
        """
        if group_column not in processed_df.columns:
            return {'error': f"Column '{group_column}' not found in processed data"}
        
        # Get actual extraction data
        extraction_data = self._analyze_real_extraction_data(processed_df, group_column)
        
        if not extraction_data:
            return {'error': 'No extraction data found'}
        
        visualizations = {}
        
        # 1. Extraction Success by Group
        visualizations['extraction_by_group'] = self._create_extraction_success_chart(extraction_data)
        
        # 2. Mandatory Field Coverage
        visualizations['mandatory_coverage'] = self._create_mandatory_field_chart(extraction_data)
        
        # 3. Field Type Distribution
        visualizations['field_distribution'] = self._create_field_distribution_chart(extraction_data)
        
        return {
            'visualizations': visualizations,
            'extraction_data': extraction_data,
            'summary_stats': self._calculate_extraction_summary(extraction_data),
            'generated_at': datetime.now().isoformat()
        }
    
    def _analyze_real_extraction_data(self, df, group_column):
        """
        Analyze real extraction data from processed DataFrame - WITH DEBUG
        """
        print("🔍 DEBUG: Starting real extraction data analysis")
        print(f"   DataFrame shape: {df.shape}")
        print(f"   Group column: {group_column}")
        
        extraction_data = {}
        
        # Get extracted columns
        extracted_cols = [col for col in df.columns if col.startswith('extracted_')]
        print(f"🔍 DEBUG: Found extracted columns: {extracted_cols}")
        
        if not extracted_cols:
            print("🔍 DEBUG: No extracted columns found!")
            return {}
        
        for group in df[group_column].unique():
            if pd.isna(group):
                continue
                
            print(f"🔍 DEBUG: Processing group: {group}")
            group_data = df[df[group_column] == group]
            group_info = self.group_manager.get_group_info(group)
            
            if not group_info:
                print(f"   No group info found for {group}")
                continue
            
            mandatory_fields = self.group_manager.get_mandatory_fields(group)
            print(f"   Mandatory fields: {mandatory_fields}")
            
            # Calculate extraction rates
            field_stats = {}
            total_extractions = 0
            
            for field in mandatory_fields:
                field_col = f'extracted_{field}'
                print(f"   Checking field: {field} -> column: {field_col}")
                
                if field_col in group_data.columns:
                    # Count non-null AND non-empty values
                    non_null_count = group_data[field_col].notna().sum()
                    non_empty_count = group_data[field_col].astype(str).str.strip().ne('').sum()
                    
                    extracted_count = non_empty_count  # Use non-empty count
                    total_count = len(group_data)
                    success_rate = (extracted_count / total_count) * 100 if total_count > 0 else 0
                    
                    print(f"      Non-null: {non_null_count}, Non-empty: {non_empty_count}, Total: {total_count}")
                    print(f"      Success rate: {success_rate}%")
                    
                    field_stats[field] = {
                        'extracted_count': int(extracted_count),
                        'total_count': int(total_count),
                        'success_rate': round(success_rate, 1)
                    }
                    total_extractions += extracted_count
                else:
                    print(f"      Column {field_col} not found in data")
                    field_stats[field] = {
                        'extracted_count': 0,
                        'total_count': len(group_data),
                        'success_rate': 0.0
                    }
            
            # Overall group stats
            overall_success = (total_extractions / (len(mandatory_fields) * len(group_data))) * 100 if mandatory_fields and len(group_data) > 0 else 0
            
            extraction_data[group] = {
                'name': group_info['name'],
                'category': group_info.get('category', 'unknown'),
                'priority': group_info.get('priority_level', 'medium'),
                'total_records': len(group_data),
                'mandatory_fields': mandatory_fields,
                'field_stats': field_stats,
                'overall_success_rate': round(overall_success, 1),
                'total_extractions': int(total_extractions)
            }
            
            print(f"   Final group stats: {extraction_data[group]}")
        
        print(f"🔍 DEBUG: Final extraction_data keys: {list(extraction_data.keys())}")
        return extraction_data
    
    def _create_extraction_success_chart(self, extraction_data):
        """
        Create chart showing extraction success rates by group
        """
        try:
            fig, ax = plt.subplots(figsize=(14, 8))
            
            groups = []
            success_rates = []
            record_counts = []
            categories = []
            
            for group_key, data in extraction_data.items():
                groups.append(data['name'][:30])  # Truncate long names
                success_rates.append(data['overall_success_rate'])
                record_counts.append(data['total_records'])
                categories.append(data['category'])
            
            if not groups:
                return None
            
            # Create color map based on success rate
            colors = plt.cm.RdYlGn([rate/100 for rate in success_rates])
            
            bars = ax.barh(groups, success_rates, color=colors, alpha=0.8)
            ax.set_xlabel('Taxa de Sucesso da Extração (%)', fontweight='bold')
            ax.set_title('Taxa de Sucesso da Extração por Grupo de Produto', fontweight='bold', fontsize=14)
            ax.set_xlim(0, 100)
            
            # Add value labels and record counts
            for bar, rate, records in zip(bars, success_rates, record_counts):
                width = bar.get_width()
                ax.text(width + 2, bar.get_y() + bar.get_height()/2, 
                       f'{rate}%\n({records:,} reg)', ha='left', va='center', fontweight='bold', fontsize=9)
            
            # Add reference lines
            ax.axvline(x=80, color='green', linestyle='--', alpha=0.7, label='Meta (80%)')
            ax.axvline(x=60, color='orange', linestyle='--', alpha=0.7, label='Mínimo (60%)')
            ax.legend()
            
            plt.tight_layout()
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️ Error creating extraction success chart: {e}")
            return None
    
    def _create_mandatory_field_chart(self, extraction_data):
        """
        Create chart showing mandatory field extraction success - WITH DEBUG
        """
        try:
            print("🔍 DEBUG: Starting mandatory field chart creation")
            
            # Collect all mandatory fields and their success rates
            field_success = {}
            
            for group_key, group_data in extraction_data.items():
                print(f"🔍 DEBUG: Processing group {group_key}")
                print(f"   Group data keys: {list(group_data.keys())}")
                print(f"   Field stats: {group_data.get('field_stats', {})}")
                
                for field, stats in group_data['field_stats'].items():
                    print(f"   Field: {field}, Stats: {stats}")
                    
                    if field not in field_success:
                        field_success[field] = []
                    field_success[field].append(stats['success_rate'])
            
            print(f"🔍 DEBUG: Final field_success: {field_success}")
            
            if not field_success:
                print("🔍 DEBUG: No field success data found")
                return None
            
            # Calculate average success rate per field
            field_names = []
            avg_success = []
            
            for field, rates in field_success.items():
                field_names.append(field.replace('_', ' ').title())
                avg_rate = np.mean(rates)
                avg_success.append(avg_rate)
                print(f"🔍 DEBUG: Field {field}: rates={rates}, avg={avg_rate}")
            
            # Sort by success rate
            sorted_data = sorted(zip(field_names, avg_success), key=lambda x: x[1], reverse=True)
            field_names, avg_success = zip(*sorted_data)
            
            print(f"🔍 DEBUG: Final chart data:")
            for name, rate in zip(field_names, avg_success):
                print(f"   {name}: {rate}%")
            
            fig, ax = plt.subplots(figsize=(12, 8))
            
            colors = plt.cm.RdYlGn([rate/100 for rate in avg_success])
            bars = ax.bar(range(len(field_names)), avg_success, color=colors, alpha=0.8)
            
            ax.set_xlabel('Campos Obrigatórios', fontweight='bold')
            ax.set_ylabel('Taxa de Sucesso Média (%)', fontweight='bold')
            ax.set_title('Sucesso de Extração por Campo Obrigatório', fontweight='bold', fontsize=14)
            ax.set_xticks(range(len(field_names)))
            ax.set_xticklabels(field_names, rotation=45, ha='right')
            ax.set_ylim(0, 100)
            
            # Add value labels
            for bar, rate in zip(bars, avg_success):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 1,
                       f'{rate:.1f}%', ha='center', va='bottom', fontweight='bold')
            
            # Add reference lines
            ax.axhline(y=80, color='green', linestyle='--', alpha=0.7, label='Meta (80%)')
            ax.axhline(y=60, color='orange', linestyle='--', alpha=0.7, label='Mínimo (60%)')
            ax.legend()
            
            plt.tight_layout()
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"❌ Error creating mandatory field chart: {e}")
            import traceback
            print(traceback.format_exc())
            return None
    
    def _create_field_distribution_chart(self, extraction_data):
        """
        Create pie chart showing distribution of extraction success
        """
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
            
            # Chart 1: Records by group
            group_names = []
            record_counts = []
            
            for data in extraction_data.values():
                group_names.append(data['name'][:20])
                record_counts.append(data['total_records'])
            
            colors1 = plt.cm.Set3(np.linspace(0, 1, len(group_names)))
            wedges1, texts1, autotexts1 = ax1.pie(record_counts, labels=group_names, 
                                                  autopct='%1.1f%%', colors=colors1, startangle=90)
            ax1.set_title('Distribuição de Registros por Grupo', fontweight='bold', fontsize=14)
            
            # Chart 2: Success rate categories
            success_categories = {'Excelente (>80%)': 0, 'Bom (60-80%)': 0, 'Precisa Melhorar (<60%)': 0}
            
            for data in extraction_data.values():
                rate = data['overall_success_rate']
                if rate > 80:
                    success_categories['Excelente (>80%)'] += data['total_records']
                elif rate >= 60:
                    success_categories['Bom (60-80%)'] += data['total_records']
                else:
                    success_categories['Precisa Melhorar (<60%)'] += data['total_records']
            
            colors2 = ['#28a745', '#ffc107', '#dc3545']
            wedges2, texts2, autotexts2 = ax2.pie(success_categories.values(), 
                                                  labels=success_categories.keys(),
                                                  autopct='%1.1f%%', colors=colors2, startangle=90)
            ax2.set_title('Distribuição por Qualidade de Extração', fontweight='bold', fontsize=14)
            
            plt.tight_layout()
            return self._fig_to_base64(fig)
            
        except Exception as e:
            print(f"⚠️ Error creating field distribution chart: {e}")
            return None
    
    def _calculate_extraction_summary(self, extraction_data):
        """
        Calculate summary statistics from real extraction data
        """
        if not extraction_data:
            return {}
        
        total_records = sum(data['total_records'] for data in extraction_data.values())
        total_extractions = sum(data['total_extractions'] for data in extraction_data.values())
        success_rates = [data['overall_success_rate'] for data in extraction_data.values()]
        
        return {
            'total_groups': len(extraction_data),
            'total_records': total_records,
            'total_extractions': total_extractions,
            'average_success_rate': round(np.mean(success_rates), 1) if success_rates else 0,
            'best_group': max(extraction_data.items(), key=lambda x: x[1]['overall_success_rate'])[1]['name'] if extraction_data else '',
            'extraction_efficiency': round((total_extractions / total_records) * 100, 1) if total_records > 0 else 0
        }
    
    def _fig_to_base64(self, fig):
        """Convert matplotlib figure to base64 string"""
        img_buffer = io.BytesIO()
        fig.savefig(img_buffer, format='png', dpi=150, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        img_buffer.seek(0)
        img_string = base64.b64encode(img_buffer.read()).decode()
        plt.close(fig)
        return f"data:image/png;base64,{img_string}"
    
    def generate_extraction_report(self, processed_df, group_column='product_group'):
        """
        Generate complete extraction analysis report
        """
        print("🎨 Generating extraction analysis report...")
        
        if group_column not in processed_df.columns:
            return {
                'error': f"Column '{group_column}' not found in processed data",
                'suggestion': 'Ensure your data has been processed with product group classification'
            }
        
        try:
            analysis = self.create_extraction_analysis(processed_df, group_column)
            
            if 'error' in analysis:
                return analysis
            
            report = {
                'visualizations': analysis['visualizations'],
                'extraction_data': analysis['extraction_data'],
                'summary_stats': analysis['summary_stats'],
                'generated_at': datetime.now().isoformat(),
                'total_groups_analyzed': len(analysis['extraction_data']),
                'focus': 'real_extraction_results'
            }
            
            print("✅ Extraction analysis report generated successfully!")
            return report
            
        except Exception as e:
            print(f"❌ Error generating extraction report: {e}")
            return {
                'error': f"Failed to generate extraction report: {str(e)}",
                'suggestion': 'Check data format and ensure extraction has been completed'
            }


# Integration function for routes.py
def create_group_based_visualization_report(processed_df, product_group_manager, group_column='product_group'):
    """
    Helper function to create group-based visualization report
    """
    visualizer = GroupBasedDataVisualizer(product_group_manager)
    return visualizer.generate_extraction_report(processed_df, group_column)