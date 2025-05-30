#!/usr/bin/env python3
"""
Script to translate all user-facing text in the Bibliotecario Flask application
from English to Portuguese (PT-BR).

This script will:
1. Process HTML templates and translate text content
2. Process JavaScript files and translate user-facing strings
3. Process Python files and translate flash messages, form labels, etc.
4. Create backup files before making changes
5. Generate a translation report

Usage: python translate_to_ptbr.py
"""

import os
import re
import shutil
from datetime import datetime
import json

class PTBRTranslator:
    def __init__(self):
        self.translations = {
            # Navigation and Headers
            'Bibliotecario': 'Bibliotecario',  # Keep brand name
            'CSV Text Processor': 'Processador de Texto CSV',
            'Home': 'Início',
            'Upload & Process': 'Carregar e Processar',
            'Processing': 'Processando',
            'Results': 'Resultados',
            'Error': 'Erro',
            'Analysis Preview': 'Visualização da Análise',
            'Business Impact Analysis': 'Análise de Impacto Empresarial',
            'Visual Metrics': 'Métricas Visuais',
            
            # Main page content
            'Transform unstructured CSV text data into organized and actionable information with the power of artificial intelligence': 
                'Transforme dados CSV não estruturados em informações organizadas e acionáveis com o poder da inteligência artificial',
            'Get Started Now': 'Começar Agora',
            'Start Now': 'Iniciar Agora',
            'How It Works': 'Como Funciona',
            'Ready to Process?': 'Pronto para Processar?',
            'Transform your data in seconds with our advanced technology': 
                'Transforme seus dados em segundos com nossa tecnologia avançada',
            'Start Processing': 'Iniciar Processamento',
            
            # Processing steps
            'Upload CSV': 'Carregar CSV',
            'Upload your CSV file with unstructured text data': 
                'Faça upload do seu arquivo CSV com dados de texto não estruturados',
            'Process & Clean': 'Processar e Limpar',
            'Advanced cleaning automatically removes noise and unnecessary separators': 
                'Limpeza avançada remove ruídos e separadores desnecessários automaticamente',
            'Extract Data': 'Extrair Dados',
            'Smart patterns identify and extract structured information': 
                'Padrões inteligentes identificam e extraem informações estruturadas',
            'Export Results': 'Exportar Resultados',
            'Download clean and organized data in multiple formats': 
                'Baixe dados limpos e organizados em múltiplos formatos',
            
            # Upload form
            'File Upload & Configuration': 'Upload de Arquivo e Configuração',
            'CSV File': 'Arquivo CSV',
            'Maximum file size: 100MB. Only CSV files are supported.': 
                'Tamanho máximo do arquivo: 100MB. Apenas arquivos CSV são suportados.',
            'Text Column Name': 'Nome da Coluna de Texto',
            'Column containing text data to process': 'Coluna contendo dados de texto para processar',
            'Processing Chunk Size': 'Tamanho do Lote de Processamento',
            'Number of rows to process at once': 'Número de linhas para processar de uma vez',
            'Export Format': 'Formato de Exportação',
            'CSV Only': 'Apenas CSV',
            'Excel Only': 'Apenas Excel',
            'JSON Only': 'Apenas JSON',
            'CSV + Excel': 'CSV + Excel',
            'All Formats (CSV + Excel + JSON)': 'Todos os Formatos (CSV + Excel + JSON)',
            'Processing Features': 'Recursos de Processamento',
            'Enable Advanced Text Cleaning': 'Habilitar Limpeza Avançada de Texto',
            'Remove noise, separators, and unwanted text': 'Remover ruídos, separadores e texto indesejado',
            'Start Enhanced Processing': 'Iniciar Processamento Avançado',
            'Analyze Sample First': 'Analisar Amostra Primeiro',
            "What's This?": 'O que é isso?',
            
            # Processing page
            'Processing Your File': 'Processando Seu Arquivo',
            'Please wait while we extract and organize your data': 
                'Aguarde enquanto extraímos e organizamos seus dados',
            'Processing Status': 'Status do Processamento',
            'Initializing...': 'Inicializando...',
            'Starting...': 'Iniciando...',
            'Processing time depends on file size and complexity. Large files may take several minutes.': 
                'O tempo de processamento depende do tamanho e complexidade do arquivo. Arquivos grandes podem levar vários minutos.',
            
            # Processing steps details
            'Processing Steps': 'Etapas do Processamento',
            'File Upload & Validation': 'Upload e Validação do Arquivo',
            'Text Cleaning & Sanitization': 'Limpeza e Sanitização do Texto',
            'Pattern Recognition': 'Reconhecimento de Padrões',
            'Data Extraction': 'Extração de Dados',
            'Validation & Formatting': 'Validação e Formatação',
            'Export Generation': 'Geração de Exportação',
            
            # Fields being extracted
            'Fields Being Extracted': 'Campos Sendo Extraídos',
            'Network Info': 'Informações de Rede',
            'IP Management': 'Gerenciamento de IP',
            'Gateway': 'Gateway',
            'IP Block': 'Bloco IP',
            'VLAN': 'VLAN',
            'ASN': 'ASN',
            'Hardware': 'Hardware',
            'MAC Address': 'Endereço MAC',
            'Serial Numbers': 'Números de Série',
            'CPE/Equipment': 'CPE/Equipamento',
            'Model Info': 'Informações do Modelo',
            'Interface Details': 'Detalhes da Interface',
            'Configuration': 'Configuração',
            'WiFi SSID': 'SSID WiFi',
            'WiFi Password': 'Senha WiFi',
            'PON Port': 'Porta PON',
            'ONU ID': 'ID ONU',
            '+15 more fields': '+15 mais campos',
            
            # Results page
            'Processing Completed Successfully!': 'Processamento Concluído com Sucesso!',
            'Rows Processed': 'Linhas Processadas',
            'Fields Extracted': 'Campos Extraídos',
            'Chunks Processed': 'Lotes Processados',
            'Export Formats': 'Formatos de Exportação',
            'Download Your Processed Data': 'Baixar Seus Dados Processados',
            'Files ready for download!': 'Arquivos prontos para download!',
            'Files will be available for 24 hours.': 'Arquivos estarão disponíveis por 24 horas.',
            'No download files available.': 'Nenhum arquivo de download disponível.',
            'Processing Summary': 'Resumo do Processamento',
            'Total Rows:': 'Total de Linhas:',
            'Fields Extracted:': 'Campos Extraídos:',
            'Processing Time:': 'Tempo de Processamento:',
            'Export Status:': 'Status da Exportação:',
            'Data Processing Summary': 'Resumo do Processamento de Dados',
            'Processing Details:': 'Detalhes do Processamento:',
            'Available Fields:': 'Campos Disponíveis:',
            'Common network configuration fields that were processed:': 
                'Campos comuns de configuração de rede que foram processados:',
            'Process Another File': 'Processar Outro Arquivo',
            'Go Back': 'Voltar',
            
            # Error messages
            'Page Not Found': 'Página Não Encontrada',
            "The page you're looking for doesn't exist.": 'A página que você procura não existe.',
            'File Too Large': 'Arquivo Muito Grande',
            'The file you\'re trying to upload exceeds the maximum size limit of 100MB.': 
                'O arquivo que você está tentando carregar excede o limite máximo de 100MB.',
            'Internal Server Error': 'Erro Interno do Servidor',
            'Something went wrong on our end. Please try again later.': 
                'Algo deu errado do nosso lado. Tente novamente mais tarde.',
            'Go Home': 'Ir para Início',
            'Upload File': 'Carregar Arquivo',
            
            # Analysis page
            'Large Sample Analysis Results': 'Resultados da Análise de Amostra Grande',
            'Analysis of': 'Análise de',
            'to optimize processing': 'para otimizar o processamento',
            'Business Impact Analysis Available': 'Análise de Impacto Empresarial Disponível',
            'Generate professional charts showing ROI, cost savings, efficiency gains, and competitive advantages.': 
                'Gere gráficos profissionais mostrando ROI, economia de custos, ganhos de eficiência e vantagens competitivas.',
            'Perfect for executive presentations and budget justification.': 
                'Perfeito para apresentações executivas e justificativa de orçamento.',
            'View Business Graphics': 'Ver Gráficos Empresariais',
            'Sample Information': 'Informações da Amostra',
            'Rows Analyzed': 'Linhas Analisadas',
            'Total File Rows': 'Total de Linhas do Arquivo',
            'Sample Data Size': 'Tamanho da Amostra de Dados',
            'Avg Text Length': 'Comprimento Médio do Texto',
            'Text Statistics': 'Estatísticas do Texto',
            'Average Length:': 'Comprimento Médio:',
            'characters': 'caracteres',
            'Median Length:': 'Comprimento Mediano:',
            'Max Length:': 'Comprimento Máximo:',
            'Average Lines:': 'Linhas Médias:',
            'lines': 'linhas',
            'Cleaning Impact': 'Impacto da Limpeza',
            'Text Reduction:': 'Redução do Texto:',
            'Space Saved:': 'Espaço Economizado:',
            'Reduction': 'Redução',
            
            # Visual metrics
            'Quantified Benefits of Bibliotecario Text Processing Solution': 
                'Benefícios Quantificados da Solução de Processamento de Texto Bibliotecario',
            'Time Reduction': 'Redução de Tempo',
            'Cost Savings': 'Economia de Custos',
            'ROI': 'ROI',
            'Payback': 'Retorno do Investimento',
            'Key Benefits': 'Principais Benefícios',
            'Key Business Benefits': 'Principais Benefícios Empresariais',
            'Data Processing Impact': 'Impacto do Processamento de Dados',
            'Total Rows': 'Total de Linhas',
            'Data Reduction': 'Redução de Dados',
            'Fields per Record': 'Campos por Registro',
            'Financial Impact': 'Impacto Financeiro',
            'Annual Savings': 'Economia Anual',
            'Months to Payback': 'Meses para Retorno',
            'Year 1 Net Benefit': 'Benefício Líquido do Ano 1',
            'Efficiency Improvements': 'Melhorias de Eficiência',
            'Processing Speed': 'Velocidade de Processamento',
            'Accuracy Improvement': 'Melhoria da Precisão',
            'Result Consistency': 'Consistência dos Resultados',
            'Investment Analysis': 'Análise de Investimento',
            'Investment Breakdown': 'Detalhamento do Investimento',
            'Software License (Annual)': 'Licença de Software (Anual)',
            'Training (One-time)': 'Treinamento (Uma vez)',
            'Setup & Integration': 'Configuração e Integração',
            'Total First Year': 'Total do Primeiro Ano',
            'Return Analysis': 'Análise de Retorno',
            'Monthly Savings': 'Economia Mensal',
            'Break-even Period': 'Período de Equilíbrio',
            '3-Year Net Benefit': 'Benefício Líquido de 3 Anos',
            'Competitive Advantages': 'Vantagens Competitivas',
            
            # JavaScript messages
            'Processing...': 'Processando...',
            'Uploading...': 'Carregando...',
            'Loading...': 'Carregando...',
            'Connection error. Please refresh the page.': 'Erro de conexão. Atualize a página.',
            'Processing completed successfully!': 'Processamento concluído com sucesso!',
            'Processing Failed': 'Processamento Falhou',
            'Try Again': 'Tente Novamente',
            'Selected:': 'Selecionado:',
            'File size exceeds 100MB limit. Please choose a smaller file.': 
                'O tamanho do arquivo excede o limite de 100MB. Escolha um arquivo menor.',
            'Please select a CSV file.': 'Selecione um arquivo CSV.',
            'Uploading file and starting processing. Please wait...': 
                'Carregando arquivo e iniciando processamento. Aguarde...',
            
            # Form validation messages
            'Please select a CSV file': 'Selecione um arquivo CSV',
            'Only CSV files allowed': 'Apenas arquivos CSV são permitidos',
            'Please fix the form errors and try again': 'Corrija os erros do formulário e tente novamente',
            'Invalid session': 'Sessão inválida',
            'Session not found': 'Sessão não encontrada',
            'Processing not completed': 'Processamento não concluído',
            'File not found': 'Arquivo não encontrado',
            
            # Success/Error messages
            'Analysis failed:': 'Análise falhou:',
            'Visual analysis failed:': 'Análise visual falhou:',
            'Please run analysis first before generating visual metrics': 
                'Execute a análise primeiro antes de gerar métricas visuais',
            'Session not found. Please run analysis first.': 
                'Sessão não encontrada. Execute a análise primeiro.',
            
            # Time and date formats
            'minutes': 'minutos',
            'hours': 'horas',
            'seconds': 'segundos',
            'Unknown': 'Desconhecido',
            
            # File formats
            'Format': 'Formato',
            'Download': 'Baixar',
            'CSV Format': 'Formato CSV',
            'EXCEL Format': 'Formato EXCEL',
            'JSON Format': 'Formato JSON',
            
            # Buttons and actions
            'Close': 'Fechar',
            'Save': 'Salvar',
            'Cancel': 'Cancelar',
            'Continue': 'Continuar',
            'Back to Home': 'Voltar ao Início',
            'Print': 'Imprimir',
            'Save Report': 'Salvar Relatório',
            'Print Analysis Results': 'Imprimir Resultados da Análise',
            'Presentation Mode': 'Modo Apresentação',
            
            # Additional business terms
            'faster than manual': 'mais rápido que manual',
            'higher accuracy': 'maior precisão',
            'consistent results': 'resultados consistentes',
            'processing capability': 'capacidade de processamento',
            'reduction in human errors': 'redução em erros humanos',
            'Faster time-to-insight for business decisions': 'Tempo mais rápido para insights em decisões empresariais',
            'Higher data quality and consistency': 'Maior qualidade e consistência dos dados',
            'Scalable solution for growing data volumes': 'Solução escalável para volumes crescentes de dados',
            'Reduced dependency on manual processes': 'Dependência reduzida de processos manuais',
            
            # Footer
            'Developed by João Paulo Saporito': 'Desenvolvido por João Paulo Saporito',
            'This code is subject to the licensing agreement between João Paulo Saporito and Alloha Fibra': 
                'Este código está sujeito ao acordo de licenciamento entre João Paulo Saporito e Alloha Fibra',
        }
        
        # Patterns for different types of content
        self.html_patterns = [
            # Title tags
            (r'<title>([^<]+)</title>', self._translate_title),
            # Button text
            (r'<button[^>]*>([^<]+)</button>', self._translate_button),
            # Link text
            (r'<a[^>]*>([^<]*<i[^>]*></i>[^<]*)</a>', self._translate_link_with_icon),
            (r'<a[^>]*>([^<]+)</a>', self._translate_link),
            # Heading tags
            (r'<h[1-6][^>]*>([^<]*<i[^>]*></i>[^<]*)</h[1-6]>', self._translate_heading_with_icon),
            (r'<h[1-6][^>]*>([^<]+)</h[1-6]>', self._translate_heading),
            # Paragraph text
            (r'<p[^>]*>([^<]+)</p>', self._translate_paragraph),
            # Label text
            (r'<label[^>]*>([^<]+)</label>', self._translate_label),
            # Span text (but not with classes that might be dynamic)
            (r'<span(?![^>]*class=["\'][^"\']*badge[^"\']*["\'])[^>]*>([^<]+)</span>', self._translate_span),
            # Small text
            (r'<small[^>]*>([^<]+)</small>', self._translate_small),
            # Strong text
            (r'<strong>([^<]+)</strong>', self._translate_strong),
            # Option values
            (r'<option[^>]*>([^<]+)</option>', self._translate_option),
            # Alert content
            (r'<div[^>]*class=["\'][^"\']*alert[^"\']*["\'][^>]*>([^<]+)</div>', self._translate_alert),
        ]
        
        self.js_patterns = [
            # String literals in quotes
            (r"'([^']*(?:Processing|Loading|Error|Success|Failed|Complete|Upload|Download|Select|File|Please)[^']*)'", self._translate_js_string),
            (r'"([^"]*(?:Processing|Loading|Error|Success|Failed|Complete|Upload|Download|Select|File|Please)[^"]*)"', self._translate_js_string),
            # Alert and console messages
            (r'alert\(["\']([^"\']+)["\']\)', self._translate_alert_message),
            (r'console\.log\(["\']([^"\']+)["\']\)', self._keep_console_log),
        ]
        
        self.backup_dir = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.translation_report = []
        
    def _translate_text(self, text):
        """Translate text using the translations dictionary"""
        # Remove extra whitespace and normalize
        text = ' '.join(text.split())
        
        # Direct translation
        if text in self.translations:
            return self.translations[text]
        
        # Try partial matches for longer texts
        for english, portuguese in self.translations.items():
            if english.lower() in text.lower():
                return text.replace(english, portuguese)
        
        return text
    
    def _translate_title(self, match):
        original = match.group(1)
        translated = self._translate_text(original)
        if translated != original:
            self.translation_report.append(f"Title: '{original}' -> '{translated}'")
        return f'<title>{translated}</title>'
    
    def _translate_button(self, match):
        original = match.group(1).strip()
        if '<i' in original:  # Has icon
            return match.group(0)  # Handle separately
        translated = self._translate_text(original)
        if translated != original:
            self.translation_report.append(f"Button: '{original}' -> '{translated}'")
        return match.group(0).replace(original, translated)
    
    def _translate_link_with_icon(self, match):
        return match.group(0)  # Keep as is for now, too complex to parse reliably
    
    def _translate_link(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        if translated != original:
            self.translation_report.append(f"Link: '{original}' -> '{translated}'")
        return match.group(0).replace(original, translated)
    
    def _translate_heading_with_icon(self, match):
        return match.group(0)  # Keep complex headings as is
    
    def _translate_heading(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        if translated != original:
            self.translation_report.append(f"Heading: '{original}' -> '{translated}'")
        return match.group(0).replace(original, translated)
    
    def _translate_paragraph(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        if translated != original:
            self.translation_report.append(f"Paragraph: '{original}' -> '{translated}'")
        return match.group(0).replace(original, translated)
    
    def _translate_label(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        if translated != original:
            self.translation_report.append(f"Label: '{original}' -> '{translated}'")
        return match.group(0).replace(original, translated)
    
    def _translate_span(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        return match.group(0).replace(original, translated)
    
    def _translate_small(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        return match.group(0).replace(original, translated)
    
    def _translate_strong(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        return match.group(0).replace(original, translated)
    
    def _translate_option(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        return match.group(0).replace(original, translated)
    
    def _translate_alert(self, match):
        original = match.group(1).strip()
        translated = self._translate_text(original)
        return match.group(0).replace(original, translated)
    
    def _translate_js_string(self, match):
        original = match.group(1)
        translated = self._translate_text(original)
        if translated != original:
            self.translation_report.append(f"JS String: '{original}' -> '{translated}'")
        return match.group(0).replace(original, translated)
    
    def _translate_alert_message(self, match):
        original = match.group(1)
        translated = self._translate_text(original)
        return match.group(0).replace(original, translated)
    
    def _keep_console_log(self, match):
        return match.group(0)  # Keep console.log as is
    
    def create_backup(self, file_path):
        """Create backup of original file"""
        if not os.path.exists(self.backup_dir):
            os.makedirs(self.backup_dir)
        
        relative_path = file_path
        backup_path = os.path.join(self.backup_dir, relative_path.replace('/', '_').replace('\\', '_'))
        
        # Create backup directory structure if needed
        backup_dir = os.path.dirname(backup_path)
        if backup_dir and not os.path.exists(backup_dir):
            os.makedirs(backup_dir, exist_ok=True)
        
        shutil.copy2(file_path, backup_path)
        print(f"✅ Backup created: {backup_path}")
    
    def translate_html_file(self, file_path):
        """Translate HTML template file"""
        print(f"🔄 Translating HTML: {file_path}")
        
        # Create backup
        self.create_backup(file_path)
        
        # Read file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply translations
        for pattern, translator in self.html_patterns:
            content = re.sub(pattern, translator, content, flags=re.IGNORECASE | re.MULTILINE)
        
        # Manual translations for complex cases
        content = self._manual_html_translations(content)
        
        # Write translated content
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Translated: {file_path}")
            return True
        else:
            print(f"ℹ️  No changes needed: {file_path}")
            return False
    
    def translate_js_file(self, file_path):
        """Translate JavaScript file"""
        print(f"🔄 Translating JS: {file_path}")
        
        # Create backup
        self.create_backup(file_path)
        
        # Read file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply translations
        for pattern, translator in self.js_patterns:
            content = re.sub(pattern, translator, content, flags=re.MULTILINE)
        
        # Manual translations for JavaScript
        content = self._manual_js_translations(content)
        
        # Write translated content
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Translated: {file_path}")
            return True
        else:
            print(f"ℹ️  No changes needed: {file_path}")
            return False
    
    def translate_py_file(self, file_path):
        """Translate Python file (flash messages, form labels, etc.)"""
        print(f"🔄 Translating Python: {file_path}")
        
        # Create backup
        self.create_backup(file_path)
        
        # Read file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Translate flash messages
        for english, portuguese in self.translations.items():
            # Flash messages
            content = re.sub(
                rf"flash\(['\"]({re.escape(english)})['\"]",
                f"flash('{portuguese}'",
                content
            )
            
            # String literals in general
            content = re.sub(
                rf"['\"]({re.escape(english)})['\"]",
                f"'{portuguese}'",
                content
            )
        
        # Write translated content
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✅ Translated: {file_path}")
            return True
        else:
            print(f"ℹ️  No changes needed: {file_path}")
            return False
    
    def _manual_html_translations(self, content):
        """Manual translations for complex HTML cases"""
        manual_replacements = {
            # Block replacements
            'What happens next?': 'O que acontece a seguir?',
            'File validation and upload': 'Validação e upload do arquivo',
            'Text cleaning and sanitization': 'Limpeza e sanitização do texto',
            'Pattern recognition and extraction': 'Reconhecimento de padrões e extração',
            'Data validation and formatting': 'Validação e formatação dos dados',
            'Export to selected formats': 'Exportar para formatos selecionados',
            'Extracted Fields:': 'Campos Extraídos:',
            'IP Addresses': 'Endereços IP',
            'VLANs': 'VLANs',
            'MAC Addresses': 'Endereços MAC',
            'Serial Numbers': 'Números de Série',
            'Gateways': 'Gateways',
            'CPE Info': 'Informações CPE',
            '+20 more': '+20 mais',
        }
        
        for english, portuguese in manual_replacements.items():
            content = content.replace(english, portuguese)
        
        return content
    
    def _manual_js_translations(self, content):
        """Manual translations for JavaScript"""
        manual_replacements = {
            'Uploading & Starting Process...': 'Carregando e Iniciando Processo...',
            'Processing completed successfully!': 'Processamento concluído com sucesso!',
            'Try Again': 'Tente Novamente',
            'Connection error. Please refresh the page.': 'Erro de conexão. Atualize a página.',
        }
        
        for english, portuguese in manual_replacements.items():
            content = content.replace(f"'{english}'", f"'{portuguese}'")
            content = content.replace(f'"{english}"', f'"{portuguese}"')
        
        return content
    
    def translate_all_files(self):
        """Translate all files in the project"""
        print("🚀 Starting PT-BR translation process...")
        
        files_to_translate = [
            # HTML templates
            'templates/base.html',
            'templates/index.html',
            'templates/upload.html',
            'templates/processing.html',
            'templates/results.html',
            'templates/error.html',
            'templates/analysis_preview.html',
            'templates/visual_metrics.html',
            
            # JavaScript files
            'static/js/custom.js',
            
            # Python files (be careful with these)
            'app/forms.py',
            'app/routes.py',
        ]
        
        translated_count = 0
        
        for file_path in files_to_translate:
            if not os.path.exists(file_path):
                print(f"⚠️  File not found: {file_path}")
                continue
            
            try:
                if file_path.endswith('.html'):
                    if self.translate_html_file(file_path):
                        translated_count += 1
                elif file_path.endswith('.js'):
                    if self.translate_js_file(file_path):
                        translated_count += 1
                elif file_path.endswith('.py'):
                    if self.translate_py_file(file_path):
                        translated_count += 1
            except Exception as e:
                print(f"❌ Error translating {file_path}: {str(e)}")
        
        # Generate translation report
        self.generate_report(translated_count)
        
        print(f"🎉 Translation completed! {translated_count} files were modified.")
        print(f"📁 Backups saved to: {self.backup_dir}")
    
    def generate_report(self, translated_count):
        """Generate translation report"""
        report_path = f"translation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("BIBLIOTECARIO PT-BR TRANSLATION REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Translation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Files Modified: {translated_count}\n")
            f.write(f"Backup Directory: {self.backup_dir}\n\n")
            
            f.write("TRANSLATIONS APPLIED:\n")
            f.write("-" * 30 + "\n")
            for translation in self.translation_report:
                f.write(f"{translation}\n")
            
            f.write(f"\nTOTAL TRANSLATIONS: {len(self.translation_report)}\n")
            
        print(f"📄 Translation report saved: {report_path}")
    
    def add_custom_translation(self, english, portuguese):
        """Add custom translation to the dictionary"""
        self.translations[english] = portuguese
        print(f"➕ Added custom translation: '{english}' -> '{portuguese}'")
    
    def preview_translations(self, file_path):
        """Preview what translations would be applied to a file"""
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return
        
        print(f"🔍 Preview translations for: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        preview_count = 0
        for english, portuguese in self.translations.items():
            if english in content and english != portuguese:
                print(f"  '{english}' -> '{portuguese}'")
                preview_count += 1
        
        if preview_count == 0:
            print("  No translations found for this file.")
        else:
            print(f"  {preview_count} potential translations found.")


def main():
    """Main function to run the translator"""
    print("🌐 BIBLIOTECARIO PT-BR TRANSLATOR")
    print("=" * 40)
    
    translator = PTBRTranslator()
    
    # Interactive mode
    while True:
        print("\nChoose an option:")
        print("1. Translate all files")
        print("2. Preview translations for specific file")
        print("3. Add custom translation")
        print("4. Translate specific file")
        print("5. Exit")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == '1':
            # Confirm before proceeding
            confirm = input("\n⚠️  This will modify your files. Backups will be created. Continue? (y/N): ")
            if confirm.lower() == 'y':
                translator.translate_all_files()
            else:
                print("❌ Translation cancelled.")
        
        elif choice == '2':
            file_path = input("Enter file path: ").strip()
            translator.preview_translations(file_path)
        
        elif choice == '3':
            english = input("Enter English text: ").strip()
            portuguese = input("Enter Portuguese translation: ").strip()
            if english and portuguese:
                translator.add_custom_translation(english, portuguese)
            else:
                print("❌ Both texts are required.")
        
        elif choice == '4':
            file_path = input("Enter file path: ").strip()
            if not os.path.exists(file_path):
                print(f"❌ File not found: {file_path}")
                continue
            
            try:
                if file_path.endswith('.html'):
                    if translator.translate_html_file(file_path):
                        print("✅ File translated successfully!")
                    else:
                        print("ℹ️  No changes were needed.")
                elif file_path.endswith('.js'):
                    if translator.translate_js_file(file_path):
                        print("✅ File translated successfully!")
                    else:
                        print("ℹ️  No changes were needed.")
                elif file_path.endswith('.py'):
                    if translator.translate_py_file(file_path):
                        print("✅ File translated successfully!")
                    else:
                        print("ℹ️  No changes were needed.")
                else:
                    print("❌ Unsupported file type. Only .html, .js, and .py files are supported.")
            except Exception as e:
                print(f"❌ Error translating file: {str(e)}")
        
        elif choice == '5':
            print("👋 Goodbye!")
            break
        
        else:
            print("❌ Invalid choice. Please try again.")


if __name__ == "__main__":
    main()