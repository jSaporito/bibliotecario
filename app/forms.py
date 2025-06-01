from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed, FileRequired
from wtforms import StringField, SelectField, IntegerField, BooleanField, SubmitField
from wtforms.validators import DataRequired, NumberRange
from wtforms import SelectField
from core.product_groups import product_group_manager
class UploadForm(FlaskForm):
    """Enhanced upload form with analysis options"""
    validate_product_groups = BooleanField(
        'Validar Grupos de Produto',
        default=True,
        description='Verificar se os grupos de produto são válidos'
    )
    
    file = FileField(
        'Arquivo CSV',
        validators=[
            FileRequired(message='Selecione um arquivo CSV'),
            FileAllowed(['csv'], message='Apenas arquivos CSV são permitidos')
        ]
    )
    
    obs_column = StringField(
        'Nome da Coluna de Texto',
        validators=[DataRequired()],
        default='obs',
        description='Coluna contendo dados de texto para processar'
    )
    
    chunk_size = IntegerField(
        'Tamanho do Lote de Processamento',
        validators=[NumberRange(min=100, max=10000)],
        default=5000,
        description='Número de linhas para processar de uma vez'
    )
    
    export_formats = SelectField(
        'Formato de Exportação',
        choices=[
            ('csv', 'Apenas CSV'), 
            ('excel', 'Apenas Excel'), 
            ('json', 'Apenas JSON'),
            ('both', 'CSV + Excel'), 
            ('all', 'Todos os Formatos (CSV + Excel + JSON)')
        ],
        default='both'
    )
    
    enable_cleaning = BooleanField(
        'Habilitar Limpeza Avançada de Texto',
        default=True,
        description='Remover ruídos, separadores e texto indesejado'
    )
    
    enable_extraction = BooleanField(
        'Enable Field Extraction',
        default=True,
        description='Extract structured fields (IP, VLAN, Serial, etc.)'
    )
    
    extraction_mode = SelectField(
        'Extraction Mode',
        choices=[
            ('standard', 'Standard Fields'),
            ('comprehensive', 'All Available Fields'),
            ('network_only', 'Network Fields Only'),
            ('equipment_only', 'Equipment Fields Only')
        ],
        default='comprehensive',
        description='Choose which fields to extract'
    )
    
    submit = SubmitField('Iniciar Processamento Avançado')