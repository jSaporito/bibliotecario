from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed, FileRequired
from wtforms import StringField, SelectField, IntegerField, BooleanField, SubmitField
from wtforms.validators import DataRequired, NumberRange, Optional

class UploadForm(FlaskForm):
    """Formulário para upload de CSV e opções de processamento"""
    
    file = FileField(
        'Arquivo CSV',
        validators=[
            FileRequired(message='Por favor, selecione um arquivo CSV'),
            FileAllowed(['csv'], message='Apenas arquivos CSV são permitidos')
        ]
    )
    
    obs_column = StringField(
        'Nome da Coluna de Texto',
        validators=[DataRequired()],
        default='obs',
        description='Nome da coluna que contém o texto para processar'
    )
    
    chunk_size = IntegerField(
        'Tamanho do Lote',
        validators=[NumberRange(min=100, max=10000)],
        default=5000,
        description='Número de linhas para processar por vez (100-10000)'
    )
    
    enable_cleaning = BooleanField(
        'Ativar Limpeza de Texto',
        default=True,
        description='Remover linhas separadoras e ruídos do texto'
    )
    
    export_formats = SelectField(
        'Formatos de Exportação',
        choices=[
            ('csv', 'Apenas CSV'),
            ('excel', 'Apenas Excel'),
            ('both', 'CSV e Excel'),
            ('all', 'CSV, Excel e JSON')
        ],
        default='both'
    )
    
    submit = SubmitField('Processar Arquivo')

class ColumnSelectionForm(FlaskForm):
    """Formulário para seleção de colunas a extrair"""
    
    # Será populado dinamicamente baseado nos campos disponíveis
    selected_fields = SelectField(
        'Campos para Extrair',
        choices=[],
        description='Selecione quais campos extrair do texto'
    )
    
    submit = SubmitField('Atualizar Seleção')