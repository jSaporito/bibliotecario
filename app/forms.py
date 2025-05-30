from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed, FileRequired
from wtforms import StringField, SelectField, IntegerField, BooleanField, SubmitField
from wtforms.validators import DataRequired, NumberRange

class UploadForm(FlaskForm):
    """Enhanced upload form with analysis options"""
    
    file = FileField(
        'CSV File',
        validators=[
            FileRequired(message='Please select a CSV file'),
            FileAllowed(['csv'], message='Only CSV files allowed')
        ]
    )
    
    obs_column = StringField(
        'Text Column Name',
        validators=[DataRequired()],
        default='obs',
        description='Column containing text data to process'
    )
    
    chunk_size = IntegerField(
        'Processing Chunk Size',
        validators=[NumberRange(min=100, max=10000)],
        default=5000,
        description='Number of rows to process at once'
    )
    
    export_formats = SelectField(
        'Export Format',
        choices=[
            ('csv', 'CSV Only'), 
            ('excel', 'Excel Only'), 
            ('json', 'JSON Only'),
            ('both', 'CSV + Excel'), 
            ('all', 'All Formats (CSV + Excel + JSON)')
        ],
        default='both'
    )
    
    enable_cleaning = BooleanField(
        'Enable Advanced Text Cleaning',
        default=True,
        description='Remove noise, separators, and unwanted text'
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
    
    submit = SubmitField('Start Enhanced Processing')