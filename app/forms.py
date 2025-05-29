from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed, FileRequired
from wtforms import StringField, SelectField, IntegerField, BooleanField, SubmitField
from wtforms.validators import DataRequired, NumberRange

class UploadForm(FlaskForm):
    """Simple upload form"""
    
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
        default='obs'
    )
    
    chunk_size = IntegerField(
        'Chunk Size',
        validators=[NumberRange(min=100, max=10000)],
        default=5000
    )
    
    export_formats = SelectField(
        'Export Format',
        choices=[('csv', 'CSV'), ('excel', 'Excel'), ('both', 'Both')],
        default='both'
    )
    
    # ✅ Added the missing field that templates expect
    enable_cleaning = BooleanField(
        'Enable Text Cleaning',
        default=True,
        description='Remove line separators and noise from text'
    )
    
    submit = SubmitField('Process File')