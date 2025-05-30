from flask import Flask
import os
import logging

def create_app():
    """Simple Flask app factory"""
    app = Flask(__name__, 
                template_folder='../templates',  
                static_folder='../static')        
    
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    app.config.update({
        'UPLOAD_FOLDER': os.path.join(project_root, 'uploads'),
        'DOWNLOAD_FOLDER': os.path.join(project_root, 'downloads'),
        'MAX_CONTENT_LENGTH': 100 * 1024 * 1024,  # 100MB
        'SECRET_KEY': 'dev-secret-key-change-in-production'
    })
    
    for folder in [app.config['UPLOAD_FOLDER'], app.config['DOWNLOAD_FOLDER']]:
        os.makedirs(folder, exist_ok=True)
    
    logging.basicConfig(level=logging.INFO)
    
    from app.routes import bp
    app.register_blueprint(bp)
    
    return app