from flask import Flask
import os
import logging

def create_app():
    """Simple Flask app factory"""
    # Create Flask app with correct template folder
    app = Flask(__name__, 
                template_folder='../templates',  # Point to templates folder
                static_folder='../static')        # Point to static folder
    
    # Basic config
    app.config.update({
        'UPLOAD_FOLDER': 'uploads',
        'DOWNLOAD_FOLDER': 'downloads',
        'MAX_CONTENT_LENGTH': 100 * 1024 * 1024,  # 100MB
        'SECRET_KEY': 'dev-secret-key-change-in-production'
    })
    
    # Create directories
    for folder in [app.config['UPLOAD_FOLDER'], app.config['DOWNLOAD_FOLDER']]:
        os.makedirs(folder, exist_ok=True)
    
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Register routes
    from app.routes import bp
    app.register_blueprint(bp)
    
    return app