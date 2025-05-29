import os
from datetime import timedelta

class Config:
    SECRET_KEY = '94057dcfa25f20dc4fe94576381e10115ffc907ae506b2d7'
    
    # File upload settings
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size
    UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
    DOWNLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloads')
    ALLOWED_EXTENSIONS = {'csv'}
    
    # Processing settings
    CHUNK_SIZE = 5000  # Rows per chunk for processing
    MAX_PROCESSING_TIME = 3600  # 1 hour max processing time
    
    # Logging
    LOG_FOLDER = 'logs'
    LOG_LEVEL = 'INFO'
    
    # Session settings
    PERMANENT_SESSION_LIFETIME = timedelta(hours=2)

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    LOG_LEVEL = 'DEBUG'

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    LOG_LEVEL = 'WARNING'

class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    WTF_CSRF_ENABLED = False

# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}