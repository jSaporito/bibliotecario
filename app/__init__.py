from flask import Flask
import os
import logging
from datetime import datetime

import sys
import traceback

def debug_excepthook(exc_type, exc_value, exc_traceback):
    if 'Invalid format specifier' in str(exc_value):
        print(f'FORMAT ERROR: {exc_value}')
        traceback.print_exception(exc_type, exc_value, exc_traceback)
    sys.__excepthook__(exc_type, exc_value, exc_traceback)

sys.excepthook = debug_excepthook

def create_app(config_name=None):
    """Create and configure the Flask application with enhanced error handling"""
    
    # Create Flask app with explicit template folder
    template_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'templates'))
    static_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'static'))
    
    app = Flask(__name__, 
                template_folder=template_dir,
                static_folder=static_dir)
    
    try:
        from config import config
        if config_name is None:
            config_name = 'development'
        
        app.config.from_object(config[config_name])
    except ImportError as e:
        print(f"Warning: Could not import config: {e}")
        # Fallback configuration
        app.config.update({
            'UPLOAD_FOLDER': os.path.join(os.getcwd(), 'uploads'),
            'DOWNLOAD_FOLDER': os.path.join(os.getcwd(), 'downloads'),
            'LOG_FOLDER': os.path.join(os.getcwd(), 'logs'),
            'MAX_CONTENT_LENGTH': 100 * 1024 * 1024,  # 100MB max file size
            'SECRET_KEY': 'dev-secret-key-change-in-production'
        })
    
    # Ensure all required config keys exist
    required_config = {
        'UPLOAD_FOLDER': os.path.join(os.getcwd(), 'uploads'),
        'DOWNLOAD_FOLDER': os.path.join(os.getcwd(), 'downloads'),
        'LOG_FOLDER': os.path.join(os.getcwd(), 'logs'),
        'MAX_CONTENT_LENGTH': 100 * 1024 * 1024,
    }
    
    for key, default_value in required_config.items():
        if key not in app.config:
            app.config[key] = default_value
    
    # Create necessary directories with error handling
    directories_to_create = [
        app.config['UPLOAD_FOLDER'],
        app.config['DOWNLOAD_FOLDER'],
        app.config['LOG_FOLDER']
    ]
    
    for directory in directories_to_create:
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"✓ Directory ready: {directory}")
        except OSError as e:
            print(f"✗ Failed to create directory {directory}: {e}")
            # Try to create in current working directory as fallback
            fallback_dir = os.path.join(os.getcwd(), os.path.basename(directory))
            try:
                os.makedirs(fallback_dir, exist_ok=True)
                app.config[directory] = fallback_dir
                print(f"✓ Fallback directory created: {fallback_dir}")
            except OSError:
                print(f"✗ Could not create fallback directory either")
    
    # Setup logging
    setup_logging(app)
    
    # Test core module imports
    try:
        from core.data_processor import DataProcessor
        from core.text_extractor import TextExtractor
        from core.text_cleaner import TextCleaner
        from core.export_handler import ExportHandler
        
        # Test that classes can be instantiated
        test_extractor = TextExtractor()
        test_cleaner = TextCleaner()
        
        app.logger.info("✓ All core modules imported successfully")
        app.logger.info(f"✓ Available fields: {len(test_extractor.get_field_list())}")
        
    except ImportError as e:
        app.logger.error(f"✗ Failed to import core modules: {e}")
        print(f"Import Error: {e}")
        print("Please check that all core modules are properly saved and have no syntax errors")
    except Exception as e:
        app.logger.error(f"✗ Error testing core modules: {e}")
        print(f"Module Test Error: {e}")
    
    # Register blueprints with error handling
    try:
        from app import routes
        app.register_blueprint(routes.bp)
        app.logger.info("✓ Routes blueprint registered successfully")
    except ImportError as e:
        app.logger.error(f"✗ Failed to import routes: {e}")
        print(f"Routes Import Error: {e}")
    except Exception as e:
        app.logger.error(f"✗ Error registering routes: {e}")
        print(f"Routes Registration Error: {e}")
    
    # Add some useful template globals
    @app.template_global()
    def get_current_year():
        return datetime.now().year
    
    @app.template_global()
    def get_app_version():
        try:
            from core import get_version
            return get_version()
        except:
            return "2.0.0"
    
    # Add error handlers
    @app.errorhandler(413)
    def file_too_large(error):
        app.logger.warning("File upload too large")
        return "File too large. Maximum size is 100MB.", 413
    
    @app.errorhandler(500)
    def internal_server_error(error):
        app.logger.error(f"Internal server error: {error}")
        return "Internal server error occurred.", 500
    
    # Add a health check route
    @app.route('/health')
    def health_check():
        try:
            # Test core functionality
            from core.text_extractor import TextExtractor
            extractor = TextExtractor()
            test_result = extractor.process_text("test text")
            
            return {
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'core_modules': 'working',
                'available_fields': len(extractor.get_field_list())
            }, 200
        except Exception as e:
            return {
                'status': 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }, 500
    
    app.logger.info(f"Flask app created successfully with config: {config_name}")
    app.logger.info(f"Upload folder: {app.config.get('UPLOAD_FOLDER')}")
    app.logger.info(f"Download folder: {app.config.get('DOWNLOAD_FOLDER')}")
    
    return app

def setup_logging(app):
    """Setup logging for the application"""
    try:
        # Create logs directory if it doesn't exist
        log_dir = app.config.get('LOG_FOLDER', 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        # Setup file handler
        log_file = os.path.join(log_dir, 'bibliotecario.log')
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Setup formatter
        formatter = logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to app logger
        app.logger.addHandler(file_handler)
        app.logger.setLevel(logging.INFO)
        
        # Also setup console logging for development
        if app.config.get('DEBUG', False):
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            app.logger.addHandler(console_handler)
        
        app.logger.info("Logging setup completed")
        
    except Exception as e:
        print(f"Failed to setup logging: {e}")

# Optional: Add a function to test the app setup
def test_app_setup():
    """Test function to verify app setup"""
    try:
        app = create_app('development')
        with app.app_context():
            print("✓ App created successfully")
            print(f"✓ Upload folder: {app.config['UPLOAD_FOLDER']}")
            print(f"✓ Download folder: {app.config['DOWNLOAD_FOLDER']}")
            
            # Test core imports
            from core.data_processor import DataProcessor
            processor = DataProcessor()
            print("✓ DataProcessor can be instantiated")
            
            return True
    except Exception as e:
        print(f"✗ App setup test failed: {e}")
        return False

if __name__ == "__main__":
    # Allow testing the setup directly
    test_app_setup()