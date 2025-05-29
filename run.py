import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app

if __name__ == '__main__':
    app = create_app()
    
    app.run(
        debug=True,
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 5000))
    )
    

def debug_excepthook(exc_type, exc_value, exc_traceback):
    if 'Invalid format specifier' in str(exc_value):
        print(f'FORMAT ERROR: {exc_value}')
        traceback.print_exception(exc_type, exc_value, exc_traceback)
    sys.__excepthook__(exc_type, exc_value, exc_traceback)

sys.excepthook = debug_excepthook    