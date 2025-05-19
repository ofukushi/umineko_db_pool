

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Load .env on import
load_dotenv()

def detect_environment():
    is_heroku = os.getenv("HEROKU_ENV", "false").lower() == "true"
    is_render = os.getenv("RENDER_ENV", "false").lower() == "true"
    is_local = not is_heroku and not is_render
    return is_local, is_heroku, is_render

def get_database_engine():
    is_local, is_heroku, is_render = detect_environment()

    if is_heroku:
        db_url = os.getenv('HEROKU_DATABASE_URL')
        environment = "Heroku"
    elif is_render:
        db_url = os.getenv('RENDER_DATABASE_URL') 
        # db_url = os.getenv('External_RENDER_DATABASE_URL') # For local testing
        environment = "Render"
    else:
        db_url = os.getenv('LOCAL_DATABASE_URL')
        environment = "Local"

    if not db_url:
        raise ValueError("No database URL found in environment variables.")

    db_url = db_url.replace('postgres://', 'postgresql+psycopg2://')
    engine = create_engine(db_url)
    return engine, environment

def get_table_names():
    is_local, is_heroku, is_render = detect_environment()

    if is_heroku:
        prefix = 'heroku_fins_all'
    elif is_render:
        prefix = 'render_fins_all'
    else:
        prefix = 'fins_all'

    return {
        'fins_all': prefix,
        'fins_all_adjusted': f'{prefix}_adjusted',
        'fins_all_bps_opvalues': f'{prefix}_bps_opvalues',
        'fins_all_netsales': f'{prefix}_netsales'
    }

