from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

USER = ""
PASSWORD = ""
PORT = ""
DATABASE_NAME = ""

# Database connection string
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@localhost:{PORT}/{DATABASE_NAME}"

# Create the SQLAlchemy engine (handles the actual DB connection pool)
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

# Create a configured session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Dependency function that provides a DB session
def get_orkuflaedi_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
