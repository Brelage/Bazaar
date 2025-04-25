from database_engine import SessionLocal
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from contextlib import contextmanager


@contextmanager
def session_query():
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def session_commit():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def bulk_upsert(ORM, data):
    """
    upsert multiple rows into a given table with a composite primary key.

    Args:
    ORM: SQLAlchemy ORM class
    data: list of dictionaries to be upserted into the database
    """
    ## 
    table = ORM.__table__
    if isinstance(data, dict):
        data = [data]

    # Get primary key column names
    primary_keys = [col.name for col in table.primary_key.columns]
    # Columns to update (all except primary keys)
    update_columns = [col for col in data[0].keys() if col not in primary_keys]
    
    with session_commit() as session:
        dialect_name = session.bind.dialect.name

        ## PostgreSQL specific upsert logic
        if dialect_name == "postgresql":
            stmt = pg_insert(table).values(data)
            stmt = stmt.on_conflict_do_update(
                index_elements=primary_keys,
                set_={col: getattr(stmt.excluded, col) for col in update_columns}
            )
        
        ## MySQL specific upsert logic
        elif dialect_name == "mysql":
            stmt = mysql_insert(table).values(data)
            stmt = stmt.on_duplicate_key_update(
                {col: getattr(stmt.inserted, col) for col in update_columns}
            )
        
        ## SQLite specific upsert logic
        elif dialect_name == "sqlite":
            stmt = sqlite_insert(table).values(data)
            stmt = stmt.on_conflict_do_update(
                index_elements=primary_keys,
                set_={col: getattr(stmt.excluded, col) for col in update_columns}
            )
        
        # Fallback: generic upsert (slow, not bulk)
        else:
            for row in data:
                # Try update, if rowcount==0 then insert
                update_stmt = table.update().where(
                    *(getattr(table.c, col) == row[col] for col in primary_keys)
                ).values({col: row[col] for col in update_columns})
                result = session.execute(update_stmt)
                
                if result.rowcount == 0:
                    session.execute(table.insert().values(**row))
            return

        # For supported dialects, do bulk upsert
        session.execute(stmt)