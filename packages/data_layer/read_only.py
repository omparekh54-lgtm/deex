"""Enforce read-only transactions for every connection in a verification pool."""
from sqlalchemy import event


def readonly_connections(engine):
    @event.listens_for(engine, 'connect')
    def pin_readonly(dbapi, _):
        with dbapi.cursor() as cursor:
            cursor.execute('SET default_transaction_read_only=on')
        dbapi.commit()
