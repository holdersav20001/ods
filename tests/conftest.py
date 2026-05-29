import pytest
from control.db import connect

@pytest.fixture
def conn():
    c = connect()
    c.autocommit = False
    try:
        yield c
    finally:
        c.rollback()   # every test isolated; no committed state leaks
        c.close()
