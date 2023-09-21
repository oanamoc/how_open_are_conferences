from middleware import middleware_db
from infocom import infocom_db
from nsdi import nsdi_db

def make_databases():
    middleware_db()
    infocom_db()
    nsdi_db()

if __name__ == '__main__':
    make_databases()