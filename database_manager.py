#!/usr/bin/env python
"""Multicustomer microservice database manager."""
import json
import os
import psycopg2
import psycopg2.extras
import random
import string
import sys
import tornado.ioloop
import tornado.web

from psycopg2 import sql

# TODO: Add logging
# TODO: Split in different files
# TODO: Change exception management

DATABASE_HOST = os.environ.get("DATABASE_HOST", "localhost")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "postgres")
DATABASE_USER = os.environ.get("DATABASE_USER", "postgres")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DATABASE_ENCODING = os.environ.get("DATABASE_ENCODING", "UTF-8")

LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8888"))

DEFAULT_PASSWORD_LENGTH = 40
CUSTOMER_PREFIX = os.environ.get("CUSTOMER_PREFIX", "customer_")
SERVICE_PREFIX = os.environ.get("SERVICE_PREFIX", "service_")

__version__ = '1.0'


# Connection management
def __create_conn(database=DATABASE_NAME):
    conn = psycopg2.connect(dbname=database,
                            user=DATABASE_USER,
                            host=DATABASE_HOST,
                            password=DATABASE_PASSWORD)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


CONN = {}


def __get_conn(database=DATABASE_NAME):
    global CONN
    if database not in CONN:
        CONN[database] = __create_conn(database)
    try:
        CONN[database].cursor().execute('SELECT 1')
    except psycopg2.OperationalError:
        CONN[database] = __create_conn(database)

    return CONN[database]


def __create_random_password(length=DEFAULT_PASSWORD_LENGTH):
    password_chars = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.SystemRandom().choice(password_chars)
                   for _ in range(length))


# Customer management
def get_customers():
    """Get database customers."""
    conn = __get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT datname FROM pg_database')
    return [x['datname'].replace(CUSTOMER_PREFIX, "")
            for x in cur.fetchall()
            if x['datname'].startswith(CUSTOMER_PREFIX)]


def create_customer_database(customer):
    """Create customer database with all service schemas."""
    conn = __get_conn()
    cur = conn.cursor()
    changed = False
    try:
        cur.execute(
            sql.SQL('CREATE DATABASE {database} ENCODING %s')
            .format(database=sql.Identifier(CUSTOMER_PREFIX + customer)),
            (DATABASE_ENCODING,))
        changed = True
    except Exception as e:
        raise e

    return changed


def create_customer(customer):
    """Create customer."""
    changed = False
    if create_customer_database(customer):
        changed = True

    for service in get_services():
        if create_service_schema(service, customer):
            changed = False

    return changed


def get_services(customer=None):
    """Get database services."""
    if customer is None:
        database = DATABASE_NAME
    else:
        database = CUSTOMER_PREFIX + customer

    conn = __get_conn(database)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT nspname FROM pg_namespace')
    return [x['nspname'].replace(SERVICE_PREFIX, "")
            for x in cur.fetchall()
            if x['nspname'].startswith(SERVICE_PREFIX)]


def create_service_schema(service, customer=None):
    """Create service schema in all customer databases."""
    if customer is None:
        database = DATABASE_NAME
    else:
        database = CUSTOMER_PREFIX + customer

    changed = False
    conn = __get_conn(database)
    cur = conn.cursor()

    try:
        cur.execute(
            sql.SQL('CREATE SCHEMA {schema} AUTHORIZATION {owner_user}')
            .format(schema=sql.Identifier(SERVICE_PREFIX + service),
                    owner_user=sql.Identifier(service + '_owner')))
        changed = True
    except Exception as e:
        pass

    try:
        cur.execute(
            sql.SQL('GRANT USAGE ON SCHEMA {schema} TO {oltp_user}')
            .format(schema=sql.Identifier(SERVICE_PREFIX + service),
                    oltp_user=sql.Identifier(service + '_oltp')))
    except Exception as e:
        raise e

    try:
        cur.execute(
            sql.SQL('ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} ' +
                    'FOR USER {owner_user} GRANT ALL ON TABLES TO {oltp_user}')
            .format(schema=sql.Identifier(SERVICE_PREFIX + service),
                    owner_user=sql.Identifier(service + '_owner'),
                    oltp_user=sql.Identifier(service + '_oltp')))
    except Exception as e:
        raise e

    return changed


def create_service(service):
    """Create service."""
    changed = False
    if create_service_users(service):
        changed = True
    if create_service_schema(service):
        changed = True

    for customer in get_customers():
        if create_service_schema(service, customer):
            changed = True

    return changed


def create_service_users(service):
    """Create service users."""
    changed = False
    conn = __get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            sql.SQL('CREATE USER {owner_user} WITH UNENCRYPTED PASSWORD %s')
            .format(owner_user=sql.Identifier(service + '_owner')),
            (__create_random_password(),))
        changed = True
    except Exception as e:
        pass

    try:
        cur.execute(
            sql.SQL('GRANT {owner_user} TO {admin_user}')
            .format(owner_user=sql.Identifier(service + '_owner'),
                    admin_user=sql.Identifier(DATABASE_USER)))
    except Exception as e:
        raise e

    try:
        cur.execute(
            sql.SQL('CREATE USER {oltp_user} WITH UNENCRYPTED PASSWORD %s')
            .format(oltp_user=sql.Identifier(service + '_oltp')),
            (__create_random_password(),))
        changed = True
    except Exception as e:
        pass

    return changed


def get_service_users(service):
    """Get service users."""
    conn = __get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT rolname AS user, rolpassword AS password ' +
                'FROM pg_shadow WHERE rolname LIKE %s', (service + '\_%',))
    return [dict(user) for user in cur]


# Handlers
class ApiHandler(tornado.web.RequestHandler):
    """Empty handler."""

    def get(self):
        """Generate an empty response."""
        pass


class CustomerCatalogHandler(tornado.web.RequestHandler):
    """Customer catalog handler."""

    def get(self):
        """Get the list of customers."""
        self.write(json.dumps(get_customers()))


class CustomerHandler(tornado.web.RequestHandler):
    """Customer handler."""

    def get(self, customer):
        """Get customer information."""
        try:
            self.write(self.__get_customer_content(customer))
        except:
            raise tornado.web.HTTPError(404)

    def put(self, customer):
        """Add a new customer."""
        if create_customer(customer):
            self.set_status(201)
        self.write(self.__get_customer_content(customer))

    def __get_customer_content(self, customer):
        content = {
            "services": get_services(customer),
            "database": CUSTOMER_PREFIX + customer
        }

        return json.dumps(content)


class ServiceCatalogHandler(tornado.web.RequestHandler):
    """Service catalog handler."""

    def get(self):
        """Get the list of services."""
        self.write(json.dumps(get_services()))


class ServiceHandler(tornado.web.RequestHandler):
    """Service Handler."""

    def get(self, service):
        """Get service conection parameters."""
        if service not in get_services():
            raise tornado.web.HTTPError(404)
        else:
            self.write(self.__get_service_content(service))

    def put(self, service=None):
        """Add a new service."""
        if create_service(service):
            self.set_status(201)
        self.write(self.__get_service_content(service))

    def __get_service_content(self, service):
        users = get_service_users(service)

        content = {
            "schema": SERVICE_PREFIX + service,
            "users": {
                "owner": [x for x in users if x['user'].endswith('_owner')][0],
                "oltp": [x for x in users if x['user'].endswith('_oltp')][0]
            }
        }

        return json.dumps(content)


class VersionHandler(tornado.web.RequestHandler):
    """API version Handler."""

    def get(self):
        """Print API version."""
        self.write(__version__)


# Application
HANDLERS = [
    (r"/", ApiHandler),
    (r"/customers/?", CustomerCatalogHandler),
    (r"/customers/(?P<customer>\d+)", CustomerHandler),
    (r"/services/?", ServiceCatalogHandler),
    (r"/services/(?P<service>.+)", ServiceHandler),
    (r"/version", VersionHandler),
]


def application():
    """Application."""
    return tornado.web.Application(HANDLERS)


def main(argv=None):
    """Main function."""
    app = application()
    port = LISTEN_PORT
    app.listen(port)
    tornado.ioloop.IOLoop.current().start()


# Main
if (__name__ == "__main__"):
    sys.exit(main(sys.argv))
