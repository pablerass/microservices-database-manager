"""Multicustomer microservice database manager."""
import os
import psycopg2
import psycopg2.extras
import random
import string

from psycopg2 import sql

# TODO: Add logging
# TODO: Split in different files
# TODO: Change exception management

DATABASE_HOST = os.environ.get("DATABASE_HOST", "localhost")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "postgres")
DATABASE_USER = os.environ.get("DATABASE_USER", "postgres")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD")
DATABASE_ENCODING = os.environ.get("DATABASE_ENCODING", "UTF-8")

DEFAULT_PASSWORD_LENGTH = 40
CUSTOMER_PREFIX = os.environ.get("CUSTOMER_PREFIX", "customer_")
SERVICE_PREFIX = os.environ.get("SERVICE_PREFIX", "service_")


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
