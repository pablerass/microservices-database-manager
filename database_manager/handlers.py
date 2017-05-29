"""Multicustomer microservice database manager."""
import json
import tornado.ioloop
import tornado.web

import db
import version


class ApiHandler(tornado.web.RequestHandler):
    """Empty handler."""

    def get(self):
        """Generate an empty response."""
        pass


class CustomerCatalogHandler(tornado.web.RequestHandler):
    """Customer catalog handler."""

    def get(self):
        """Get the list of customers."""
        self.write(json.dumps(db.get_customers()))


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
        if db.create_customer(customer):
            self.set_status(201)
        self.write(self.__get_customer_content(customer))

    def __get_customer_content(self, customer):
        # TODO: Move this funtion to db
        content = {
            "services": db.get_services(customer),
            "database": db.CUSTOMER_PREFIX + customer
        }

        return json.dumps(content)


class ServiceCatalogHandler(tornado.web.RequestHandler):
    """Service catalog handler."""

    def get(self):
        """Get the list of services."""
        self.write(json.dumps(db.get_services()))


class ServiceHandler(tornado.web.RequestHandler):
    """Service Handler."""

    def get(self, service):
        """Get service conection parameters."""
        if service not in db.get_services():
            raise tornado.web.HTTPError(404)
        else:
            self.write(self.__get_service_content(service))

    def put(self, service=None):
        """Add a new service."""
        if db.create_service(service):
            self.set_status(201)
        self.write(self.__get_service_content(service))

    def __get_service_content(self, service):
        # TODO: Move this funtion to db
        users = db.get_service_users(service)

        content = {
            "schema": db.SERVICE_PREFIX + service,
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
        self.write(version.__version__)
