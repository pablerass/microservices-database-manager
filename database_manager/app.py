"""Multicustomer microservice database manager."""
import os
import tornado.ioloop
import tornado.web

import handlers


LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "8888"))

HANDLERS = [
    (r"/", handlers.ApiHandler),
    (r"/customers/?", handlers.CustomerCatalogHandler),
    (r"/customers/(?P<customer>\d+)", handlers.CustomerHandler),
    (r"/services/?", handlers.ServiceCatalogHandler),
    (r"/services/(?P<service>.+)", handlers.ServiceHandler),
    (r"/version", handlers.VersionHandler),
]


def application():
    """Application."""
    return tornado.web.Application(HANDLERS)


def launch():
    """Launch function."""
    app = application()
    port = LISTEN_PORT
    app.listen(port)
    tornado.ioloop.IOLoop.current().start()
