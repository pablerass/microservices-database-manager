FROM python:3-onbuild

ENV LISTEN_PORT=8888

EXPOSE $LISTEN_PORT

CMD [ "python", "./database_manager" ]

