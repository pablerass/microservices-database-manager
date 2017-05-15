FROM python:3-onbuild

EXPOSE 80

CMD [ "python", "./database_manager.py" ]

