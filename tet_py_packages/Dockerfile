FROM python:3.10-slim-bullseye

RUN apt-get update && apt-get install -y cron

WORKDIR /app

#ENV PATH /usr/local/bin:$PATH
#ENV VIRTUAL_ENV "/venv"
#RUN python -m venv $VIRTUAL_ENV
#ENV PATH "$VIRTUAL_ENV/bin:$PATH"

COPY . /app

RUN cd /app/tetrading && pip install .
RUN cd /app/tet_doc_db && pip install .
RUN cd /app/securities_db_py_dal && pip install .
RUN cd /app/tet_trading_systems && pip install .

COPY crontab /etc/cron.d/crontab
#RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

CMD ["cron", "-f", "dev/null"]
