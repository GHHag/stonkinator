FROM python:3.10-slim-bullseye

RUN apt-get update && apt-get install -y cron git

WORKDIR /app

#ENV PATH /usr/local/bin:$PATH

COPY . /app

RUN cd /app/tetrading && pip install -e .
RUN cd /app/tet_doc_db && pip install -e .
RUN cd /app/securities_db_py_dal && pip install -e .
RUN cd /app/tet_trading_systems && pip install -e .

COPY crontab /etc/cron.d/crontab
#RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

CMD ["cron", "-f", "dev/null"]
