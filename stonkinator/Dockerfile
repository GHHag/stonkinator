FROM python:3.10-slim-bullseye

RUN apt-get update && apt-get install -y cron git

WORKDIR /app

COPY . /app

RUN cd /app/stonkinator && pip install -e .

COPY crontab /etc/cron.d/crontab

RUN crontab /etc/cron.d/crontab

CMD ["cron", "-f"]