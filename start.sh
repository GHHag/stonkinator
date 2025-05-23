#!/bin/bash

watch=false
run_cron=false

for argument in "$@"
do
	case $argument in
		--watch)
			watch=true
			;;
		--run-cron)
			run_cron=true
			;;
	esac
done

certificates_dir="stonkinator/.certs"
if [ ! -d "$certificates_dir" ]; then
	bash create_certs.sh $certificates_dir

	openssl req \
		-x509 \
		-sha256 \
		-nodes \
		-newkey rsa:4096 \
		-keyout web_api/stonkinator.key \
		-out web_api/stonkinator.crt \
		-subj /O=me
fi

if $watch; then
    docker compose -f ./docker-compose.yml watch
else
    docker compose -f ./docker-compose.yml up -d
fi

if $run_cron; then
	DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
	CRON_JOB="0 21 1 * * $DIR/dump_db.sh >> /var/log/dump_db.log 2>&1"

	crontab -l | grep -q "$CRON_JOB"
	if [ $? -eq 0 ]; then
	    echo "Cron job already exists."
	else
	    (crontab -l; echo "$CRON_JOB") | crontab -
	    echo "Cron job added."
	fi
fi
