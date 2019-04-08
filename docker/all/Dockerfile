FROM storm
WORKDIR /

RUN apk update
RUN apk add --no-cache py-pip
RUN apk add --update py-pip && apk upgrade --no-cache \
  && apk add --no-cache \
  python3 build-base gcc git wget python3-dev postgresql-dev

RUN /usr/bin/pip3 install psycopg2-binary redis

COPY storm.yaml conf/
COPY write-0.1.0-jar-with-dependencies.jar conf/
COPY startStorm.sh /
RUN chmod ug+x /startStorm.sh
