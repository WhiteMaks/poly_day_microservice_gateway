FROM alpine:latest

RUN apk update
RUN apk upgrade
RUN apk add gcompat

EXPOSE 8181

ENV WORK_PATH=/usr/poly_day

COPY ./docker $WORK_PATH/
COPY ./.bin/gateway/poly_day_microservice_gateway $WORK_PATH/

WORKDIR $WORK_PATH

RUN chmod 777 ./entrypoint.sh
RUN chmod 777 ./export_variables.sh
RUN chmod 777 ./poly_day_microservice_gateway

ENTRYPOINT ./entrypoint.sh