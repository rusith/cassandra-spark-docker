FROM alpine:3.8
EXPOSE 8000
RUN apk update \
  && apk upgrade \
  && apk add bash
  && apk add --update \
  python3 \
  py-pip \
  && pip install --upgrade pip \
  && rm -rf /var/cache/apk/*

COPY ./src /src
WORKDIR /src
RUN pip install -r requirements.txt
CMD run.sh
