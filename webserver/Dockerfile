FROM alpine:3.8
RUN apk update \
  && apk upgrade \
  && apk add bash \
  && apk add --update \
  python3 \
  py-pip \
  && pip install --upgrade pip \
  && rm -rf /var/cache/apk/*

COPY ./src/requirements.txt /src/requirements.txt
RUN pip3 install -r /src/requirements.txt

ENV FLASK_ENV=production
ENV FLASK_APP=app.py
COPY ./src /src/
WORKDIR /src
CMD flask run --host=0.0.0.0
EXPOSE 5000
