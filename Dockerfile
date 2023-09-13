# Build dependencies
FROM python:3.11.4-alpine3.18@sha256:0135ae6442d1269379860b361760ad2cf6ab7c403d21935a8015b48d5bf78a86 AS build

WORKDIR /usr/app
RUN python -m venv /usr/app/venv
ENV PATH="/usr/app/venv/bin:$PATH"

COPY requirements.txt .
RUN pip install -r requirements.txt

# Deployment image
FROM python:3.11.4-alpine3.18@sha256:0135ae6442d1269379860b361760ad2cf6ab7c403d21935a8015b48d5bf78a86 AS backend

RUN addgroup -g 1001 pdmv && adduser --disabled-password -u 1001 -G pdmv pdmv

RUN mkdir /usr/app && chown pdmv:pdmv /usr/app
WORKDIR /usr/app

COPY --chown=pdmv:pdmv --from=build /usr/app/venv ./venv
COPY --chown=pdmv:pdmv . .

USER 1001

ENV PATH="/usr/app/venv/bin:$PATH"
CMD [ "gunicorn", "main:app" ]