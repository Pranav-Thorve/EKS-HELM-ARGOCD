FROM nginx:1.25-alpine

RUN apk add --no-cache gettext

COPY default.conf.template /etc/nginx/templates/default.conf.template
COPY index.html /usr/share/nginx/html/index.html

ENV API_UPSTREAM_HOST=api-service

CMD envsubst '$API_UPSTREAM_HOST' \
  < /etc/nginx/templates/default.conf.template \
  > /etc/nginx/conf.d/default.conf \
  && nginx -g 'daemon off;'
