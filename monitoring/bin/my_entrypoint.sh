#!/bin/bash

/bin/node_exporter --web.listen-address=:9100 &

/bin/statsd_exporter --statsd.listen-udp localhost:8125 &

exec /entrypoint "${@}"
