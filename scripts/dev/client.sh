#!/bin/bash

set -eu

(cd client/src/standalone/player && node ../../../node_modules/vite/bin/vite.js build --mode production --config ./vite.config.mjs)

clientConfiguration="hmr"

if [ ! -z ${2+x} ] && [ "$2" = "--ar-locale" ]; then
  clientConfiguration="ar-locale"
fi

playerCommand="cd client/src/standalone/player && node ../../../node_modules/vite/bin/vite.js build --mode dev --watch --config ./vite.config.mjs"
embedCommand="cd client && node ./node_modules/vite/bin/vite.js -c ./src/standalone/videos/vite.config.mjs dev"
clientCommand="cd client && node ./node_modules/@angular/cli/bin/ng.js serve --proxy-config proxy.config.json --hmr --configuration $clientConfiguration --host 0.0.0.0 --port 3000"
serverCommand="sh -c 'export ANGULAR_CLIENT_ENABLED=true NODE_ENV=dev; node dist/server'"

if [ ! -z ${1+x} ] && [ "$1" = "--skip-server" ]; then
  node ./node_modules/concurrently/dist/bin/concurrently.js -k \
    "$playerCommand" \
    "$clientCommand" \
    "$embedCommand"
else
  bash ./scripts/build/server.sh

  node ./node_modules/concurrently/dist/bin/concurrently.js -k \
    "$playerCommand" \
    "$clientCommand" \
    "$embedCommand" \
    "$serverCommand"
fi
