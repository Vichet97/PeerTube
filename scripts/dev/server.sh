#!/bin/bash

set -eu

if [ ! -f "./client/dist/en-US/index.html" ]; then
  if [ -z ${1+x} ] || [ "$1" != "--skip-client" ]; then
    echo "client/dist/en-US/index.html does not exist, compile client files..."
    bash ./scripts/build/client.sh
  fi
fi

# Copy locales
mkdir -p "./client/dist"
rm -rf "./client/dist/locale"
cp -r "./client/src/locale" "./client/dist/locale"

mkdir -p "./dist/core/lib"

node ./node_modules/typescript/bin/tsc -b -v --incremental server/tsconfig.json
node ./node_modules/@peertube/resolve-tspaths/dist/main.js --project server/tsconfig.json --src server --out dist

cp -r "./server/core/static" "./server/core/assets" ./dist/core
cp -r "./server/locales" ./dist

node ./node_modules/tsc-watch/dist/lib/tsc-watch.js --build --preserveWatchOutput --verbose --onSuccess 'sh -c "node ./node_modules/@peertube/resolve-tspaths/dist/main.js --project server/tsconfig.json --src server --out dist && NODE_ENV=dev node --inspect dist/server"' server/tsconfig.json
