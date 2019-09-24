#!/usr/bin/env bash

set -e
set -o pipefail

pushd $(dirname $0) > /dev/null # GO TO PROJECT ROOT

echo "Working dir: $(pwd -P)"

find . -path ./vendor -prune -o -name '*.go' -print | xargs -n 1 -I{} -P 6 sh -c 'echo "reformat: {}" && gofmt -w {}'

popd > /dev/null # EXIT FROM PROJECT ROOT
