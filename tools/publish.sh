#!/bin/bash

export GIT_MERGE_AUTOEDIT=no

CURRENT_VERSION=$1
UPGRADE_VERSION=$2

set -e

git flow release start ${UPGRADE_VERSION}
git flow release finish -m "${UPGRADE_VERSION}" ${UPGRADE_VERSION}
git push --all && git push --tags
git checkout develop
