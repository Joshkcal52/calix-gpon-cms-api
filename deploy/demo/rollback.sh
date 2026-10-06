#!/bin/sh
# Demo entry point for the rollback playbook. Turns the sample snapshot into a
# Git repository (standing in for network-backups), then runs the playbook with
# whatever arguments were given.
set -e

REPO=/tmp/network-backups
if [ ! -d "$REPO/.git" ]; then
    mkdir -p "$REPO"
    cp -r /app/deploy/demo/network-backups/. "$REPO/"
    git -C "$REPO" init -q -b main
    git -C "$REPO" add .
    git -C "$REPO" -c user.name=demo -c user.email=demo@example.invalid commit -q -m "Nightly snapshot"
fi

export NETWORK_BACKUPS_REPO="$REPO"
cd /app/ansible/rollback
exec ansible-playbook rollback.yml "$@"
