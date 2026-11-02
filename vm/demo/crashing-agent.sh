#!/bin/sh
# An agent "upgrade" that passes the --version smoke test, then fails to
# start: systemd's start limit and OnFailure= must roll it back.
if [ "$1" = --version ]; then echo "ceph-agent 9.9.9-crashes"; exit 0; fi
echo "crashing on purpose" >&2
exit 1
