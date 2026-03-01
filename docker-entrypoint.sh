#!/bin/bash
# Wraps any command with a 2h30 OS-level timeout.
# This is a safety net: if any process inside the container hangs (e.g. a stuck
# Playwright/Chromium browser), SIGTERM is sent after 9000s, then SIGKILL 60s later.
# This cannot be bypassed by hung C extensions or subprocesses.
exec timeout --kill-after=60s 100s "$@"
