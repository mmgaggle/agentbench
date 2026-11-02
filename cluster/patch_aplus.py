#!/usr/bin/env python3
"""
A+ : a minimal Python-side fix to the cephadm agent handler, for comparison
with the stock module (A) and agentd (B).

HostData.handle_metadata() rescans every daemon in the cluster three times
per agent report: get_error_daemons() before and after processing (to decide
whether to kick the serve loop) and update_failed_daemon_health_check() (to
rebuild CEPHADM_FAILED_DAEMON). Only the reporting host's daemons can change
in that call, so compare that host's error set instead and rebuild the
health check only when it changed. The serve loop still rebuilds the health
check every iteration, as before.

  patch_aplus.py apply|revert [--modules /ab/mgr-modules]
"""
import argparse
import shutil
import sys

OLD_BEFORE = """            error_daemons_old = set([dd.name() for dd in self.mgr.cache.get_error_daemons()])
            daemon_count_old = len(self.mgr.cache.get_daemons_by_host(host))
"""
NEW_BEFORE = """            def host_error_daemons() -> Set[str]:
                return {dd.name() for dd in self.mgr.cache.get_daemons_by_host(host)
                        if dd.status == DaemonDescriptionStatus.error}

            error_daemons_old = host_error_daemons()
            daemon_count_old = len(self.mgr.cache.get_daemons_by_host(host))
"""
OLD_LS = """            if 'ls' in data and data['ls']:
                self.mgr._process_ls_output(host, data['ls'])
                self.mgr.update_failed_daemon_health_check()
"""
NEW_LS = """            error_daemons_new = error_daemons_old
            if 'ls' in data and data['ls']:
                self.mgr._process_ls_output(host, data['ls'])
                error_daemons_new = host_error_daemons()
                if error_daemons_new != error_daemons_old:
                    self.mgr.update_failed_daemon_health_check()
"""
OLD_KICK = """                error_daemons_old != set([dd.name() for dd in self.mgr.cache.get_error_daemons()])
"""
NEW_KICK = """                error_daemons_old != error_daemons_new
"""


def main():
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['apply', 'revert'])
    p.add_argument('--modules', default='/ab/mgr-modules')
    p.add_argument('--orig', default='/ab/mgr-modules.orig')
    a = p.parse_args()
    path = f'{a.modules}/cephadm/agent.py'
    if a.action == 'revert':
        shutil.copy(f'{a.orig}/cephadm/agent.py', path)
        print('reverted', path)
        return 0
    s = open(path).read()
    if NEW_BEFORE in s:
        print('already applied')
        return 0
    for old, new in ((OLD_BEFORE, NEW_BEFORE), (OLD_LS, NEW_LS), (OLD_KICK, NEW_KICK)):
        if s.count(old) != 1:
            print(f'patch context not found:\n{old}', file=sys.stderr)
            return 1
        s = s.replace(old, new)
    open(path, 'w').write(s)
    print('applied', path)
    return 0


if __name__ == '__main__':
    sys.exit(main())
