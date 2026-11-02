"""
agentd: an orchestrator backend that reads host state from agentd and
changes hosts by publishing signed intents, instead of running cephadm over
SSH. A prototype of the mgr side of docs/cephadm-integration.md and
docs/agent-executes-intent.md, kept outside cephadm.

  ceph config-key set mgr/agentd/intent_key <base64 Ed25519 seed>
  ceph mgr module enable agentd
  ceph orch set backend agentd
  ceph orch host ls | ceph orch ps | ceph orch daemon restart osd.1
  ceph agentd stats

Reads are level-triggered: agentd's event log only says which hosts
changed, and the module re-reads those hosts' status (one batch request),
so a missed event costs at most a stale host until the next one. An agentd
restart (event sequence going backwards) or events lost off the end of
agentd's ring (`first` past our position) cause a full re-read. Liveness
comes from /v1/liveness, re-read only on liveness events. The work per poll
is proportional to the hosts that changed, not to the cluster (measured
with bench/scale: at 5,000+ hosts a request per host and the full summary
per poll cost far more).
"""
import base64
import datetime
import hashlib
import json
import threading
import time
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

import orchestrator
from ceph.deployment.hostspec import HostSpec
from mgr_module import CLICommandBase, HandleCommandResult, MgrModule, Option
from orchestrator import DaemonDescription, DaemonDescriptionStatus, handle_orch_error


# Tentacle: each module registers its commands in its own registry
AgentdCLICommand = CLICommandBase.make_registry_subtype('AgentdCLICommand')


def _utc(us: int) -> Optional[datetime.datetime]:
    if not us:
        return None
    return datetime.datetime.fromtimestamp(us / 1e6, tz=datetime.timezone.utc)


class Module(MgrModule, orchestrator.Orchestrator):
    CLICommand = AgentdCLICommand
    MODULE_OPTIONS = [
        Option('url', default='http://10.77.0.1:7161', desc="agentd's admin endpoint"),
        Option('poll_interval', type='float', default=1.0, desc='seconds between event polls'),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self.status: Dict[str, Dict[str, Any]] = {}
        self.summary: Dict[str, Dict[str, Any]] = {}
        self.updated: Dict[str, float] = {}
        self.seq = 0
        self.stats = {'polls': 0, 'events': 0, 'host_reads': 0, 'resyncs': 0, 'errors': 0,
                      'cpu_s': 0.0, 'intents_published': 0}
        self.run = True
        self.wake = threading.Event()

    def available(self) -> Tuple[bool, str, Dict[str, Any]]:
        return True, '', {}

    def shutdown(self) -> None:
        self.run = False
        self.wake.set()

    # --- agentd -----------------------------------------------------------

    def _http(self, path: str, data: Optional[bytes] = None, method: str = 'GET') -> Any:
        req = urllib.request.Request(str(self.get_module_option('url')) + path, data=data, method=method)
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
        return json.loads(body) if body else None

    def _host_statuses(self, hosts: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        self.stats['host_reads'] += len(hosts)
        return self._http('/v1/hosts-status', json.dumps(sorted(hosts)).encode(), 'POST') or {}

    def _liveness(self) -> Dict[str, Dict[str, Any]]:
        # host -> [SWIM state, has reported, applied generation]
        return {h: {'liveness': {'state': v[0]} if v[0] else None, 'applied': v[2]}
                for h, v in self._http('/v1/liveness').items()}

    def _resync(self) -> None:
        self.seq = self._http('/v1/events?since=18446744073709551615')['seq']
        summary = self._liveness()
        status = self._host_statuses(list(summary))
        now = time.time()
        with self.lock:
            self.summary = summary
            self.status = {h: s for h, s in status.items() if s}
            self.updated = {h: now for h in self.status}
        self.stats['resyncs'] += 1

    def serve(self) -> None:
        resync = True
        while self.run:
            c0 = time.thread_time()
            try:
                if resync:
                    self._resync()
                    self._update_health()
                    resync = False
                ev = self._http(f'/v1/events?since={self.seq}')
                self.stats['polls'] += 1
                if ev['seq'] < self.seq or self.seq + 1 < ev.get('first', 0):
                    resync = True  # agentd restarted, or we fell off its ring
                    continue
                self.seq = ev['seq']
                events = ev['events']
                if events:
                    self.stats['events'] += len(events)
                    changed = {e['host'] for e in events if e.get('host') and not e['host'].startswith('member-')}
                    fresh = self._host_statuses(list(changed)) if changed else {}
                    summary = None
                    if any(e['kind'] in ('liveness', 'leaving') for e in events):
                        summary = self._liveness()
                    now = time.time()
                    with self.lock:
                        for h, s in fresh.items():
                            if s:
                                self.status[h] = s
                                self.updated[h] = now
                        if summary is not None:
                            self.summary = summary
                    self._update_health()
            except Exception as e:
                self.stats['errors'] += 1
                self.log.warning(f'agentd: {e}')
                resync = True
            self.stats['cpu_s'] += time.thread_time() - c0
            self.wake.wait(float(self.get_module_option('poll_interval')))  # type: ignore
            self.wake.clear()

    def _update_health(self) -> None:
        with self.lock:
            status = dict(self.status)
            summary = dict(self.summary)
        failed = []
        for h, st in sorted(status.items()):
            for d, u in sorted(st.get('units', {}).items()):
                if u['active_state'] == 'failed' or u.get('apply_error'):
                    why = f" ({u['apply_error']})" if u.get('apply_error') else f" (result {u['result']}, {u['n_restarts']} restarts)"
                    failed.append(f'daemon {d} on {h} is {u["active_state"]}/{u["sub_state"]}{why}')
        down = [f'host {h} is {row["liveness"]["state"]}' for h, row in sorted(summary.items())
                if row.get('liveness') and row['liveness']['state'] != 'up']
        checks = {}
        if failed:
            checks['AGENTD_FAILED_DAEMON'] = {'severity': 'warning', 'summary': f'{len(failed)} failed daemon(s)',
                                              'count': len(failed), 'detail': failed}
        if down:
            checks['AGENTD_HOST_DOWN'] = {'severity': 'warning', 'summary': f'{len(down)} host(s) not up',
                                          'count': len(down), 'detail': down}
        self.set_health_checks(checks)

    # --- orchestrator interface ---------------------------------------------

    @handle_orch_error
    def get_hosts(self) -> List[HostSpec]:
        with self.lock:
            summary = dict(self.summary)
        out = []
        for h, row in sorted(summary.items()):
            live = (row.get('liveness') or {}).get('state', 'unknown')
            out.append(HostSpec(h, addr=h, status='' if live == 'up' else live.capitalize()))
        return out

    @handle_orch_error
    def list_daemons(self, service_name: Optional[str] = None, daemon_type: Optional[str] = None,
                     daemon_id: Optional[str] = None, host: Optional[str] = None,
                     refresh: bool = False) -> List[DaemonDescription]:
        with self.lock:
            items = sorted(self.status.items())
            updated = dict(self.updated)
        out = []
        for h, st in items:
            if host and h != host:
                continue
            for name, u in sorted(st.get('units', {}).items()):
                dtype, _, did = name.partition('.')
                if (daemon_type and dtype != daemon_type) or (daemon_id and did != daemon_id):
                    continue
                if u['active_state'] == 'active':
                    status = DaemonDescriptionStatus.running
                elif u['active_state'] == 'failed':
                    status = DaemonDescriptionStatus.error
                elif u['active_state'] == 'inactive':
                    status = DaemonDescriptionStatus.stopped
                else:
                    status = DaemonDescriptionStatus.starting
                dd = DaemonDescription(
                    daemon_type=dtype, daemon_id=did, hostname=h, status=status,
                    status_desc=f"{u['active_state']}/{u['sub_state']}",
                    started=_utc(u.get('active_since_us', 0)),
                    last_refresh=datetime.datetime.fromtimestamp(updated.get(h, time.time()), tz=datetime.timezone.utc),
                    memory_usage=u.get('memory_bytes'), systemd_unit=u.get('unit'))
                if service_name and dd.service_name() != service_name:
                    continue
                out.append(dd)
        return out

    def _sign(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        seed = self.get_store('intent_key')
        if not seed:
            raise orchestrator.OrchestratorError('no intent signing key: ceph config-key set mgr/agentd/intent_key ...')
        key = Ed25519PrivateKey.from_private_bytes(base64.b64decode(seed))
        pub = key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        payload = json.dumps(intent, separators=(',', ':')).encode()
        return {'host': intent['host'], 'generation': intent['generation'], 'key_id': hashlib.sha256(pub).hexdigest()[:16],
                'payload': base64.b64encode(payload).decode(), 'sig': base64.b64encode(key.sign(payload)).decode()}

    @handle_orch_error
    def daemon_action(self, action: str, daemon_name: str, image: Optional[str] = None, force: bool = False) -> str:
        if action not in ('restart', 'redeploy'):
            raise orchestrator.OrchestratorError(f'{action}: this prototype only restarts')
        with self.lock:
            hosts = [h for h, st in self.status.items() if daemon_name in st.get('units', {})]
        if not hosts:
            raise orchestrator.OrchestratorError(f'no daemon {daemon_name}')
        h = hosts[0]
        env = self._http(f'/v1/hosts/{h}/intent')
        intent = json.loads(base64.b64decode(env['payload']))
        u = intent['units'][daemon_name]
        u['restart_key'] = hashlib.sha256(f"{u['restart_key']}/{action}/{time.time()}".encode()).hexdigest()[:16]
        intent['generation'] = max(intent['generation'], int(self.get_store(f'generation.{h}') or 0)) + 1
        self._http(f'/v1/hosts/{h}/intent', json.dumps(self._sign(intent)).encode(), 'PUT')
        self.set_store(f'generation.{h}', str(intent['generation']))
        self.stats['intents_published'] += 1
        return f'Scheduled to {action} {daemon_name} on host {h!r} (intent generation {intent["generation"]})'

    @AgentdCLICommand('agentd stats', perm='r')
    def _stats(self) -> HandleCommandResult:
        """What following agentd costs this mgr"""
        with self.lock:
            hosts = len(self.status)
            daemons = sum(len(s.get('units', {})) for s in self.status.values())
        return HandleCommandResult(stdout=json.dumps(dict(self.stats, hosts=hosts, daemons=daemons, seq=self.seq), indent=1))
