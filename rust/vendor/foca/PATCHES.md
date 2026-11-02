# Changes to foca 2.0.0

This is foca 2.0.0 from crates.io (MPL-2.0, by Caio), vendored so it can carry
two changes the agent's SWIM needs. Both are off unless asked for. Without
Lifeguard, the wire format is stock foca's (see below).

## Lifeguard (`Config::lifeguard`)

Lifeguard comes from Dadgar, Phillips and Currey, "Lifeguard: Local Health
Awareness for More Accurate Failure Detection" (2018), as hashicorp/memberlist
implements it. Plain SWIM assumes that a member whose probe goes unanswered is
looking at a failed peer. When the member is itself slow (starved of CPU, or
its packets queue), it accuses healthy peers instead. Each false suspicion
then adds gossip, which makes every member slower.

- **Local health** (`Foca::local_health`): a counter from 0 to
  `max_local_health`, default 8.
  - It goes up by one for a probe that nobody answered, and by one for
    refuting a suspicion of ourselves.
  - It goes down by one for an answered probe.
  - The probe period and probe rtt are multiplied by `1 + local health`.
- **Dynamic suspicion**: a suspicion starts at `suspicion_max_factor`
  (default 6) times `suspect_to_down_after`, and shrinks on memberlist's log
  curve to `suspect_to_down_after` as `suspicion_confirmations` (default 2)
  other members independently suspect the same member.
  - Suspect updates now carry their origin (`Member::suspected_by`).
  - A member whose probe of an already-suspected member fails gossips its own
    suspicion as a confirmation.
  - Members pass on confirmations they hadn't seen, up to the number that
    count.
  - Only the members whose probe failed run a timer for the suspicion, as in
    stock foca. The timer ticks at most every probe period, so confirmations
    take effect within a second.
- **Buddy system**: a ping to a member we hold as suspect carries that
  suspicion first, so the member can refute at once.

Not implemented: memberlist's nacks. These would let a helper doing an
indirect probe report that it couldn't reach the target either, which tells a
prober whether a failed probe was its own fault. Here every failed probe
raises local health.

## `Foca::restore_membership`

This restores a membership saved with `iter_membership_state` into a new Foca,
without disseminating any of it.

Without it, a member that restarts learns the whole cluster again one member
at a time. Every member it hasn't heard of yet counts as news to it, so it
gossips each one. When every agent restarted at once, as in an upgrade, this
filled every packet in the cluster for minutes.

## Wire format

`Member` has a new field, `suspected_by`, which is set only on a Lifeguard
member's suspicions. Its serde encoding is written by hand.

- Without an origin, a member encodes byte for byte as stock foca's derived
  `Member { id, incarnation, state }`. So members without Lifeguard and stock
  foca understand each other, and a cluster can upgrade to this foca one
  member at a time.
- A suspicion with an origin is encoded as a fourth state followed by the
  origin, which stock foca can't read.

Turn Lifeguard on in every member at once.
