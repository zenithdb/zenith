---- MODULE MCProposerAcceptorReconfig ----
EXTENDS TLC, ProposerAcceptorReconfig

\* Augments the spec with model checking constraints.

\* It slightly duplicates MCProposerAcceptorStatic, but we can't EXTENDS it
\* because it EXTENDS ProposerAcceptorStatic in turn. The duplication isn't big
\* anyway.

\* For model checking.
CONSTANTS
  max_entries, \* model constraint: max log entries acceptor/proposer can hold
  max_term, \* model constraint: max allowed term
  max_generation \* mode constraint: max config generation

ASSUME max_entries \in Nat /\ max_term \in Nat /\ max_generation \in Nat

\* Model space constraint.
StateConstraint == /\ \A p \in proposers:
                     /\ prop_state[p].term <= max_term
                     /\ Len(prop_state[p].wal) <= max_entries
                   /\ conf_store.generation <= max_generation

\* Sets of proposers and acceptors and symmetric because we don't take any
\* actions depending on some concrete proposer/acceptor (like IF p = p1 THEN
\* ...)
ProposerAcceptorSymmetry == Permutations(proposers) \union Permutations(acceptors)

\* enforce order of the vars in the error trace with ALIAS
\* Note that ALIAS is supported only since version 1.8.0 which is pre-release
\* as of writing this.
Alias == [
           prop_state |-> prop_state,
           prop_conf |-> prop_conf,
           acc_state |-> acc_state,
           acc_conf |-> acc_conf,
           committed |-> committed,
           conf_store |-> conf_store
         ]

====
