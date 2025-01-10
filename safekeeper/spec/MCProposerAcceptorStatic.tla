---- MODULE MCProposerAcceptorStatic ----
EXTENDS TLC, ProposerAcceptorStatic

\* Augments the spec with model checking constraints.

\* Note that MCProposerAcceptorReconfig duplicates it and might need to
\* be updated as well.

\* For model checking.
CONSTANTS
  max_entries, \* model constraint: max log entries acceptor/proposer can hold
  max_term \* model constraint: max allowed term

ASSUME max_entries \in Nat /\ max_term \in Nat

\* Model space constraint.
StateConstraint == \A p \in proposers:
                    /\ prop_state[p].term <= max_term
                    /\ Len(prop_state[p].wal) <= max_entries
\* Sets of proposers and acceptors are symmetric because we don't take any
\* actions depending on some concrete proposer/acceptor (like IF p = p1 THEN
\* ...)
ProposerAcceptorSymmetry == Permutations(proposers) \union Permutations(acceptors)

\* enforce order of the vars in the error trace with ALIAS
\* Note that ALIAS is supported only since version 1.8.0 which is pre-release
\* as of writing this.
Alias == [
           prop_state |-> prop_state,
           acc_state |-> acc_state,
           committed |-> committed
         ]

====
