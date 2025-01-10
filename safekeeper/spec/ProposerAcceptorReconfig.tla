---- MODULE ProposerAcceptorReconfig ----

(*
    Spec for https://github.com/neondatabase/neon/blob/538e2312a617c65d489d391892c70b2e4d7407b5/docs/rfcs/035-safekeeper-dynamic-membership-change.md

    Simplifications:
    - The ones inherited from ProposerAcceptorStatic.
    - We don't model transient state of the configuration change driver process
      (storage controller in the implementation). Its actions StartChange and FinishChange
      are taken based on the persistent state of safekeepers and conf store. The
      justification for that is the following: once new configuration n is
      created (e.g with StartChange or FinishChange), any old configuration
      change driver working on older conf < n will never be able to commit
      it to the conf store because it is protected by CAS. The
      propagation of these older confs is still possible though, and
      spec allows to do it through acceptors.
      Plus the model is already pretty huge.
    - Previous point also means that the FinishChange action is
      based only on the current state of safekeepers, not from
      the past. That's ok because while individual
      acceptor <last_log_term, flush_lsn> may go down,
      quorum one never does. So the FinishChange
      condition which collects max of the quorum may get
      only more strict over time.

    The invariants expectedly break if any of FinishChange
    required conditions are removed.
*)

EXTENDS Integers, Sequences, FiniteSets, TLC

VARIABLES
  \* state which is the same in the static spec
  prop_state,
  acc_state,
  committed,
  elected_history,
  \* reconfiguration only state
  prop_conf, \* prop_conf[p] is current configuration of proposer p
  acc_conf, \* acc_conf[a] is current configuration of acceptor a
  conf_store \* configuration in the configuration store.

CONSTANT
  acceptors,
  proposers

CONSTANT NULL

\* Import ProposerAcceptorStatic under PAS.
\*
\* Note that all vars and consts are named the same and thus substituted
\* implicitly.
PAS == INSTANCE ProposerAcceptorStatic

\********************************************************************************
\* Helpers
\********************************************************************************

\********************************************************************************
\* Type assertion
\********************************************************************************

\* Is c a valid config?
IsConfig(c) ==
    /\ DOMAIN c = {"generation", "members", "newMembers"}
    \* Unique id of the configuration.
    /\ c.generation \in Nat
    /\ c.members \in SUBSET acceptors
    \* newMembers is NULL when it is not a joint conf.
    /\ \/ c.newMembers = NULL
       \/ c.newMembers \in SUBSET acceptors

TypeOk ==
    /\ PAS!TypeOk
    /\ \A p \in proposers: IsConfig(prop_conf[p])
    /\ \A a \in acceptors: IsConfig(acc_conf[a])
    /\ IsConfig(conf_store)

\********************************************************************************
\* Initial
\********************************************************************************

Init ==
  /\ PAS!Init
  /\ \E init_members \in SUBSET acceptors:
       LET init_conf == [generation |-> 1, members |-> init_members, newMembers |-> NULL] IN
           \* refer to RestartProposer why it is not NULL
           /\ prop_conf = [p \in proposers |-> init_conf]
           /\ acc_conf = [a \in acceptors |-> init_conf]
           /\ conf_store = init_conf
           \* We could start with anything, but to reduce state space state with
           \* the most reasonable total acceptors - 1 conf size, which e.g.
           \* makes basic {a1} -> {a2} change in {a1, a2} acceptors and {a1, a2,
           \* a3} -> {a2, a3, a4} in {a1, a2, a3, a4} acceptors models even in
           \* the smallest models with single change.
           /\ Cardinality(init_members) = Cardinality(acceptors) - 1

\********************************************************************************
\* Actions
\********************************************************************************

\* Proposer p loses all state, restarting. In the static spec we bump restarted
\* proposer term to max of some quorum + 1 which is a minimal term which can win
\* election. With reconfigurations it's harder to calculate such a term, so keep
\* it simple and take random acceptor one + 1.
\*
\* Also make proposer to adopt configuration of another random acceptor. In the
\* impl proposer starts with NULL configuration until handshake with first
\* acceptor. Removing this NULL special case makes the spec a bit simpler.
RestartProposer(p) ==
    /\ \E a \in acceptors: PAS!RestartProposerWithTerm(p, acc_state[a].term + 1)
    /\ \E a \in acceptors: prop_conf' = [prop_conf EXCEPT ![p] = acc_conf[a]]
    /\ UNCHANGED <<acc_conf, conf_store>>

\* Acceptor a immediately votes for proposer p.
Vote(p, a) ==
    \* Configuration must be the same.
    /\ prop_conf[p].generation = acc_conf[a].generation
    \* And a is expected be a member of it. This is likely redundant as long as
    \* becoming leader checks membership (though vote also contributes to max
    \* <term, lsn> calculation).
    /\ \/ a \in prop_conf[p].members
       \/ (prop_conf[p].newMembers /= NULL) /\ (a \in prop_conf[p].newMembers)
    /\ PAS!Vote(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

\* Proposer p gets elected.
BecomeLeader(p) ==
    /\ prop_state[p].state = "campaign"
    \* Votes must form quorum in both sets (if the newMembers exists).
    /\ PAS!FormsQuorum(DOMAIN prop_state[p].votes, prop_conf[p].members)
    /\ \/ prop_conf[p].newMembers = NULL
       \* TLA+ disjunction evaluation doesn't short-circuit for a good reason:
       \* https://groups.google.com/g/tlaplus/c/U6tOJ4dsjVM/m/UdOznPCVBwAJ
       \* so repeat the null check.
       \/ (prop_conf[p].newMembers /= NULL)  /\ (PAS!FormsQuorum(DOMAIN prop_state[p].votes, prop_conf[p].newMembers))
    \* DoBecomeLeader will copy WAL of the highest voter to proposer's WAL, so
    \* ensure its conf is still the same. In the impl WAL fetching also has to
    \* check the configuration.
    /\ prop_conf[p].generation = acc_conf[PAS!MaxVoteAcc(p)].generation
    /\ \A a \in DOMAIN prop_state[p].votes: prop_conf[p].generation = acc_conf[a].generation
    /\ PAS!DoBecomeLeader(p)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

UpdateTerm(p, a) ==
    /\ PAS!UpdateTerm(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

TruncateWal(p, a) ==
    /\ prop_state[p].state = "leader"
    \* Configuration must be the same.
    /\ prop_conf[p].generation = acc_conf[a].generation
    /\ PAS!TruncateWal(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

NewEntry(p) ==
    /\ PAS!NewEntry(p)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

AppendEntry(p, a) ==
    /\ prop_state[p].state = "leader"
    \* Configuration must be the same.
    /\ prop_conf[p].generation = acc_conf[a].generation
    \* And a is member of it. Ignoring this likely wouldn't hurt, but not useful
    \* either.
    /\ \/ a \in prop_conf[p].members
       \/ (prop_conf[p].newMembers /= NULL) /\ (a \in prop_conf[p].newMembers)
    /\ PAS!AppendEntry(p, a)
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

\* see PAS!CommitEntries for comments.
CommitEntries(p) ==
    /\ prop_state[p].state = "leader"
    /\ \E q1 \in PAS!AllMinQuorums(prop_conf[p].members):
           LET q1_commit_lsn == PAS!QuorumCommitLsn(p, q1) IN
               \* Configuration must be the same.
               /\ \A a \in q1: prop_conf[p].generation = acc_conf[a].generation
               /\ q1_commit_lsn /= NULL
               \* We must collect acks from both quorums, if newMembers is present.
               /\ IF prop_conf[p].newMembers = NULL THEN
                      PAS!DoCommitEntries(p, q1_commit_lsn)
                  ELSE
                      \E q2 \in PAS!AllMinQuorums(prop_conf[p].newMembers):
                          LET q2_commit_lsn == PAS!QuorumCommitLsn(p, q2) IN
                              \* Configuration must be the same.
                              /\ \A a \in q1: prop_conf[p].generation = acc_conf[a].generation
                              /\ q2_commit_lsn /= NULL
                              /\ PAS!DoCommitEntries(p, PAS!Min(q1_commit_lsn, q2_commit_lsn))
    /\ UNCHANGED <<prop_conf, acc_conf, conf_store>>

\* Proposer p adopts higher conf c from conf store or from some acceptor.
ProposerSwitchConf(p) ==
    /\ \E c \in ({conf_store} \union {acc_conf[a]: a \in acceptors}):
        \* p's conf is lower than c.
        /\ (c.generation > prop_conf[p].generation)
        \* We allow to bump conf without restart only when wp is already elected.
        \* If it isn't, the votes it has already collected are from the previous
        \* configuration and can't be used.
        \*
        \* So if proposer is in 'campaign' in the impl we would restart preserving
        \* conf and increasing term. In the spec this transition is already covered
        \* by more a generic RestartProposer, so we don't specify it here.
        /\ prop_state[p].state = "leader"
        /\ prop_conf' = [prop_conf EXCEPT ![p] = c]
        /\ UNCHANGED <<prop_state, acc_state, committed, elected_history, acc_conf, conf_store>>

\* Do CAS on the conf store, starting change into the new_members conf.
StartChange(new_members) ==
    \* Possible only if we don't already have the change in progress.
    /\ conf_store.newMembers = NULL
    \* Not necessary, but reduces space a bit.
    /\ new_members /= conf_store.members
    /\ conf_store' = [generation |-> conf_store.generation + 1, members |-> conf_store.members, newMembers |-> new_members]
    /\ UNCHANGED <<prop_state, acc_state, committed, elected_history, prop_conf, acc_conf>>

\* Acceptor's last_log_term.
AccLastLogTerm(acc) ==
    PAS!LastLogTerm(PAS!AcceptorTermHistory(acc))

\* Do CAS on the conf store, transferring joint conf into the newMembers only.
FinishChange ==
    \* have joint conf
    /\ conf_store.newMembers /= NULL
    \* The conditions for finishing the change are:
    /\ \E qo \in PAS!AllMinQuorums(conf_store.members):
           \* 1) Old majority must be aware of the joint conf.
           \* Note: generally the driver can't know current acceptor
           \* generation, it can only know that it once had been the
           \* expected one, but it might have advanced since then.
           \* But as explained at the top of the file if acceptor gen
           \* advanced, FinishChange will never be able to complete
           \* due to CAS anyway. We use strict equality here because
           \* that's what makes sense conceptually (old driver should
           \* abandon its attempt if it observes that conf has advanced).
           /\ \A a \in qo: conf_store.generation = acc_conf[a].generation
           \* 2) New member set must have log synced, i.e. some its majority needs
           \*    to have <last_log_term, lsn> at least as high as max of some
           \*    old majority.
           \* 3) Term must be synced, i.e. some majority of the new set must
           \*    have term >= than max term of some old majority.
           \*    This ensures that two leaders are never elected with the same
           \*    term even after config change (which would be bad unless we treat
           \*    generation as a part of term which we don't).
           \* 4) A majority of the new set must be aware of the joint conf.
           \*    This allows to safely destoy acceptor state if it is not a
           \*    member of its current conf (which is useful for cleanup after
           \*    migration as well as for aborts).
           /\ LET sync_pos == PAS!MaxTermLsn({[term |-> AccLastLogTerm(a), lsn |-> PAS!FlushLsn(a)]: a \in qo})
                  sync_term == PAS!Maximum({acc_state[a].term: a \in qo})
              IN
                  \E qn \in PAS!AllMinQuorums(conf_store.newMembers):
                      \A a \in qn:
                          /\ PAS!TermLsnGE([term |-> AccLastLogTerm(a), lsn |-> PAS!FlushLsn(a)], sync_pos)
                          /\ acc_state[a].term >= sync_term
                          \* The same note as above about strict equality applies here.
                          /\ conf_store.generation = acc_conf[a].generation
    /\ conf_store' = [generation |-> conf_store.generation + 1, members |-> conf_store.newMembers, newMembers |-> NULL]
    /\ UNCHANGED <<prop_state, acc_state, committed, elected_history, prop_conf, acc_conf>>

\* Do CAS on the conf store, aborting the change in progress.
AbortChange ==
    \* have joint conf
    /\ conf_store.newMembers /= NULL
    /\ conf_store' = [generation |-> conf_store.generation + 1, members |-> conf_store.members, newMembers |-> NULL]
    /\ UNCHANGED <<prop_state, acc_state, committed, elected_history, prop_conf, acc_conf>>

\* Acceptor a switches to higher configuration from the conf store
\* or from some proposer.
AccSwitchConf(a) ==
    /\ \E c \in ({conf_store} \union {prop_conf[p]: p \in proposers}):
        /\ acc_conf[a].generation < c.generation
        /\ acc_conf' = [acc_conf EXCEPT ![a] = c]
        /\ UNCHANGED <<prop_state, acc_state, committed, elected_history, prop_conf, conf_store>>

\* Nuke all acceptor state if it is not a member of its current conf. Models
\* cleanup after migration/abort.
AccReset(a) ==
    /\ \/ (acc_conf[a].newMembers = NULL) /\ (a \notin acc_conf[a].members)
       \/ (acc_conf[a].newMembers /= NULL) /\ (a \notin (acc_conf[a].members \union acc_conf[a].newMembers))
    /\ acc_state' = [acc_state EXCEPT ![a] = PAS!InitAcc]
    \* Set nextSendLsn to `a` to NULL everywhere. nextSendLsn serves as a mark
    \* that elected proposer performed TruncateWal on the acceptor, which isn't
    \* true anymore after state reset. In the impl local deletion is expected to
    \* terminate all existing connections.
    /\ prop_state' = [p \in proposers |-> [prop_state[p] EXCEPT !.nextSendLsn[a] = NULL]]
    /\ UNCHANGED <<committed, elected_history, prop_conf, acc_conf, conf_store>>

\*******************************************************************************
\* Final spec
\*******************************************************************************

Next ==
  \/ \E p \in proposers: RestartProposer(p)
  \/ \E p \in proposers: \E a \in acceptors: Vote(p, a)
  \/ \E p \in proposers: BecomeLeader(p)
  \/ \E p \in proposers: \E a \in acceptors: UpdateTerm(p, a)
  \/ \E p \in proposers: \E a \in acceptors: TruncateWal(p, a)
  \/ \E p \in proposers: NewEntry(p)
  \/ \E p \in proposers: \E a \in acceptors: AppendEntry(p, a)
  \/ \E p \in proposers: CommitEntries(p)
  \/ \E new_members \in SUBSET acceptors: StartChange(new_members)
  \/ FinishChange
  \/ AbortChange
  \/ \E p \in proposers: ProposerSwitchConf(p)
  \/ \E a \in acceptors: AccSwitchConf(a)
  \/ \E a \in acceptors: AccReset(a)

Spec == Init /\ [][Next]_<<prop_state, acc_state, committed, elected_history, prop_conf, acc_conf, conf_store>>

\********************************************************************************
\* Invariants
\********************************************************************************

AllConfs ==
    {conf_store} \union {prop_conf[p]: p \in proposers} \union {acc_conf[a]: a \in acceptors}

\* Fairly trivial (given the conf store) invariant that different configurations
\* with the same generation are never issued.
ConfigSafety ==
    \A c1, c2 \in AllConfs:
        (c1.generation = c2.generation) => (c1 = c2)

ElectionSafety == PAS!ElectionSafety

ElectionSafetyFull == PAS!ElectionSafetyFull

LogIsMonotonic == PAS!LogIsMonotonic

LogSafety == PAS!LogSafety

\********************************************************************************
\* Invariants which don't need to hold, but useful for playing/debugging.
\********************************************************************************

\* Check that we ever switch into non joint conf.
MaxAccConf == ~ \E a \in acceptors:
    /\ acc_conf[a].generation = 3
    /\ acc_conf[a].newMembers /= NULL

CommittedNotTruncated == PAS!CommittedNotTruncated

MaxTerm == PAS!MaxTerm

MaxStoreConf == conf_store.generation <= 1

MaxAccWalLen == PAS!MaxAccWalLen

MaxCommitLsn == PAS!MaxCommitLsn

====
