#!/bin/bash
set -x

extdir=${1}

cd "${extdir}" || exit 2
export FAILED_FILE=/tmp/failed
touch ${FAILED_FILE}
LIST=$( (echo -e "${SKIP//","/"\n"}"; ls) | sort | uniq -u)
parallel -j3 "[ -d {} ] || exit 0; if ! psql -c "select 1" >/dev/null; then {echo {} >> ${FAILED_FILE}; exit 1; fi ; if [ -f $extdir/{}/neon-test.sh ]; then echo Running from script; PGHOST=pcompute{%} time ${extdir}/{}/neon-test.sh || echo {} >> ${FAILED_FILE}; else echo Running using make; time USE_PGXS=1 PGHOST=pcompute{%} make -C {} installcheck || echo {} >> ${FAILED_FILE}; fi;" ::: ${LIST}
[ -z "${FAILED}" ] && exit 0
echo "${FAILED}"
exit 1