#!/bin/bash
set -x

extdir=${1}

cd "${extdir}" || exit 2
FAILED=
LIST=$( (echo -e "${SKIP//","/"\n"}"; ls) | sort | uniq -u)
for d in ${LIST}; do
    [ -d "${d}" ] || continue
    if ! psql -w -c "select 1" >/dev/null; then
      FAILED="${d} ${FAILED}"
      break
    fi
    if [ -f "${d}/neon-test.sh" ]; then
       "time ${d}/neon-test.sh" || FAILED="${d} ${FAILED}"
    else
       USE_PGXS=1 time make -C "${d}" installcheck || FAILED="${d} ${FAILED}"
    fi
done
[ -z "${FAILED}" ] && exit 0
echo "${FAILED}"
exit 1