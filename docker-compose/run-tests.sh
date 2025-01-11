#!/bin/bash
set -x

cd /ext-src || exit 2
FAILED=
LIST=$( (echo -e "${SKIP//","/"\n"}"; ls -d -- *-src) | sort | uniq -u)
for d in ${LIST}; do
    [ -d "${d}" ] || continue
    psql -c "select 1" >/dev/null || break
    if [ -f "${d}/neon-test.sh" ]; then
       cd "${d}" || exit 1
       ./neon-test.sh || FAILED="${d} ${FAILED}"
       cd ..
    else
       USE_PGXS=1 make -C "${d}" installcheck || FAILED="${d} ${FAILED}"
    fi
done
[ -z "${FAILED}" ] && exit 0
echo "${FAILED}"
exit 1