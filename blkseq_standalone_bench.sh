#!/usr/bin/env bash
# Round-robins private_blkseq_skip_standalone modes (0=stock,1=wal-only,
# 2=everything) on a throwaway standalone db under parallel insert load.
# Usage: ./blkseq_standalone_bench.sh [BUILDDIR]   (BUILDDIR defaults to ./build)
set -u

# ---- knobs (edit if you like) ------------------------------------------------
BUILDDIR="${1:-$(cd "$(dirname "$0")" && pwd)/build}"
WRITERS=128            # parallel writer connections; ~6x your core count to
                       # actually pressure the log region (nproc here: check below)
PER=1500              # autocommit inserts per writer per trial
ROUNDS=8             # per mode; round 1 is a discarded warmup
LOGBUFS="10485760 262144"   # log buffer sizes to sweep: 10MB (roomy) then 256KB (pressured)
# -----------------------------------------------------------------------------

COMDB2="$BUILDDIR/db/comdb2"
CDB2SQL="$BUILDDIR/tools/cdb2sql/cdb2sql"
PMUX="$BUILDDIR/tools/pmux/pmux"
for b in "$COMDB2" "$CDB2SQL" "$PMUX"; do
    [ -x "$b" ] || { echo "missing binary: $b (pass the build dir as arg 1)"; exit 1; }
done

DBNAME=blkseqsabench
WORK=$(mktemp -d /tmp/blkseqsa.XXXXXX)
DBDIR=$WORK/db
LRL=$DBDIR/$DBNAME.lrl
CFG=$WORK/comdb2db.cfg
PIDFILE=$WORK/$DBNAME.pid
INSFILE=$WORK/inserts.sql
export COMDB2_ROOT=$WORK
mkdir -p "$DBDIR"

STARTED_PMUX=0
DBPID=""
cleanup() {
    [ -n "$DBPID" ] && kill "$DBPID" 2>/dev/null
    sleep 1
    [ "$STARTED_PMUX" = 1 ] && { pgrep -x pmux >/dev/null && kill "$(pgrep -x pmux)" 2>/dev/null; }
    rm -rf "$WORK"
}
trap cleanup EXIT

sq()  { "$CDB2SQL" --tabs --cdb2cfg "$CFG" "$DBNAME" default "$1" 2>/dev/null; }
lstat() { sq "exec procedure sys.cmd.send('bdb logstat')" | awk -v f="$1:" '$1==f{print $2; exit}'; }
lsn() {
    local raw f o
    raw=$(sq "exec procedure sys.cmd.send('bdb cluster')" | grep -oP 'lsn \K[0-9]+:[0-9]+' | head -1)
    f=${raw%%:*}; o=${raw##*:}; echo $(( f*41943040 + o ))
}

# --- config + insert payload --------------------------------------------------
cat > "$CFG" <<EOF
comdb2_config:default_type=local
comdb2_config:allow_pmux_route:true
EOF
awk -v n="$PER" 'BEGIN{for(i=1;i<=n;i++) printf "insert into t values(%d,'\''nm%d'\'',%d)\n", i, i%1000, i*7}' > "$INSFILE"
TOTAL=$(( WRITERS*PER ))

# --- pmux ---------------------------------------------------------------------
if ! pgrep -x pmux >/dev/null; then
    COMDB2_PMUX_FILE="$WORK/pmux.sqlite" "$PMUX" -l >"$WORK/pmux.log" 2>&1 &
    STARTED_PMUX=1
    sleep 2
    pgrep -x pmux >/dev/null || { echo "failed to start pmux"; exit 1; }
fi

# --- create db once -----------------------------------------------------------
cat > "$LRL" <<EOF
name $DBNAME
dir $DBDIR
EOF
echo "creating db in $DBDIR ..."
"$COMDB2" --create "$DBNAME" --no-global-lrl --lrl "$LRL" --pidfile "$PIDFILE" >"$WORK/create.log" 2>&1 \
    || { echo "create failed:"; tail -20 "$WORK/create.log"; exit 1; }

start_db() { # $1 = log buffer size
    [ -n "$DBPID" ] && { kill "$DBPID" 2>/dev/null; wait "$DBPID" 2>/dev/null; DBPID=""; }
    cat > "$LRL" <<EOF
name $DBNAME
dir $DBDIR
setattr LOGMEMSIZE $1
EOF
    "$COMDB2" "$DBNAME" --no-global-lrl --lrl "$LRL" --pidfile "$PIDFILE" >"$WORK/db.log" 2>&1 &
    DBPID=$!
    local i
    for i in $(seq 1 60); do
        [ "$(sq 'select 1')" = "1" ] && return 0
        sleep 1
    done
    echo "db did not come up; tail of log:"; tail -20 "$WORK/db.log"; exit 1
}

trial() { # $1 mode(0/1/2)  $2 label  $3 round  $4 csv
    sq "put tunable private_blkseq_skip_standalone $1" >/dev/null
    sq "drop table if exists t" >/dev/null
    sq "create table t(id int, nm char(16), val int)" >/dev/null
    local bq0 rw0 wcf0 lb bq1 rw1 wcf1 la t0 t1 ms rps bpi
    bq0=$(sq "select count(*) from comdb2_blkseq")
    rw0=$(lstat st_region_wait); wcf0=$(lstat st_wcount_fill); lb=$(lsn)
    t0=$(date +%s%N)
    local w pids=()
    # wait only on writer pids: a bare `wait` blocks on the background db too
    for w in $(seq 1 "$WRITERS"); do
        "$CDB2SQL" --tabs --cdb2cfg "$CFG" "$DBNAME" default -f "$INSFILE" >/dev/null 2>&1 &
        pids+=($!)
    done
    wait "${pids[@]}"
    t1=$(date +%s%N)
    rw1=$(lstat st_region_wait); wcf1=$(lstat st_wcount_fill); la=$(lsn)
    bq1=$(sq "select count(*) from comdb2_blkseq")
    ms=$(( (t1-t0)/1000000 )); [ "$ms" -le 0 ] && ms=1
    rps=$(awk -v n="$TOTAL" -v ms="$ms" 'BEGIN{printf "%d", n*1000.0/ms}')
    bpi=$(awk -v b="$(( la-lb ))" -v n="$TOTAL" 'BEGIN{printf "%.1f", b*1.0/n}')
    echo "$2,$3,$rps,$(( rw1-rw0 )),$(( wcf1-wcf0 )),$bpi,$(( bq1-bq0 ))" >> "$4"
    printf "  buf=%-8s r%-2s %-11s rows/s=%-7s region_wait=%-7s wcount_fill=%-6s B/ins=%-6s blkseq+=%s\n" \
        "$CURBUF" "$3" "$2" "$rps" "$(( rw1-rw0 ))" "$(( wcf1-wcf0 ))" "$bpi" "$(( bq1-bq0 ))"
}

summary() { # $1 csv  $2 bufsize
    echo ""
    echo "  ---- SUMMARY buf=$2 (rounds 2-$ROUNDS, $WRITERS writers x $PER = $TOTAL rows/trial) ----"
    awk -F, -v R="$ROUNDS" '$2>1 {
        n[$1]++; s[$1]+=$3; ss[$1]+=$3*$3; rw[$1]+=$4; wcf[$1]+=$5; bpi[$1]+=$6
    } END {
        printf "  %-11s  %9s  %8s  %6s   %11s  %11s  %9s\n","mode","rows/s","stdev","rsd%","region_wait","wcnt_fill","B/insert"
        split("stock wal-only everything", ord, " ")
        for (i=1;i<=3;i++){ v=ord[i]; if(n[v]<1) continue
            m=s[v]/n[v]; sd=(n[v]>1)?sqrt((ss[v]-n[v]*m*m)/(n[v]-1)):0
            printf "  %-11s  %9.0f  %8.0f  %5.1f%%   %11.0f  %11.1f  %9.1f\n", v, m, sd, (m>0?100*sd/m:0), rw[v]/n[v], wcf[v]/n[v], bpi[v]/n[v]
        }
    }' "$1"
}

echo "comdb2=$COMDB2"
echo "cores=$(nproc)  writers=$WRITERS  per=$PER  rounds=$ROUNDS  (round 1 discarded)"
echo "modes: 0=stock  1=wal-only  2=everything   (rotated per trial)"
echo "=========================================================================================="
for buf in $LOGBUFS; do
    CURBUF=$buf
    start_db "$buf"
    # sanity: confirm tunable + standalone + actual buffer size
    got=$(lstat st_lg_bsize)
    echo ">> log buffer requested=$buf  actual st_lg_bsize=$got"
    CSV="$WORK/res_$buf.csv"; : > "$CSV"
    for r in $(seq 1 "$ROUNDS"); do
        trial 0 stock      "$r" "$CSV"
        trial 1 wal-only   "$r" "$CSV"
        trial 2 everything "$r" "$CSV"
    done
    summary "$CSV" "$buf"
    echo ""
done
echo "done."
