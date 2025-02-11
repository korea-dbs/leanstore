#!/usr/bin/env bash

MAJOR=259
MINOR=7
DEV=$(( (MAJOR << 20) | MINOR ))

echo "Tracing I/O for (major=$MAJOR, minor=$MINOR, dev=$DEV)"

# /args->dev == (uint32)($DEV)/
#tracepoint:block:block_bio_queue

echo "sector,nr_sector,rwbs"

#### try  --per-cpu

sudo env BPFTRACE_PERF_RB_PAGES=4096 \
     bpftrace -e "tracepoint:block:block_rq_issue /args->dev==${DEV}/ {
         printf(\"%llu,%u,%s\n\", args->sector, args->nr_sector, args->rwbs);
     }"

exit 0

# Run bpftrace with computed dev
sudo bpftrace -b 131072 -e "
tracepoint:block:block_rq_issue
/args->dev == ${DEV}/
{
  printf(\"%llu,%u,%s\\n\",
         args->sector,
         args->nr_sector,
         args->rwbs);
}"

exit 0

echo "comm,major,minor,dev,sector,nr_sector,bytes,rwbs"
sudo bpftrace -b 131072 -e "
tracepoint:block:block_rq_issue
/args->dev == ${DEV}/
{
  printf(\"%s,%d:%d,%llu,%llu,%u,%u,%s\\n\",
         args->comm,
         args->dev >> 20,
         args->dev & ((1<<20)-1),
         args->dev,
         args->sector,
         args->nr_sector,
         args->bytes,
         args->rwbs);
}"


sudo bpftrace -e '
tracepoint:nvme:nvme_setup_cmd
/args->ctrl_id == 0/
{
    printf("NVMe CMD: ctrl_id=%d disk=%s qid=%d opcode=0x%x flags=0x%x fctype=0x%x cmd_id=%d nsid=%u metadata=%d\n",
        args->ctrl_id, args->disk, args->qid, args->opcode, args->flags,
        args->fctype, args->cid, args->nsid, args->metadata);
}
'
exit 0
