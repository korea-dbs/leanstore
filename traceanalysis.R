library('duckdb')
library('ggplot2')
library('reshape2')
'%+%' <- function(x, y) paste0(x,y)
duck <- function(query) {
 if (!exists("duckdbcon")) {
   duckdbcon <<- dbConnect(duckdb(environment_scan = TRUE))
	dbExecute(duckdbcon, "INSTALL json")
	dbExecute(duckdbcon, "LOAD json")
 }
 dbFetch(dbSendQuery(duckdbcon, query))
}

	dbExecute(duckdbcon, "INSTALL json")
	dbExecute(duckdbcon, "LOAD json")

#############################################################################################
# bpf traces

file="/home/haas/leanstore-main/build/traceio_output_64.log.gz"

duck("create or replace table trace as select row_number() over () as time, * from read_csv('"%+%file%+%"', delim = ',')")

duck("select count(*), count(distinct sector), min(sector), max(sector), avg(nr_sector) from trace")

duck("with trace as ( select row_number() over () as time, * from read_csv('"%+%file%+%"', delim = ','))
	  select count(*), count(distinct sector), min(sector), max(sector), avg(nr_sector) from trace")

# sample
trace = duck("select * from trace where time % 5000 == 0  ")
plot <- ggplot(trace, aes(x=time, y=sector*512/1e9)) +
  geom_point(size=0.1) +
  scale_x_continuous() +
  expand_limits(y=0) +
  theme_bw()
pdf("bpf_output_plot.pdf", width=8, height=6)
print(plot)
dev.off()

# 
trace = duck("select * from trace where sector < 15*1e9/512 and time > 5.0e+07 limit 10000 ")
plot <- ggplot(trace, aes(x=time, y=sector*512/1e9)) +
  geom_point(size=0.1) +
  scale_x_continuous() +
  expand_limits(y=0) +
  theme_bw()
pdf("bpf_output_plot.pdf", width=8, height=6)
print(plot)
dev.off()



# TX
#prefix='f31'
#prefix='f33nosteady'
prefix='f37'
#s0ssd = 'nvme2'
#m0ssd = 'nvme11'
#m0ssd = 'nvme14'
logcr = duck("select * from 'build/log_*_"%+%prefix%+%"*_cr.csv'")
logbm = duck("select * from 'build/log_*_"%+%prefix%+%"*_bm.csv'")
logco = duck("select * from 'build/log_*_"%+%prefix%+%"*_configs.csv'")
log = duck("select * from logcr
									join logbm using (c_hash, t,key)
									join logco using(c_hash, t)")
log = duck("select c_tag, c_hash, key, min(t) as t, avg(tx) as tx 
					from log 
					group by c_tag, c_hash, key, (t/1)::integer")
log <- duck("
  SELECT 
    *,
    regexp_extract(c_tag, 'tpcc-(nvme[^_]+)', 1) AS c_ssd_path,
    regexp_extract(c_tag, '_([0-9]+)$', 1) AS c_tx_rate
  FROM log
")

head(log)

unique(log$c_tag)

duck("select count(*) from log")
#names(log)[duplicated(names(log))]
logm  = melt(log, id = c('t', 'c_hash', 'c_tag', 'c_ssd_path', 'c_tx_rate', 'key'))
logmf = duck("select * 
				 from logm 
				where t > 0 and (variable ='tx' or variable = 'txiavg')")
plot <- ggplot(logmf, aes(x=t, y=value, color=factor(c_tx_rate))) +
  geom_point(size=0.2, alpha=0.3) +
  geom_smooth() +
  expand_limits(y=0) +
  facet_grid(variable ~ c_ssd_path)+
  scale_x_continuous(limits=c(0, 30000)) +
  theme_bw()
pdf("lean_output_plot.pdf", width=16, height=12)
print(plot)
dev.off()

duck("select avg(tx) from log where t > 10000 group by ssd")

# f19 "2025-04-29 00:15:10" / oder mind 545 sekunden vorher

#tpcc_run = "2025-04-29 00:06:0i0"
tpcc_run = "2025-05-06 22:30:07"
json =duck("select 's0' as ssd,* from 'build/log_nvme_"%+%s0ssd%+%"_"%+%prefix%+%".log.csv'
			  union all select 'm0' as ssd,* from 'build/log_nvme_"%+%m0ssd%+%"_"%+%prefix%+%".log.csv'")
smart = duck("SELECT
	 ssd,
  timestamp,
  row_number() over (partition by ssd order by timestamp) as t,
  json_extract(ocp_json, '$.\"Physical media units written\".lo')::BIGINT  / 1024 AS physical_media_units_written_lo,
  json_extract(smart_json, '$.data_units_written')::BIGINT * 1000*512/1024 AS data_units_written
FROM
	json
;")
wa = duck("
WITH diffs AS (
  SELECT
    *, physical_media_units_written_lo - LAG(physical_media_units_written_lo) OVER (partition by ssd ORDER BY timestamp) AS delta_physical,
    data_units_written - LAG(data_units_written) OVER (partition by ssd ORDER BY timestamp) AS delta_logical
  FROM
	smart 
)
SELECT
  *, CASE 
    WHEN delta_logical > 0 THEN delta_physical::DOUBLE / delta_logical
    ELSE NULL
  END AS wa
FROM
  diffs
ORDER BY
  timestamp
;")
head(wa, 10)
plot <- ggplot(wa, aes(x=timestamp, y=wa, color=factor(ssd))) +
  geom_point(size=1) +
  geom_vline(xintercept = as.numeric(as.POSIXct(tpcc_run, tz = "UTC")), color = "red", linetype = "dashed", size = 1) +
  theme_bw()
pdf("lean_output_plot_wa.pdf", width=8, height=6)
print(plot)
dev.off()




duck("select  wal_write_gib, count(*) from 'build/log_cr.csv' group by wal_write_gib")

logcr = duck("select * from 'build/log_cr.csv' where wal_write_gib < 10")
plot <- ggplot(logcr, aes(x=t, y=wal_write_gib, color=factor(c_hash))) +
  geom_point(size=0.1, alpha=0.04) +
  scale_x_continuous() +
  expand_limits(y=0) +
  facet_grid(~ c_hash)+
  theme_bw()
pdf("bpf_output_plot.pdf", width=8, height=6)
print(plot)
dev.off()



#############################################################################################
# old traces

#duck("from sniff_csv('/home/haas/leanstore-main/build/iotrace_32.csv')")

file="/home/haas/leanstore-main/build/iotrace_32.csv"
#duck("select count(*) from read_csv('"%+%file%+%"', delim = ',', ignore_errors=true)")

# unique pids
#duck("select count(distinct pid)
#		from read_csv('"%+%file%+%"', delim = ',', ignore_errors=true)
#		")


if (FALSE) {
df = duck("select dt_id, count(*) as cnt
				from read_csv('"%+%file%+%"', delim = ',', ignore_errors=true)
				where pid::INT64 <> 0
				group by dt_id
				order by cnt desc
		")
df
}

if (TRUE) {

print("hist")

hist = duck("with trace as (
					select 64 as buffer, * from read_csv('iotrace_64.csv', delim = ',', ignore_errors=true) where pid::INT64 <> 0
					UNION ALL select 32 as buffer, * from read_csv('iotrace_32.csv', delim = ',', ignore_errors=true) where pid::INT64 <> 0
					UNION ALL select 16 as buffer, * from read_csv('iotrace_16.csv', delim = ',', ignore_errors=true) where pid::INT64 <> 0
					UNION ALL select  8 as buffer, * from read_csv('iotrace_8.csv', delim = ',', ignore_errors=true) where pid::INT64 <> 0
					UNION ALL select  4 as buffer, * from read_csv('iotrace_4.csv', delim = ',', ignore_errors=true) where pid::INT64 <> 0
				),
				totaltrace as (
					select buffer, count(*) as totalaccesses
					from trace
					group by buffer
				),
				hist as (
					select buffer, pid, count(*) cnt
					from trace
					group by buffer, pid
				),
				ridhist as (
					select row_number() over (partition by buffer order by cnt desc) -1 rid, *
					from hist
				),
				buckethist as (
					select buffer, FLOOR(rid/100) as rid, SUM(cnt) as cnt
					from ridhist
					group by buffer, FLOOR(rid/100)
				),
				relhist as (
					select buffer, rid, cnt, cnt / totalaccesses as pct
					from buckethist join totaltrace using (buffer)
				)
				select * from relhist
		")

print("hist")
print(hist)

print("sums")
print(duck("select buffer, sum(pct) from hist group by buffer"))

plot <- ggplot(hist, aes(x=rid, y=pct, color=factor(buffer))) +
  geom_line() +
  scale_x_continuous() +
  expand_limits(y=0) +
  theme_bw()
pdf("output_plot.pdf", width=8, height=6)
print(plot)
dev.off()

plot <- ggplot(hist, aes(x=rid, y=pct, color=factor(buffer))) +
  geom_line() +
  scale_x_log10() +
  expand_limits(y=0) +
  theme_bw()
pdf("output_plot2.pdf", width=8, height=6)
print(plot)
dev.off()

plot <- ggplot(hist, aes(x=rid, y=pct, color=factor(buffer))) +
  geom_line() +
  scale_y_log10() +
  expand_limits(y=0) +
  theme_bw()
pdf("output_plot3.pdf", width=8, height=6)
print(plot)
dev.off()
}

if (FALSE) {
df = duck("with cntperpid as (select pid, count(*) as cnt
				from read_csv('"%+%file%+%"', delim = ',', ignore_errors=true)
				where pid::INT64 <> 0
				group by pid
			)
			select cnt, count(*) cntcnt
				from cntperpid
				group by cnt
				order by cntcnt desc
		")
head(df, 100)
plot <- ggplot(df, aes(x=as.numeric(cnt), y=cntcnt)) +
  geom_line() +
  scale_x_continuous() +
  theme_bw()

pdf("output_plot.pdf", width=8, height=6)
print(plot)
dev.off()
}

if (FALSE) {
df = duck("with trace as (select * from read_csv('"%+%file%+%"', delim = ',', ignore_errors=true)),
			 cntperpid as (select dt_id, pid, count(*) as cnt
				from trace
				where pid::INT64 <> 0
				group by dt_id, pid
			),
			cntindt as (select dt_id from trace group by dt_id having count(*) > 10000)
			select dt_id, cnt, count(*) cntcnt
				from cntperpid join cntindt using (dt_id)
				group by dt_id, cnt
				order by cntcnt desc
		")
head(df, 100)
plot <- ggplot(df, aes(x=as.numeric(cnt), y=cntcnt, color=dt_id)) +
  geom_line() +
  scale_x_continuous() +
  facet_wrap(~ dt_id) +
  theme_bw() +
  theme(axis.text.x = element_text(angle=90, hjust=1))
pdf("output_plot.pdf", width=8, height=6)
print(plot)
dev.off()

}


#duck("select count(*) from read_csv('/home/haas/leanstore-main/build/iotrace_32.csv', delim = ',', ignore_errors=true)")
#duck("select 1")

