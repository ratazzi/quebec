# bpftrace scripts for Quebec's USDT probes

Quebec's extension module carries two SystemTap SDT (USDT) probes on Linux.
They compile to a single `nop` each, so they cost nothing until a tracer
attaches. Check they are present:

```bash
SO=$(python -c "import quebec, os; print(os.path.dirname(quebec.__file__))")/quebec.abi3.so
readelf -n "$SO" | grep -A3 stapsdt
sudo bpftrace -l "usdt:$SO:quebec:*"
```

| probe | arguments |
|---|---|
| `quebec:job_start` | `arg0,arg1` jid (ptr,len) · `arg2,arg3` class (ptr,len) · `arg4,arg5` queue (ptr,len) |
| `quebec:job_end` | `arg0,arg1` jid · `arg2,arg3` class · `arg4` ok (1/0) · `arg5` duration ns · `arg6` minflt · `arg7` new RSS bytes (counters are `-1` when unavailable) |

Strings are passed as pointer + length, so read them with `str(argN, argN+1)`.
Both probes fire on the thread that runs the job, so `tid` inside a handler is
the job's kernel thread id.

- `job_latency.bt` — duration and new-RSS histograms per class, failure counts.
- `job_memory_peak.bt` — exact per-job peak of anonymous RSS per class via
  `kmem:rss_stat`; needs root and shows reused memory the built-in metric cannot.

Pass the `.so` path as the first positional argument, or attach by pid with
`-p` and drop the path (`usdt::quebec:job_end`).
