# Comparison

generated: 2026-11-16T23:54:06Z

Overall verdict: FAIL

## Settings

- base: 7409395
- candidate: 7409395
- runs per side: 3
- threshold: 5.0%
- min valid runs: 3
- profiles: latency
- scenarios: basic, mixed
- candidate env: BENCH_LATENCY_MS=3

## Environment

| key | base | candidate |
| --- | --- | --- |
| arch | aarch64-apple-darwin25.5.0 | aarch64-apple-darwin25.5.0 |
| deps | benchee 1.5.1, jason 1.4.5, kayrock 1.1.0, telemetry 1.4.2 | benchee 1.5.1, jason 1.4.5, kayrock 1.1.0, telemetry 1.4.2 |
| elixir | 1.19.5 | 1.19.5 |
| erts | 16.3.1 | 16.3.1 |
| harness_dirty | false | false |
| harness_sha | eb354c417087ffdf940675c5a2ecbf8b582c6539 | eb354c417087ffdf940675c5a2ecbf8b582c6539 |
| kafka_ex_dirty | false | false |
| kafka_ex_sha | 7409395401fdf36521052cded786168cb5328bca | 7409395401fdf36521052cded786168cb5328bca |
| kafka_ex_vsn | 1.2.0 | 1.2.0 |
| latency_ms | 2 | 3 |
| logical_processors | 18 | 18 |
| os | unix darwin 25.5.0 | unix darwin 25.5.0 |
| otp | 28 | 28 |
| profile | latency | latency |
| scheduler_bind_type | unbound | unbound |
| schedulers_online | 8 | 8 |
| toxiproxy_version | 2.9.0 | 2.9.0 |

## latency / basic: FAIL

| job | metric | usable base/cand | base median | candidate median | delta % | noise % | limit % | verdict | reason |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| produce_c1 | p50_us | 3/3 vs 3/3 | 3160.0 | 4576.0 | 44.81 | 0.60 | 5.00 | FAIL | worse by 44.81%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| produce_c1 | vm_reductions_per_op | 3/3 vs 3/3 | 1608.9 | 1610.6 | 0.11 | 0.18 | 5.00 | PASS | within 5.00% (usable: base 3/3, candidate 3/3) |
| produce_c10 | p50_us | 3/3 vs 3/3 | 31321.0 | 45069.0 | 43.89 | 0.97 | 5.00 | FAIL | worse by 43.89%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| produce_c10 | throughput_ops | 3/3 vs 3/3 | 307.0 | 218.4 | 28.85 | 1.02 | 5.00 | FAIL | worse by 28.85%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| produce_c10 | vm_reductions_per_op | 3/3 vs 3/3 | 1612.6 | 1616.4 | 0.24 | 0.07 | 5.00 | PASS | within 5.00% (usable: base 3/3, candidate 3/3) |
| produce_c100 | p50_us | 3/3 vs 3/3 | 326538.0 | 465864.0 | 42.67 | 2.25 | 5.00 | FAIL | worse by 42.67%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| produce_c100 | throughput_ops | 3/3 vs 3/3 | 307.5 | 221.4 | 28.01 | 1.37 | 5.00 | FAIL | worse by 28.01%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| produce_c100 | vm_reductions_per_op | 3/3 vs 3/3 | 1635.4 | 1648.0 | 0.77 | 0.09 | 5.00 | PASS | within 5.00% (usable: base 3/3, candidate 3/3) |
| fetch_c1 | p50_us | 3/3 vs 3/3 | 3534.0 | 4772.0 | 35.03 | 0.63 | 5.00 | FAIL | worse by 35.03%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| fetch_c1 | vm_reductions_per_op | 3/3 vs 3/3 | 26369.5 | 26377.6 | 0.03 | 0.05 | 5.00 | PASS | within 5.00% (usable: base 3/3, candidate 3/3) |
| fetch_c10 | p50_us | 3/3 vs 3/3 | 35284.0 | 47404.0 | 34.35 | 0.68 | 5.00 | FAIL | worse by 34.35%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| fetch_c10 | throughput_ops | 3/3 vs 3/3 | 277.3 | 209.5 | 24.43 | 0.91 | 5.00 | FAIL | worse by 24.43%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| fetch_c10 | vm_reductions_per_op | 3/3 vs 3/3 | 26380.8 | 26389.8 | 0.03 | 0.04 | 5.00 | PASS | within 5.00% (usable: base 3/3, candidate 3/3) |
| fetch_c100 | p50_us | 3/3 vs 3/3 | 355468.0 | 476211.0 | 33.97 | 1.04 | 5.00 | FAIL | worse by 33.97%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| fetch_c100 | throughput_ops | 3/3 vs 3/3 | 283.5 | 214.3 | 24.41 | 0.91 | 5.00 | FAIL | worse by 24.41%, limit 5.00% (usable: base 3/3, candidate 3/3) |
| fetch_c100 | vm_reductions_per_op | 3/3 vs 3/3 | 26443.4 | 26444.6 | 0.00 | 0.03 | 5.00 | PASS | within 5.00% (usable: base 3/3, candidate 3/3) |

Informational:

| job | metric | base median | candidate median | worse Δ % | noise % |
| --- | --- | --- | --- | --- | --- |
| produce_c1 | throughput_ops | 306.5 | 216.9 | 29.23 | 1.04 |
| produce_c1 | client_reductions_per_op | 874.4 | 874.3 | -0.01 | 0.33 |
| produce_c10 | client_reductions_per_op | 875.7 | 877.4 | 0.19 | 0.17 |
| produce_c100 | client_reductions_per_op | 881.5 | 883.8 | 0.26 | 0.12 |
| fetch_c1 | throughput_ops | 279.1 | 209.5 | 24.94 | 0.90 |
| fetch_c1 | client_reductions_per_op | 11168.6 | 11168.9 | 0.00 | 0.10 |
| fetch_c10 | client_reductions_per_op | 11180.9 | 11178.9 | -0.02 | 0.12 |
| fetch_c100 | client_reductions_per_op | 11231.8 | 11227.8 | -0.04 | 0.10 |

Informational:

| job | metric | base median | candidate median | raw Δ % | noise % |
| --- | --- | --- | --- | --- | --- |
| produce_c1 | p99_us | 4138.0 | 5726.0 | 38.38 | 3.28 |
| produce_c1 | vm_cpu_us_per_op | 50.6 | 55.0 | 8.66 | 10.17 |
| produce_c1 | max_mailbox | 1.0 | 0.0 | -100.00 | 86.60 |
| produce_c10 | p99_us | 42801.0 | 57066.0 | 33.33 | 6.05 |
| produce_c10 | vm_cpu_us_per_op | 51.9 | 56.1 | 8.26 | 7.40 |
| produce_c10 | max_mailbox | 10.0 | 10.0 | 0.00 | 5.59 |
| produce_c100 | p99_us | 376510.0 | 506690.0 | 34.58 | 1.62 |
| produce_c100 | vm_cpu_us_per_op | 62.6 | 73.9 | 17.99 | 6.86 |
| produce_c100 | max_mailbox | 100.0 | 100.0 | 0.00 | 0.00 |
| fetch_c1 | p99_us | 4453.0 | 5827.0 | 30.86 | 0.96 |
| fetch_c1 | vm_cpu_us_per_op | 144.9 | 140.8 | -2.83 | 5.35 |
| fetch_c1 | max_mailbox | 1.0 | 0.0 | -100.00 | 173.21 |
| fetch_c10 | p99_us | 43108.0 | 56308.0 | 30.62 | 3.73 |
| fetch_c10 | vm_cpu_us_per_op | 145.1 | 147.9 | 1.96 | 4.14 |
| fetch_c10 | max_mailbox | 10.0 | 10.0 | 0.00 | 0.00 |
| fetch_c100 | p99_us | 404062.0 | 518677.0 | 28.37 | 1.54 |
| fetch_c100 | vm_cpu_us_per_op | 154.6 | 169.3 | 9.48 | 4.33 |
| fetch_c100 | max_mailbox | 100.0 | 100.0 | 0.00 | 0.00 |

## latency / mixed: PASS

| job | metric | usable base/cand | base median | candidate median | delta % | noise % | limit % | verdict | reason |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| produce_under_longpoll | p50_us | 3/3 vs 3/3 | 319858.0 | 324936.0 | 1.59 | 0.08 | 5.00 | PASS | within 5.00% (usable: base 3/3, candidate 3/3) |
| metadata_under_longpoll | p50_us | 3/3 vs 3/3 | 318838.0 | 324117.0 | 1.66 | 0.09 | 5.00 | PASS | within 5.00% (usable: base 3/3, candidate 3/3) |

Informational:

| job | metric | base median | candidate median | worse Δ % | noise % |
| --- | --- | --- | --- | --- | --- |
| produce_under_longpoll | throughput_ops | 3.1 | 3.1 | 1.36 | 0.17 |
| metadata_under_longpoll | throughput_ops | 3.1 | 3.1 | 1.54 | 0.11 |

Informational:

| job | metric | base median | candidate median | raw Δ % | noise % |
| --- | --- | --- | --- | --- | --- |
| produce_under_longpoll | p99_us | 335688.0 | 340076.0 | 1.31 | 1.52 |
| produce_under_longpoll | vm_cpu_us_per_op | 776.6 | 795.7 | 2.46 | 6.55 |
| produce_under_longpoll | vm_reductions_per_op | 6141.5 | 6139.7 | -0.03 | 0.18 |
| produce_under_longpoll | client_reductions_per_op | 3617.1 | 3619.0 | 0.05 | 0.31 |
| produce_under_longpoll | max_mailbox | 4.0 | 4.0 | 0.00 | 0.00 |
| metadata_under_longpoll | p99_us | 333647.0 | 336834.0 | 0.96 | 0.89 |
| metadata_under_longpoll | vm_cpu_us_per_op | 797.9 | 795.7 | -0.27 | 6.38 |
| metadata_under_longpoll | vm_reductions_per_op | 7096.1 | 7101.5 | 0.08 | 0.22 |
| metadata_under_longpoll | client_reductions_per_op | 4731.0 | 4728.5 | -0.05 | 0.22 |
| metadata_under_longpoll | max_mailbox | 4.0 | 4.0 | 0.00 | 0.00 |

Extras (median or sum across runs):

| job | metric | base | candidate | aggregate |
| --- | --- | --- | --- | --- |
| produce_under_longpoll | background_ops | 303 | 300 | median |
| metadata_under_longpoll | background_ops | 303 | 300 | median |

