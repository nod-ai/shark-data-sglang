# LLM Server Benchmark Dashboard

Automated dashboard for tracking and comparing performance metrics between Shortfin LLM Server and SGLang Server.

## Overview

This project collects daily performance metrics from two LLM servers:
- Shortfin LLM Server with SGLang frontend integration
- SGLang's native LLM server (baseline)

## Metrics Collected

For each server at varying request rates (1, 2, 4, 8, 16, 32):

- Median E2E Latency (ms)
- Median TTFT (Time to First Token)
- Median ITL (Inter-Token Latency)
- Request Throughput (req/s)
- Benchmark Duration (s)

## Data Collection

### Input Format

Data is collected in jsonlines files, named according to the pattern:
```
{server}_{date}_{request_rate}.jsonl
```

Example:
```
shortfin_10_1.jsonl
shortfin_10_2.jsonl
...
sglang_10_1.jsonl
sglang_10_2.jsonl
```

### Collection Frequency
- Runs nightly via CI
- Data refresh rate: Daily
- Initial retention period: 3 months (configurable)

## Dashboard Details

### Grafana Integration
- Integrated with existing Grafana instance
- Tracks performance improvements over time
- Compares Shortfin vs SGLang server performance

### Visualization Goals
- Track Shortfin server improvements
- Benchmark against SGLang baseline
- Identify performance trends and regressions

## References

- [Shortfin LLM with SGLang Documentation](https://nod-ai.github.io/shark-ai/llm/sglang/index.html?sort=result)
- Performance test results can be viewed at the above link

## Future Enhancements

- Automate data collection
- Additional metrics collection
- Enhanced visualization options
