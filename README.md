# Orbweaver

Distributed web crawler built with Go and AWS.

## Overview

Two stateless worker types connected by SQS queues:

- **Crawler** — polls URLs from SQS, fetches the page, stores raw HTML in S3, pushes a message to the parser queue.
- **Parser** — pulls HTML from S3, extracts visible text and links, writes parsed JSON back to S3.

A **seed** CLI reads URLs from a file and batch-enqueues them to kick off a crawl.

Workers run as concurrent Go binaries on ECS/Fargate and scale independently.

## Infrastructure

SQS queues (with DLQs), S3 for storage, ECS/Fargate for compute. Everything is defined in `infra/cloudformation.yaml`.

The crawler optionally supports per-domain rate limiting via Redis with exponential backoff.

## Project Structure

```
cmd/crawler/    crawl worker
cmd/parser/     parser worker
cmd/seed/       seed URL loader
infra/          CloudFormation templates
```

## Build

```
go build ./cmd/crawler
go build ./cmd/parser
go build ./cmd/seed
```

## Run

```sh
# crawler
QUEUE_URL=<url-queue> PARSER_QUEUE_URL=<parser-queue> S3_BUCKET=<bucket> ./crawler

# parser
PARSER_QUEUE_URL=<parser-queue> S3_BUCKET=<bucket> ./parser

# seed
QUEUE_URL=<url-queue> ./seed urls.txt
```

Optional crawler env vars: `WORKER_COUNT` (default 10), `REDIS_ENDPOINT`, `RATE_LIMIT_PER_SECOND` (default 1).

## Deploy

```sh
aws cloudformation deploy \
  --template-file infra/cloudformation.yaml \
  --stack-name orbweaver \
  --parameter-overrides Environment=dev \
  --capabilities CAPABILITY_NAMED_IAM
```
