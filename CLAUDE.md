# Orbweaver

Distributed web crawler written in Go, hosted on AWS.

## Architecture

Two stateless worker types connected by SQS queues:

- **Crawl workers** — consume URLs from the URL queue, fetch pages, store raw HTML in S3, and push references to the parser queue
- **Parser workers** — consume from the parser queue, extract links from HTML, and enqueue discovered URLs back to the URL queue

### AWS Resources

- **SQS**: URL queue (+ DLQ), parser queue (+ DLQ)
- **S3**: raw HTML storage
- **ECS/EC2**: worker containers
- **DynamoDB**: URL deduplication / crawl state

### Key Design Decisions

- Standard SQS queues (not FIFO) — crawling is idempotent and throughput matters more than ordering
- Workers are stateless binaries designed for containerized deployment
- Separate binaries for crawling and parsing so they scale independently

## Project Structure

```
cmd/crawler/     — crawl worker binary
cmd/parser/      — parser worker binary
internal/        — shared packages (config, queue client, etc.)
infra/           — CloudFormation templates
```

## Building

This is a Go monorepo. Module path: `github.com/pradrx/orbweaver`

```
go build ./cmd/crawler
go build ./cmd/parser
```

## Deploying Infrastructure

```
aws cloudformation deploy \
  --template-file infra/cloudformation.yaml \
  --stack-name orbweaver \
  --parameter-overrides Environment=dev
```
