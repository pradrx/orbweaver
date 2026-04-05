package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func main() {
	queueURL := os.Getenv("QUEUE_URL")
	if queueURL == "" {
		fmt.Fprintf(os.Stderr, "QUEUE_URL environment variable is required\n")
		os.Exit(1)
	}

	path := "urls.txt"
	if len(os.Args) >= 2 {
		path = os.Args[1]
	}

	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open file: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()

	var urls []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if line := scanner.Text(); line != "" {
			urls = append(urls, line)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "error reading file: %v\n", err)
		os.Exit(1)
	}

	if len(urls) == 0 {
		fmt.Fprintf(os.Stderr, "no URLs found in %s\n", path)
		os.Exit(1)
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load AWS config: %v\n", err)
		os.Exit(1)
	}

	client := sqs.NewFromConfig(cfg)

	// SQS SendMessageBatch supports up to 10 messages per call.
	for i := 0; i < len(urls); i += 10 {
		end := min(i+10, len(urls))
		batch := urls[i:end]

		entries := make([]types.SendMessageBatchRequestEntry, len(batch))
		for j, u := range batch {
			entries[j] = types.SendMessageBatchRequestEntry{
				Id:          aws.String(strconv.Itoa(i + j)),
				MessageBody: aws.String(u),
			}
		}

		out, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
			QueueUrl: &queueURL,
			Entries:  entries,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "SendMessageBatch failed: %v\n", err)
			os.Exit(1)
		}

		for _, f := range out.Failed {
			fmt.Fprintf(os.Stderr, "failed to enqueue entry %s: %s\n", *f.Id, *f.Message)
		}
		fmt.Printf("enqueued %d URLs\n", len(out.Successful))
	}
}
