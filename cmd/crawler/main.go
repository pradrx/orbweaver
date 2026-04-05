package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

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

	workerCount := 10
	if v := os.Getenv("WORKER_COUNT"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "WORKER_COUNT must be a positive integer\n")
			os.Exit(1)
		}
		workerCount = n
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		slog.Error("failed to load AWS config", "error", err)
		os.Exit(1)
	}

	sqsClient := sqs.NewFromConfig(cfg)
	httpClient := &http.Client{Timeout: 30 * time.Second}

	slog.Info("crawler starting", "queue_url", queueURL, "workers", workerCount)
	run(ctx, sqsClient, httpClient, queueURL, workerCount)
	slog.Info("crawler shut down")
}

func run(ctx context.Context, sqsClient *sqs.Client, httpClient *http.Client, queueURL string, workerCount int) {
	var wg sync.WaitGroup
	for i := range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("worker started", "worker_id", i)
			poll(ctx, sqsClient, httpClient, queueURL, i)
			slog.Info("worker stopped", "worker_id", i)
		}()
	}
	wg.Wait()
}

func poll(ctx context.Context, sqsClient *sqs.Client, httpClient *http.Client, queueURL string, workerID int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("failed to receive messages", "error", err, "worker_id", workerID)
			time.Sleep(5 * time.Second)
			continue
		}

		for _, msg := range msgs.Messages {
			processMessage(ctx, sqsClient, httpClient, queueURL, msg, workerID)
		}
	}
}

func processMessage(ctx context.Context, sqsClient *sqs.Client, httpClient *http.Client, queueURL string, msg types.Message, workerID int) {
	url := ""
	if msg.Body != nil {
		url = *msg.Body
	}
	if url == "" {
		slog.Warn("skipping message with empty body", "message_id", *msg.MessageId)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	slog.Info("fetching", "url", url, "message_id", *msg.MessageId, "worker_id", workerID)

	resp, err := httpClient.Get(url)
	if err != nil {
		slog.Error("fetch failed", "url", url, "error", err)
		return
	}
	defer resp.Body.Close()

	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		slog.Error("failed to read response body", "url", url, "error", err)
		return
	}

	slog.Info("fetched",
		"url", url,
		"status", resp.StatusCode,
		"content_length", n,
		"content_type", resp.Header.Get("Content-Type"),
		"message_id", *msg.MessageId,
		"worker_id", workerID,
	)

	deleteMessage(ctx, sqsClient, queueURL, msg)
}

func deleteMessage(ctx context.Context, sqsClient *sqs.Client, queueURL string, msg types.Message) {
	_, err := sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		slog.Error("failed to delete message", "message_id", *msg.MessageId, "error", err)
	}
}
