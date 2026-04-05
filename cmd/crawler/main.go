package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const maxBodySize = 5 * 1024 * 1024 // 5 MB

func main() {
	queueURL := os.Getenv("QUEUE_URL")
	if queueURL == "" {
		fmt.Fprintf(os.Stderr, "QUEUE_URL environment variable is required\n")
		os.Exit(1)
	}

	parserQueueURL := os.Getenv("PARSER_QUEUE_URL")
	if parserQueueURL == "" {
		fmt.Fprintf(os.Stderr, "PARSER_QUEUE_URL environment variable is required\n")
		os.Exit(1)
	}

	s3Bucket := os.Getenv("S3_BUCKET")
	if s3Bucket == "" {
		fmt.Fprintf(os.Stderr, "S3_BUCKET environment variable is required\n")
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
	s3Client := s3.NewFromConfig(cfg)
	httpClient := &http.Client{Timeout: 30 * time.Second}

	slog.Info("crawler starting",
		"queue_url", queueURL,
		"parser_queue_url", parserQueueURL,
		"s3_bucket", s3Bucket,
		"workers", workerCount,
	)
	run(ctx, sqsClient, s3Client, httpClient, queueURL, parserQueueURL, s3Bucket, workerCount)
	slog.Info("crawler shut down")
}

func run(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, httpClient *http.Client, queueURL, parserQueueURL, s3Bucket string, workerCount int) {
	var wg sync.WaitGroup
	for i := range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("worker started", "worker_id", i)
			poll(ctx, sqsClient, s3Client, httpClient, queueURL, parserQueueURL, s3Bucket, i)
			slog.Info("worker stopped", "worker_id", i)
		}()
	}
	wg.Wait()
}

func poll(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, httpClient *http.Client, queueURL, parserQueueURL, s3Bucket string, workerID int) {
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
			processMessage(ctx, sqsClient, s3Client, httpClient, queueURL, parserQueueURL, s3Bucket, msg, workerID)
		}
	}
}

func processMessage(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, httpClient *http.Client, queueURL, parserQueueURL, s3Bucket string, msg types.Message, workerID int) {
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

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Info("skipping non-2xx response",
			"url", url,
			"status", resp.StatusCode,
			"message_id", *msg.MessageId,
			"worker_id", workerID,
		)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(contentType, "text/html") {
		slog.Info("skipping non-html content type",
			"url", url,
			"content_type", contentType,
			"message_id", *msg.MessageId,
			"worker_id", workerID,
		)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize+1))
	if err != nil {
		slog.Error("failed to read response body", "url", url, "error", err)
		return
	}
	if len(body) > maxBodySize {
		slog.Info("skipping oversized response",
			"url", url,
			"size", len(body),
			"message_id", *msg.MessageId,
			"worker_id", workerID,
		)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	hash := sha256.Sum256([]byte(url))
	s3Key := "html/" + hex.EncodeToString(hash[:]) + ".html"

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s3Bucket,
		Key:         &s3Key,
		Body:        bytes.NewReader(body),
		ContentType: &contentType,
	})
	if err != nil {
		slog.Error("s3 upload failed", "url", url, "s3_key", s3Key, "error", err)
		return
	}

	parserMsg, err := json.Marshal(struct {
		URL   string `json:"url"`
		S3Key string `json:"s3_key"`
	}{
		URL:   url,
		S3Key: s3Key,
	})
	if err != nil {
		slog.Error("failed to marshal parser message", "url", url, "error", err)
		return
	}
	parserMsgStr := string(parserMsg)
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &parserQueueURL,
		MessageBody: &parserMsgStr,
	})
	if err != nil {
		slog.Error("failed to send parser message", "url", url, "error", err)
		return
	}

	slog.Info("processed",
		"url", url,
		"s3_key", s3Key,
		"status", resp.StatusCode,
		"content_length", len(body),
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
