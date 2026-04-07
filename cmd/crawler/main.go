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
	"math"
	"net/http"
	"net/url"
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
	redis_rate "github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
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

	redisEndpoint := os.Getenv("REDIS_ENDPOINT")

	rateLimitPerSecond := 1
	if v := os.Getenv("RATE_LIMIT_PER_SECOND"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "RATE_LIMIT_PER_SECOND must be a positive integer\n")
			os.Exit(1)
		}
		rateLimitPerSecond = n
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

	var limiter *redis_rate.Limiter
	if redisEndpoint != "" {
		rdb := redis.NewClient(&redis.Options{
			Addr:         redisEndpoint,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
			DialTimeout:  3 * time.Second,
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			slog.Warn("redis not reachable, rate limiting disabled", "endpoint", redisEndpoint, "error", err)
		} else {
			limiter = redis_rate.NewLimiter(rdb)
			slog.Info("rate limiter enabled", "endpoint", redisEndpoint, "rate_per_second", rateLimitPerSecond)
		}
	}

	slog.Info("crawler starting",
		"queue_url", queueURL,
		"parser_queue_url", parserQueueURL,
		"s3_bucket", s3Bucket,
		"workers", workerCount,
	)
	run(ctx, sqsClient, s3Client, httpClient, queueURL, parserQueueURL, s3Bucket, workerCount, limiter, rateLimitPerSecond)
	slog.Info("crawler shut down")
}

func run(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, httpClient *http.Client, queueURL, parserQueueURL, s3Bucket string, workerCount int, limiter *redis_rate.Limiter, rateLimitPerSecond int) {
	var wg sync.WaitGroup
	for i := range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("worker started", "worker_id", i)
			poll(ctx, sqsClient, s3Client, httpClient, queueURL, parserQueueURL, s3Bucket, i, limiter, rateLimitPerSecond)
			slog.Info("worker stopped", "worker_id", i)
		}()
	}
	wg.Wait()
}

func poll(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, httpClient *http.Client, queueURL, parserQueueURL, s3Bucket string, workerID int, limiter *redis_rate.Limiter, rateLimitPerSecond int) {
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
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
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
			processMessage(ctx, sqsClient, s3Client, httpClient, queueURL, parserQueueURL, s3Bucket, msg, workerID, limiter, rateLimitPerSecond)
		}
	}
}

func processMessage(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, httpClient *http.Client, queueURL, parserQueueURL, s3Bucket string, msg types.Message, workerID int, limiter *redis_rate.Limiter, rateLimitPerSecond int) {
	rawURL := ""
	if msg.Body != nil {
		rawURL = *msg.Body
	}
	if rawURL == "" {
		slog.Warn("skipping message with empty body", "message_id", *msg.MessageId)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	if limiter != nil {
		domain, err := extractDomain(rawURL)
		if err != nil {
			slog.Warn("failed to parse domain, skipping rate limit", "url", rawURL, "error", err, "worker_id", workerID)
		} else {
			res, err := limiter.Allow(ctx, "crawl:"+domain, redis_rate.PerSecond(rateLimitPerSecond))
			if err != nil {
				slog.Warn("rate limiter error, allowing request", "domain", domain, "url", rawURL, "error", err, "worker_id", workerID)
			} else if res.Allowed == 0 {
				delay := rateLimitBackoff(msg)
				slog.Info("rate limited, deferring message",
					"domain", domain,
					"url", rawURL,
					"delay_seconds", delay,
					"message_id", *msg.MessageId,
					"worker_id", workerID,
				)
				changeVisibility(ctx, sqsClient, queueURL, msg, delay)
				return
			}
		}
	}

	slog.Info("fetching", "url", rawURL, "message_id", *msg.MessageId, "worker_id", workerID)

	resp, err := httpClient.Get(rawURL)
	if err != nil {
		slog.Error("fetch failed", "url", rawURL, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Info("skipping non-2xx response",
			"url", rawURL,
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
			"url", rawURL,
			"content_type", contentType,
			"message_id", *msg.MessageId,
			"worker_id", workerID,
		)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodySize+1))
	if err != nil {
		slog.Error("failed to read response body", "url", rawURL, "error", err)
		return
	}
	if len(body) > maxBodySize {
		slog.Info("skipping oversized response",
			"url", rawURL,
			"size", len(body),
			"message_id", *msg.MessageId,
			"worker_id", workerID,
		)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	hash := sha256.Sum256([]byte(rawURL))
	s3Key := "html/" + hex.EncodeToString(hash[:]) + ".html"

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s3Bucket,
		Key:         &s3Key,
		Body:        bytes.NewReader(body),
		ContentType: &contentType,
	})
	if err != nil {
		slog.Error("s3 upload failed", "url", rawURL, "s3_key", s3Key, "error", err)
		return
	}

	parserMsg, err := json.Marshal(struct {
		URL   string `json:"url"`
		S3Key string `json:"s3_key"`
	}{
		URL:   rawURL,
		S3Key: s3Key,
	})
	if err != nil {
		slog.Error("failed to marshal parser message", "url", rawURL, "error", err)
		return
	}
	parserMsgStr := string(parserMsg)
	_, err = sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    &parserQueueURL,
		MessageBody: &parserMsgStr,
	})
	if err != nil {
		slog.Error("failed to send parser message", "url", rawURL, "error", err)
		return
	}

	slog.Info("processed",
		"url", rawURL,
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

func extractDomain(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("empty hostname in URL: %s", rawURL)
	}
	return strings.ToLower(host), nil
}

func rateLimitBackoff(msg types.Message) int32 {
	const (
		baseDelay = 30  // seconds
		maxDelay  = 300 // 5 minutes
	)
	receiveCount := 1
	if v, ok := msg.Attributes["ApproximateReceiveCount"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			receiveCount = n
		}
	}
	delay := float64(baseDelay) * math.Pow(2, float64(receiveCount-1))
	if delay > float64(maxDelay) {
		delay = float64(maxDelay)
	}
	return int32(delay)
}

func changeVisibility(ctx context.Context, sqsClient *sqs.Client, queueURL string, msg types.Message, delaySec int32) {
	_, err := sqsClient.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          &queueURL,
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: delaySec,
	})
	if err != nil {
		slog.Error("failed to change message visibility", "message_id", *msg.MessageId, "delay_seconds", delaySec, "error", err)
	}
}
