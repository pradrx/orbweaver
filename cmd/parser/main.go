package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
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
	"golang.org/x/net/html"
)

func main() {
	verbose := flag.Bool("verbose", false, "enable debug logging")
	flag.Parse()

	queueURL := os.Getenv("PARSER_QUEUE_URL")
	if queueURL == "" {
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

	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
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

	slog.Info("parser starting", "queue_url", queueURL, "s3_bucket", s3Bucket, "workers", workerCount)
	run(ctx, sqsClient, s3Client, queueURL, s3Bucket, workerCount)
	slog.Info("parser shut down")
}

func run(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, queueURL, s3Bucket string, workerCount int) {
	var wg sync.WaitGroup
	for i := range workerCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("worker started", "worker_id", i)
			poll(ctx, sqsClient, s3Client, queueURL, s3Bucket, i)
			slog.Info("worker stopped", "worker_id", i)
		}()
	}
	wg.Wait()
}

func poll(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, queueURL, s3Bucket string, workerID int) {
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
			processMessage(ctx, sqsClient, s3Client, queueURL, s3Bucket, msg, workerID)
		}
	}
}

func processMessage(ctx context.Context, sqsClient *sqs.Client, s3Client *s3.Client, queueURL, s3Bucket string, msg types.Message, workerID int) {
	var payload struct {
		URL   string `json:"url"`
		S3Key string `json:"s3_key"`
	}
	if err := json.Unmarshal([]byte(*msg.Body), &payload); err != nil {
		slog.Error("failed to unmarshal message", "message_id", *msg.MessageId, "error", err)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	if payload.URL == "" || payload.S3Key == "" {
		slog.Warn("skipping message with empty fields", "message_id", *msg.MessageId, "url", payload.URL, "s3_key", payload.S3Key)
		deleteMessage(ctx, sqsClient, queueURL, msg)
		return
	}

	slog.Info("processing", "url", payload.URL, "s3_key", payload.S3Key, "message_id", *msg.MessageId, "worker_id", workerID)

	obj, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s3Bucket,
		Key:    &payload.S3Key,
	})
	if err != nil {
		slog.Error("s3 download failed", "url", payload.URL, "s3_key", payload.S3Key, "error", err)
		return
	}
	defer obj.Body.Close()

	htmlBytes, err := io.ReadAll(obj.Body)
	if err != nil {
		slog.Error("failed to read s3 object", "url", payload.URL, "s3_key", payload.S3Key, "error", err)
		return
	}

	text := extractText(bytes.NewReader(htmlBytes))
	links := extractLinks(bytes.NewReader(htmlBytes), payload.URL)

	slog.Debug("extracted text", "url", payload.URL, "text_length", len(text), "text", text)
	slog.Info("extracted links", "url", payload.URL, "link_count", len(links), "links", links)

	ds := time.Now().UTC().Format("2006-01-02")
	hash := sha256.Sum256([]byte(payload.URL))
	parsedKey := "parsed/ds=" + ds + "/" + hex.EncodeToString(hash[:]) + ".json"

	parsedDoc, err := json.Marshal(struct {
		URL       string `json:"url"`
		Text      string `json:"text"`
		SourceKey string `json:"source_key"`
		ParsedAt  string `json:"parsed_at"`
	}{
		URL:       payload.URL,
		Text:      text,
		SourceKey: payload.S3Key,
		ParsedAt:  time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		slog.Error("failed to marshal parsed document", "url", payload.URL, "error", err)
		return
	}

	parsedContentType := "application/json"
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &s3Bucket,
		Key:         &parsedKey,
		Body:        bytes.NewReader(parsedDoc),
		ContentType: &parsedContentType,
	})
	if err != nil {
		slog.Error("s3 upload of parsed text failed", "url", payload.URL, "s3_key", parsedKey, "error", err)
		return
	}

	slog.Info("processed", "url", payload.URL, "s3_key", payload.S3Key, "parsed_key", parsedKey, "text_length", len(text), "link_count", len(links), "message_id", *msg.MessageId, "worker_id", workerID)

	deleteMessage(ctx, sqsClient, queueURL, msg)
}

var skipTags = map[string]bool{
	"script":   true,
	"style":    true,
	"noscript": true,
	"nav":      true,
	"header":   true,
	"footer":   true,
	"aside":    true,
	"menu":     true,
	"form":     true,
}

func extractText(body io.Reader) string {
	z := html.NewTokenizer(body)
	var b strings.Builder
	skip := 0

	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			return strings.Join(strings.Fields(b.String()), " ")

		case html.StartTagToken:
			tn, _ := z.TagName()
			if skipTags[string(tn)] {
				skip++
			}

		case html.EndTagToken:
			tn, _ := z.TagName()
			if skipTags[string(tn)] && skip > 0 {
				skip--
			}

		case html.TextToken:
			if skip == 0 {
				b.Write(z.Text())
				b.WriteByte(' ')
			}
		}
	}
}

func extractLinks(body io.Reader, baseURL string) []string {
	base, err := url.Parse(baseURL)
	if err != nil {
		slog.Warn("failed to parse base URL", "url", baseURL, "error", err)
		return nil
	}

	z := html.NewTokenizer(body)
	seen := make(map[string]struct{})
	var links []string

	for {
		tt := z.Next()
		if tt == html.ErrorToken {
			return links
		}

		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}

		tn, hasAttr := z.TagName()
		if string(tn) != "a" || !hasAttr {
			continue
		}

		for {
			key, val, more := z.TagAttr()
			if string(key) == "href" {
				href := strings.TrimSpace(string(val))
				if href == "" {
					break
				}
				parsed, err := url.Parse(href)
				if err != nil {
					break
				}
				resolved := base.ResolveReference(parsed)
				if resolved.Scheme != "http" && resolved.Scheme != "https" {
					break
				}
				link := resolved.String()
				if _, ok := seen[link]; !ok {
					seen[link] = struct{}{}
					links = append(links, link)
				}
				break
			}
			if !more {
				break
			}
		}
	}
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
