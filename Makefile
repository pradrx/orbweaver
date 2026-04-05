BINDIR := bin

.PHONY: build clean seed crawl

build:
	go build -o $(BINDIR)/crawler ./cmd/crawler
	go build -o $(BINDIR)/seed ./cmd/seed

seed: build
	./$(BINDIR)/seed

crawl: build
	./$(BINDIR)/crawler

clean:
	rm -rf $(BINDIR)/*
