BINDIR := bin

.PHONY: build clean seed crawl deploy

build:
	go build -o $(BINDIR)/crawler ./cmd/crawler
	go build -o $(BINDIR)/seed ./cmd/seed

seed: build
	./$(BINDIR)/seed

crawl: build
	./$(BINDIR)/crawler

deploy:
	aws cloudformation deploy \
		--template-file infra/cloudformation.yaml \
		--stack-name orbweaver \
		--parameter-overrides Environment=$(or $(ENV),dev)

clean:
	rm -rf $(BINDIR)/*
