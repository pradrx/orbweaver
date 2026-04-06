BINDIR := bin

.PHONY: build clean seed crawler parser deploy

build:
	go build -o $(BINDIR)/crawler ./cmd/crawler
	go build -o $(BINDIR)/parser ./cmd/parser
	go build -o $(BINDIR)/seed ./cmd/seed

seed: build
	./$(BINDIR)/seed

crawler: build
	./$(BINDIR)/crawler

parser: build
	./$(BINDIR)/parser

deploy:
	aws cloudformation deploy \
		--template-file infra/cloudformation.yaml \
		--stack-name orbweaver \
		--parameter-overrides Environment=$(or $(ENV),dev)

clean:
	rm -rf $(BINDIR)/*
