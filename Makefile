BINDIR := bin
AWS_ACCOUNT_ID := $(shell aws sts get-caller-identity --query Account --output text)
AWS_REGION := $(or $(AWS_REGION),us-east-1)
ECR_URI := $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com

.PHONY: build clean seed crawler parser deploy ecr-login docker-build docker-push

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

ecr-login:
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(ECR_URI)

docker-build:
	docker build -f docker/crawler.Dockerfile -t orbweaver-crawler .
	docker build -f docker/parser.Dockerfile -t orbweaver-parser .

docker-push: docker-build
	docker tag orbweaver-crawler:latest $(ECR_URI)/orbweaver-crawler:latest
	docker tag orbweaver-parser:latest $(ECR_URI)/orbweaver-parser:latest
	docker push $(ECR_URI)/orbweaver-crawler:latest
	docker push $(ECR_URI)/orbweaver-parser:latest

clean:
	rm -rf $(BINDIR)/*
