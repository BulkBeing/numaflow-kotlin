IMAGE_NAME ?= simple-sink
TAG ?= latest
ROOT_DIR := $(shell git rev-parse --show-toplevel)

.PHONY: image
image:
	docker build -t $(IMAGE_NAME):$(TAG) -f $(ROOT_DIR)/examples/sinker/simple-sink/Dockerfile $(ROOT_DIR)
