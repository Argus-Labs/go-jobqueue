# go-jobqueue

[![Codecov](https://codecov.io/gh/Argus-Labs/go-jobqueue/graph/badge.svg?token=zvfS7xcv50)](https://codecov.io/gh/Argus-Labs/go-jobqueue)
[![Go Report Card](https://goreportcard.com/badge/github.com/argus-labs/go-jobqueue)](https://goreportcard.com/report/github.com/argus-labs/go-jobqueue)
[![Go Reference](https://pkg.go.dev/badge/github.com/argus-labs/go-jobqueue.svg)](https://pkg.go.dev/github.com/argus-labs/go-jobqueue)

<br />

A lightweight, durable, and embedded job queue for Go applications. Powered by [BadgerDB](https://github.com/dgraph-io/badger).

**Warning: this package is a work in progress, use at your own risk.**

## Features
- Portable alternative to full-fledged message brokers (i.e. RabbitMQ).
- Built on top of [BadgerDB](https://github.com/dgraph-io/badger) for durability
- Automatic job processing with support for multiple concurrent workers.
- Strong type safety using generics.

## Installation

```bash
go get github.com/argus-labs/go-jobqueue
```
