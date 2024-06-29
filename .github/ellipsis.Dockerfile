FROM golang:1.22-bookworm

WORKDIR /go/src/app

# Copy the go module files and download the dependencies
# We do this before copying the rest of the source code to avoid
# having to re-download the dependencies every time we build the image
COPY /cardinal/go.mod /cardinal/go.sum ./
RUN go mod download

# Copy the rest of the source code and build the binary
COPY . .