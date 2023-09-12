
# s3Compat: An S3 Compatibility Library for Go

## Overview

`s3Compat` is a Go package that simplifies interactions with Amazon Simple Storage Service (S3) or S3-compatible storage services. The package provides an easy-to-use and straightforward API for various S3 operations such as uploading, downloading, and deleting objects.

## Features

- Upload objects to S3 using byte arrays, streams, or seekable streams.
- Download objects from S3 as byte arrays or streams.
- Delete objects from S3.
- List objects in a bucket.
- Support for custom S3-compatible services by specifying the endpoint.

## Installation

To install `s3Compat`, run the following command:

```bash
go get -u github.com/8ff/s3Compat
```

## Usage

### Importing

First, import the package into your Go application:

```go
import "github.com/8ff/s3Compat"
```

### Initialize

Create a new S3 Params struct:

```go
client, err := s3Compat.New(s3Compat.Params{
    REGION:     "us-west-2",
    BUCKET:     "my-bucket",
    ACCESS_KEY: "your-access-key",
    SECRET_KEY: "your-secret-key",
    ENDPOINT:   "https://s3.amazonaws.com",
})
```

### Uploading an Object

Upload a byte array to S3:

```go
err := client.PutObject("my-object-key", []byte("Hello, world!"))
```

### Downloading an Object

Download an object from S3:

```go
data, err := client.GetObject("my-object-key")
```

### Deleting an Object

Delete an object from S3:

```go
err := client.DeleteObject("my-object-key")
```

### Listing Objects in a Bucket

List objects in the bucket:

```go
objectNames, err := client.ListObjects()
```

## Example

Here's a simple example that uploads and then downloads a text file:

```go
package main

import (
    "github.com/8ff/s3Compat"
)

func main() {
    client, _ := s3Compat.New(s3Compat.Params{
        REGION:     "us-west-2",
        BUCKET:     "my-bucket",
        ACCESS_KEY: "your-access-key",
        SECRET_KEY: "your-secret-key",
        ENDPOINT:   "https://s3.amazonaws.com",
    })

    // Upload
    _ = client.PutObject("test-file", []byte("Hello, world!"))

    // Download
    data, _ := client.GetObject("test-file")
    println(string(data))
}
```

## Examples with Stream and Pipe Operations

The following examples demonstrate how to use `s3Compat` for stream and pipe operations.

### Example: Upload an Object from Stdin (`pipeObject`)

This example reads from the standard input and uploads the data to S3.

```go
func pipeObject(access_key string, secret string, endpoint string, args []string) {
    // (Omitted: Error checks and argument validation)

    api, err := s3Compat.New(s3Compat.Params{BUCKET: bucket, ACCESS_KEY: access_key, SECRET_KEY: secret, ENDPOINT: endpoint})
    if err != nil {
        // Handle error
    }

    err = api.PutObjectStream(file, os.Stdin)
    if err != nil {
        // Handle error
    }
}
```

### Example: Download an Object to Stdout (`catObject`)

This example downloads an object from S3 and writes it to the standard output.

```go
func catObject(access_key string, secret string, endpoint string, args []string) {
    // (Omitted: Error checks and argument validation)

    api, err := s3Compat.New(s3Compat.Params{BUCKET: bucket, ACCESS_KEY: access_key, SECRET_KEY: secret, ENDPOINT: endpoint})
    if err != nil {
        // Handle error
    }

    readCloser, err := api.GetObjectStream(file)
    if err != nil {
        // Handle error
    }
    defer readCloser.Close()

    _, err = io.Copy(os.Stdout, readCloser)
    if err != nil {
        // Handle error
    }
}
```

### Example: Delete an Object (`rmObject`)

This example deletes an object from S3.

```go
func rmObject(access_key string, secret string, endpoint string, args []string) {
    // (Omitted: Error checks and argument validation)

    api, err := s3Compat.New(s3Compat.Params{BUCKET: bucket, ACCESS_KEY: access_key, SECRET_KEY: secret, ENDPOINT: endpoint})
    if err != nil {
        // Handle error
    }

    err = api.DeleteObject(file)
    if err != nil {
        // Handle error
    }
}
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.