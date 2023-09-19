package s3Compat

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestNew(t *testing.T) {
	// Read parameters from environment variables
	region := os.Getenv("S3_REGION")
	bucket := os.Getenv("S3_BUCKET")
	accessKey := os.Getenv("S3_ACCESS_KEY")
	secretKey := os.Getenv("S3_SECRET_KEY")
	endpoint := os.Getenv("S3_ENDPOINT")

	// Test case when all parameters are provided
	params := Params{
		REGION:     region,
		BUCKET:     bucket,
		ACCESS_KEY: accessKey,
		SECRET_KEY: secretKey,
		ENDPOINT:   endpoint,
	}

	client, err := New(params)
	if err != nil {
		t.Errorf("Error creating client: %s", err)
		return
	}

	if client == nil {
		t.Errorf("Client is nil")
	}

	testData := []byte("Hello World!")

	// Test creating an object
	err = client.PutObject("test.txt", testData)
	if err != nil {
		t.Errorf("Error putting object: %s", err)
	}

	// Test listing objects
	files, err := client.ListObjects()
	if err != nil {
		t.Errorf("Error listing objects: %s", err)
	}

	if files == nil {
		t.Errorf("Files is nil")
	}

	// Test get object
	data, err := client.GetObject("test.txt")
	if err != nil {
		t.Errorf("Error getting object: %s", err)
	}

	if data == nil {
		t.Errorf("Data is nil")
	}

	if string(data) != string(testData) {
		t.Errorf("Data is not equal to test data")
	}

	// Test stream reader
	reader, err := client.GetObjectStream("test.txt")
	if err != nil {
		t.Errorf("Error in GetObjectStream: %s", err)
		return
	}
	defer reader.Close()

	// Read from the returned reader and check the contents
	// This part is optional and depends on what you want to check
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, reader)
	if err != nil {
		t.Errorf("Error reading from stream: %s", err)
		return
	}

	if buf.String() != string(testData) {
		t.Errorf("Data is not equal to test data")
	}

	// Test simple steam reader
	reader, err = client.GetObjectStreamSimple("test.txt")
	if err != nil {
		t.Errorf("Error in GetObjectStreamSimple: %s", err)
		return
	}
	defer reader.Close()

	// Test stream seekable
	buffer := bytes.NewReader(testData) // create io.Reader from testData

	err = client.PutObjectSeekableStream("test.txt", buffer)
	if err != nil {
		t.Errorf("Error in PutObjectSeekableStream: %s", err)
		return
	}

	// Read from the returned reader and check the contents
	// This part is optional and depends on what you want to check
	buf = new(bytes.Buffer)
	_, err = io.Copy(buf, reader)
	if err != nil {
		t.Errorf("Error reading from stream: %s", err)
		return
	}

	if buf.String() != string(testData) {
		t.Errorf("Data is not equal to test data")
	}

	// Test non-seekable stream
	// Create an io.Reader from the data
	buffer = bytes.NewReader(testData) // create io.Reader from testData

	// Call the PutObjectStream method
	err = client.PutObjectStream("test.txt", buffer)
	if err != nil {
		t.Fatalf("PutObjectStream failed: %v", err)
	}

	// Update metadata
	err = client.SetMetadata("test.txt", map[string]string{"test": "test"})
	if err != nil {
		t.Errorf("Error updating metadata: %s", err)
	}

	// Get metadata
	metadata, err := client.GetMetadata("test.txt")
	if err != nil {
		t.Errorf("Error getting metadata: %s", err)
	}

	if metadata == nil {
		t.Errorf("Metadata is nil")
	}

	// Print metadata
	for k, v := range metadata {
		t.Logf("Key: %s, Value: %s", k, v)
	}

	// Test delete object
	err = client.DeleteObject("test.txt")
	if err != nil {
		t.Errorf("Error deleting object: %s", err)
	}

}
