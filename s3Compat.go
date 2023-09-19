package s3Compat

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Params struct {
	REGION     string
	BUCKET     string
	ACCESS_KEY string
	SECRET_KEY string
	ENDPOINT   string
	Service    *s3.Client
}

// Take stream data non-seekable stream like StdIn and upload it to S3
func (p *Params) PutObjectStream(key string, data io.Reader) error {
	ctx := context.TODO() // Replace with your context if necessary

	resp, err := p.Service.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: &p.BUCKET,
		Key:    &key,
	})
	if err != nil {
		return fmt.Errorf("unable to initiate multipart upload: %v", err)
	}

	var wg sync.WaitGroup
	sem := make(chan bool, 10) // Number of concurrent uploads

	partSize := int64(1024 * 1024 * 200)                    // 200MB part size
	completedParts := make([]*types.CompletedPart, 0, 2048) // Max number of total parts

	for partNum := 0; ; partNum++ {
		partBuffer := make([]byte, partSize)
		bytesRead, err := io.ReadFull(data, partBuffer)

		if err == io.EOF {
			break
		} else if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("unable to read from input data: %v", err)
		}

		partBuffer = partBuffer[:bytesRead] // Adjust buffer size to actual bytes read

		wg.Add(1)
		sem <- true // block if there are already 10 active uploads
		go func(pn int, pb []byte) {
			defer wg.Done()
			defer func() { <-sem }() // release the semaphore when the upload finishes

			uploadResult, err := p.Service.UploadPart(ctx, &s3.UploadPartInput{
				Body:       bytes.NewReader(pb),
				Bucket:     &p.BUCKET,
				Key:        &key,
				PartNumber: int32(pn + 1), // part numbers start from 1
				UploadId:   resp.UploadId,
			})

			if err != nil {
				fmt.Printf("unable to upload part: %v\n", err)
				return
			}

			completedParts[pn] = &types.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: int32(pn + 1),
			}
		}(partNum, partBuffer)

		completedParts = append(completedParts, nil) // placeholder for the completed part
	}

	wg.Wait()

	// Remove any nil elements from completedParts. This should not be necessary if all parts were uploaded successfully.
	finalParts := make([]types.CompletedPart, 0, len(completedParts))
	for _, part := range completedParts {
		if part != nil {
			finalParts = append(finalParts, *part)
		}
	}

	_, err = p.Service.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &p.BUCKET,
		Key:      &key,
		UploadId: resp.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: finalParts,
		},
	})

	if err != nil {
		return fmt.Errorf("unable to complete multipart upload: %v", err)
	}

	return nil
}

// Function to put object with a seekable stream
func (p *Params) PutObjectSeekableStream(key string, data io.Reader) error {
	// Create a context
	ctx := context.TODO() // Replace with your context if necessary

	// Create an uploader with the client and default options
	uploader := manager.NewUploader(p.Service)

	// Perform an upload.
	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &p.BUCKET,
		Key:    &key,
		Body:   data,
	})

	if err != nil {
		// Print the error and exit.
		return fmt.Errorf("unable to upload %q to %q, %v", key, p.BUCKET, err)
	}

	return nil
}

// Function to get object stream in a simple way
func (p *Params) GetObjectStreamSimple(key string) (io.ReadCloser, error) {
	// Set up the input for the GetObject method.
	input := &s3.GetObjectInput{
		Bucket: &p.BUCKET,
		Key:    &key,
	}

	// Create a context
	ctx := context.TODO() // Replace with your context if necessary

	// Call S3's GetObject method and handle the error (if any).
	result, err := p.Service.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

// Parallel
func (p *Params) GetObjectStream(key string) (io.ReadCloser, error) {
	const chunkSize int64 = 500 * 1024 * 1024 // 500MB

	// Create a context
	ctx := context.TODO() // Replace with your context if necessary

	// Get the object size
	headInput := &s3.HeadObjectInput{
		Bucket: &p.BUCKET,
		Key:    &key,
	}
	headResult, err := p.Service.HeadObject(ctx, headInput)
	if err != nil {
		return nil, err
	}
	objectSize := headResult.ContentLength

	// Calculate the number of chunks
	numChunks := int(objectSize / chunkSize)
	if objectSize%chunkSize != 0 {
		numChunks++
	}

	// Buffer to hold chunks with range metadata
	type Chunk struct {
		RangeStart int64
		Data       *bytes.Buffer
	}
	buffer := make([]*Chunk, numChunks)
	var bufferMutex sync.Mutex

	// Wait group to wait for all download workers
	var wg sync.WaitGroup

	// Launch 3 download workers
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for chunkIndex := worker; chunkIndex < numChunks; chunkIndex += 3 {
				start := int64(chunkIndex) * chunkSize
				end := start + chunkSize - 1
				if chunkIndex == numChunks-1 {
					end = objectSize - 1
				}

				// Create the GetObjectInput with the Range parameter
				rangeStr := fmt.Sprintf("bytes=%d-%d", start, end)
				input := &s3.GetObjectInput{
					Bucket: &p.BUCKET,
					Key:    &key,
					Range:  &rangeStr, // Take the address of rangeStr
				}

				// Call S3's GetObject method for this chunk
				result, err := p.Service.GetObject(ctx, input)
				if err != nil {
					// Handle error
					return
				}
				defer result.Body.Close()

				// Read the chunk into a buffer
				buf := new(bytes.Buffer)
				_, err = io.Copy(buf, result.Body)
				if err != nil {
					// Handle error
					return
				}

				chunk := &Chunk{
					RangeStart: start,
					Data:       buf,
				}

				// Store the chunk in the buffer with proper synchronization
				bufferMutex.Lock()
				buffer[chunkIndex] = chunk
				bufferMutex.Unlock()
			}
		}(worker)
	}

	// Custom reader to read from buffer in the correct order
	reader, writer := io.Pipe()
	go func() {
		wg.Wait() // Wait for all download workers to finish

		// Write chunks to writer in the correct order
		for _, chunk := range buffer {
			_, err := writer.Write(chunk.Data.Bytes())
			if err != nil {
				// Handle error
				return
			}
		}
		writer.Close()
	}()

	return reader, nil
}

// Function that takes []byte and uploads it to S3
func (p *Params) PutObject(key string, data []byte) error {
	// Set up the input for the PutObject method.
	input := &s3.PutObjectInput{
		Body:   bytes.NewReader(data),
		Bucket: &p.BUCKET,
		Key:    &key,
	}

	// Create a context
	ctx := context.TODO() // Replace with your context if necessary

	// Call S3's PutObject method and handle the error (if any).
	_, err := p.Service.PutObject(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

// Function that takes key and metadata and updates metadata for that key
func (p *Params) SetMetadata(key string, metadata map[string]string) error {
	// Create a context
	ctx := context.TODO() // Replace with your context if necessary

	// Prepare the CopyObjectInput
	input := &s3.CopyObjectInput{
		Bucket:            &p.BUCKET,
		CopySource:        aws.String(fmt.Sprintf("%s/%s", p.BUCKET, key)),
		Key:               &key,
		Metadata:          metadata,
		MetadataDirective: types.MetadataDirectiveReplace,
	}

	// Perform the CopyObject operation to update metadata
	_, err := p.Service.CopyObject(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

// Function that takes key and returns metadata for that key
func (p *Params) GetMetadata(key string) (map[string]string, error) {
	// Create a context
	ctx := context.TODO() // Replace with your context if necessary

	// Prepare the HeadObjectInput
	input := &s3.HeadObjectInput{
		Bucket: &p.BUCKET,
		Key:    &key,
	}

	// Call S3's HeadObject method
	output, err := p.Service.HeadObject(ctx, input)
	if err != nil {
		return nil, err
	}

	// Extract metadata from the output
	metadata := output.Metadata

	return metadata, nil
}

// Function that takes key and downloads it from S3
func (p *Params) GetObject(key string) ([]byte, error) {
	// Set up the input for the GetObject method.
	input := &s3.GetObjectInput{
		Bucket: &p.BUCKET,
		Key:    &key,
	}

	// Create a context
	ctx := context.TODO() // Replace with your context if necessary

	// Call S3's GetObject method and handle the error (if any).
	result, err := p.Service.GetObject(ctx, input)
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	// Read the bytes from S3
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, result.Body)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Function that deletes object from S3
func (p *Params) DeleteObject(key string) error {
	// Set up the input for the DeleteObject method.
	input := &s3.DeleteObjectInput{
		Bucket: &p.BUCKET,
		Key:    &key,
	}

	// Create a context
	ctx := context.TODO() // Replace with your context if necessary

	// Call S3's DeleteObject method and handle the error (if any).
	_, err := p.Service.DeleteObject(ctx, input)
	if err != nil {
		return err
	}

	return nil
}

// Function that lists all objects in a bucket
func (p *Params) ListObjects() ([]string, error) {
	listObjectsOutput, err := p.Service.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &p.BUCKET,
	})
	if err != nil {
		return nil, err
	}

	// Get names of all objects
	var names []string
	for _, object := range listObjectsOutput.Contents {
		names = append(names, *object.Key)
	}
	return names, nil
}

func New(params Params) (*Params, error) {
	// Go over all params and if unset set them to defaults
	if params.REGION == "" {
		params.REGION = "auto"
	}

	if params.BUCKET == "" {
		return nil, errors.New("bucket name is required")
	}

	if params.ACCESS_KEY == "" {
		return nil, errors.New("access key is required")
	}

	if params.SECRET_KEY == "" {
		return nil, errors.New("secret key is required")
	}

	if params.ENDPOINT == "" {
		return nil, errors.New("endpoint is required")
	}
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:           params.ENDPOINT,
			SigningRegion: params.REGION,
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(params.ACCESS_KEY, params.SECRET_KEY, "")),
	)
	cfg.Region = params.REGION
	if err != nil {
		return nil, err
	}

	// Create a new S3 service.
	params.Service = s3.NewFromConfig(cfg)
	return &params, nil
}
