package s3Compat

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Params struct {
	REGION     string
	BUCKET     string
	ACCESS_KEY string
	SECRET_KEY string
	ENDPOINT   string
	Service    *s3.S3
}

// Function that takes []byte and uploads it to S3
func (p *Params) PutObject(key string, data []byte) error {
	// Set up the input for the PutObject method.
	input := &s3.PutObjectInput{
		Body:   bytes.NewReader(data),
		Bucket: aws.String(p.BUCKET),
		Key:    aws.String(key),
	}

	// Call S3's PutObject method and handle the error (if any).
	_, err := p.Service.PutObject(input)
	if err != nil {
		return err
	}

	return nil
}

// Function that takes key and downloads it from S3
func (p *Params) GetObject(key string) ([]byte, error) {
	// Set up the input for the GetObject method.
	input := &s3.GetObjectInput{
		Bucket: aws.String(p.BUCKET),
		Key:    aws.String(key),
	}

	// Call S3's GetObject method and handle the error (if any).
	result, err := p.Service.GetObject(input)
	if err != nil {
		return nil, err
	}

	// Read the bytes from S3
	buf := new(bytes.Buffer)
	buf.ReadFrom(result.Body)

	return buf.Bytes(), nil
}

// Take stream data non-seekable stream like StdIn and upload it to S3
// func (p *Params) PutObjectStream(key string, data io.Reader) error {
// 	resp, err := p.Service.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
// 		Bucket: aws.String(p.BUCKET),
// 		Key:    aws.String(key),
// 	})
// 	if err != nil {
// 		return fmt.Errorf("unable to initiate multipart upload: %v", err)
// 	}

// 	var wg sync.WaitGroup

// 	partSize := int64(1024 * 1024 * 5)                   // 5MB, adjust as needed.
// 	completedParts := make([]*s3.CompletedPart, 0, 1024) // adjust as needed

// 	for partNum := 0; ; partNum++ {
// 		partBuffer := make([]byte, partSize)
// 		bytesRead, err := io.ReadFull(data, partBuffer)

// 		if err == io.EOF {
// 			break
// 		} else if err != nil && err != io.ErrUnexpectedEOF {
// 			return fmt.Errorf("unable to read from input data: %v", err)
// 		}

// 		wg.Add(1)
// 		go func(pn int, pb []byte) {
// 			defer wg.Done()
// 			uploadResult, err := p.Service.UploadPart(&s3.UploadPartInput{
// 				Body:       bytes.NewReader(pb[:bytesRead]),
// 				Bucket:     aws.String(p.BUCKET),
// 				Key:        aws.String(key),
// 				PartNumber: aws.Int64(int64(pn + 1)), // part numbers start from 1
// 				UploadId:   resp.UploadId,
// 			})

// 			if err != nil {
// 				fmt.Printf("unable to upload part: %v\n", err)
// 				return
// 			}

// 			completedParts[pn] = &s3.CompletedPart{
// 				ETag:       uploadResult.ETag,
// 				PartNumber: aws.Int64(int64(pn + 1)),
// 			}
// 		}(partNum, partBuffer[:bytesRead])

// 		completedParts = append(completedParts, nil) // placeholder for the completed part
// 	}

// 	wg.Wait()

// 	// Remove any nil elements from completedParts. This should not be necessary if all parts were uploaded successfully.
// 	finalParts := make([]*s3.CompletedPart, 0, len(completedParts))
// 	for _, part := range completedParts {
// 		if part != nil {
// 			finalParts = append(finalParts, part)
// 		}
// 	}

// 	_, err = p.Service.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
// 		Bucket:   aws.String(p.BUCKET),
// 		Key:      aws.String(key),
// 		UploadId: resp.UploadId,
// 		MultipartUpload: &s3.CompletedMultipartUpload{
// 			Parts: finalParts,
// 		},
// 	})

// 	if err != nil {
// 		return fmt.Errorf("unable to complete multipart upload: %v", err)
// 	}

// 	return nil
// }

// Take stream data non-seekable stream like StdIn and upload it to S3
func (p *Params) PutObjectStream(key string, data io.Reader) error {
	resp, err := p.Service.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(p.BUCKET),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("unable to initiate multipart upload: %v", err)
	}

	var wg sync.WaitGroup
	sem := make(chan bool, 10) // Number of concurrent uploads

	partSize := int64(1024 * 1024 * 200)                 // 200MB part size
	completedParts := make([]*s3.CompletedPart, 0, 2048) // Max number of total parts, with 200MB part size and 2048 parts this is 400GB max file size

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
		sem <- true // block if there are already 5 active uploads
		go func(pn int, pb []byte) {
			defer wg.Done()
			defer func() { <-sem }() // release the semaphore when the upload finishes

			uploadResult, err := p.Service.UploadPart(&s3.UploadPartInput{
				Body:       aws.ReadSeekCloser(bytes.NewReader(pb)),
				Bucket:     aws.String(p.BUCKET),
				Key:        aws.String(key),
				PartNumber: aws.Int64(int64(pn + 1)), // part numbers start from 1
				UploadId:   resp.UploadId,
			})

			if err != nil {
				fmt.Printf("unable to upload part: %v\n", err)
				return
			}

			completedParts[pn] = &s3.CompletedPart{
				ETag:       uploadResult.ETag,
				PartNumber: aws.Int64(int64(pn + 1)),
			}
		}(partNum, partBuffer)

		completedParts = append(completedParts, nil) // placeholder for the completed part
	}

	wg.Wait()

	// Remove any nil elements from completedParts. This should not be necessary if all parts were uploaded successfully.
	finalParts := make([]*s3.CompletedPart, 0, len(completedParts))
	for _, part := range completedParts {
		if part != nil {
			finalParts = append(finalParts, part)
		}
	}

	_, err = p.Service.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(p.BUCKET),
		Key:      aws.String(key),
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: finalParts,
		},
	})

	if err != nil {
		return fmt.Errorf("unable to complete multipart upload: %v", err)
	}

	return nil
}

func (p *Params) PutObjectSeekableStream(key string, data io.Reader) error {
	// Create an uploader with the session and default options
	uploader := s3manager.NewUploaderWithClient(p.Service)

	// Perform an upload.
	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(p.BUCKET),
		Key:    aws.String(key),
		Body:   data,
	})

	if err != nil {
		// Print the error and exit.
		return fmt.Errorf("unable to upload %q to %q, %v", key, p.BUCKET, err)
	}

	return nil
}

func (p *Params) GetObjectStreamSimple(key string) (io.ReadCloser, error) {
	// Set up the input for the GetObject method.
	input := &s3.GetObjectInput{
		Bucket: aws.String(p.BUCKET),
		Key:    aws.String(key),
	}

	// Call S3's GetObject method and handle the error (if any).
	result, err := p.Service.GetObject(input)
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

// Parallel
func (p *Params) GetObjectStream(key string) (io.ReadCloser, error) {
	const chunkSize int64 = 500 * 1024 * 1024 // 200MB

	// Get the object size
	headInput := &s3.HeadObjectInput{
		Bucket: aws.String(p.BUCKET),
		Key:    aws.String(key),
	}
	headResult, err := p.Service.HeadObject(headInput)
	if err != nil {
		return nil, err
	}
	objectSize := *headResult.ContentLength

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
				input := &s3.GetObjectInput{
					Bucket: aws.String(p.BUCKET),
					Key:    aws.String(key),
					Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
				}

				// Call S3's GetObject method for this chunk
				result, err := p.Service.GetObject(input)
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

// Function that deletes object from S3
func (p *Params) DeleteObject(key string) error {
	// Set up the input for the DeleteObject method.
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(p.BUCKET),
		Key:    aws.String(key),
	}

	// Call S3's DeleteObject method and handle the error (if any).
	_, err := p.Service.DeleteObject(input)
	if err != nil {
		return err
	}

	return nil
}

// Function that lists all objects in a bucket
func (p *Params) ListObjects() ([]string, error) {
	// Set up the input for the ListObjects method.
	input := &s3.ListObjectsInput{
		Bucket: aws.String(p.BUCKET),
	}

	// Call S3's ListObjects method and handle the error (if any).
	result, err := p.Service.ListObjects(input)
	if err != nil {
		return nil, err
	}

	// Get names of all objects
	var names []string
	for _, object := range result.Contents {
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
		return nil, fmt.Errorf("bucket name is required")
	}

	if params.ACCESS_KEY == "" {
		return nil, fmt.Errorf("access key is required")
	}

	if params.SECRET_KEY == "" {
		return nil, fmt.Errorf("secret key is required")
	}

	if params.ENDPOINT == "" {
		return nil, fmt.Errorf("endpoint is required")
	}

	// Create a session
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(params.REGION),
		Endpoint:    aws.String(params.ENDPOINT),
		Credentials: credentials.NewStaticCredentials(params.ACCESS_KEY, params.SECRET_KEY, ""),
	})
	if err != nil {
		return nil, err
	}

	// Create a new S3 service.
	params.Service = s3.New(sess)

	// Test the connection
	_, err = params.Service.ListBuckets(nil)
	if err != nil {
		return nil, err
	}

	return &params, nil
}
