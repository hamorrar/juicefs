//go:build !nos3
// +build !nos3

/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package object

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/sethvargo/go-retry"
	"storj.io/common/rpc/rpcpool"
	"storj.io/uplink"
	"storj.io/uplink/private/transport"
)

/*
NOTES FOR HILAL
TODO: Hilal delete this but for now here is example code
https://github.com/storj/uplink/blob/main/examples/walkthrough/main.go
this entire repo has the API we can use for uplink

I'm just making a guess what is needed in StorjClient so if you need to add some do it

Use object_storage_test TestStorj function

1) work towards running test function (setting up IDE)
2) work on put
3) work on delete
4) .... down the TestStorj function
*/

const maxPartCount int = math.MaxInt32 - 1 // Storj source code shows max part count is this
const minPartSize int = 0                  // No notes on this

type StorjClient struct {
	bucket  string
	project *uplink.Project
}

func (s *StorjClient) String() string {
	return fmt.Sprintf("sj://%s/", s.bucket)
}

func (s *StorjClient) Limits() Limits {
	return Limits{
		IsSupportMultipartUpload: true,
		IsSupportUploadPartCopy:  false,
		MinPartSize:              minPartSize,
		MaxPartSize:              64000000, // 64MB per https://forum.storj.io/t/uplink-library-upload-process-and-recommended-file-fragmentation-size/10839/8
		MaxPartCount:             maxPartCount,
	}
}

func (s *StorjClient) Create() error {
	_, err := s.project.EnsureBucket(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("could not create bucket: %v", err)
	}

	return nil
}

func (s *StorjClient) Get(key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	// Initiate a download of the same object again
	download, err := s.project.DownloadObject(ctx, s.bucket, key, &uplink.DownloadOptions{Length: limit, Offset: off})
	if err != nil {
		return nil, fmt.Errorf("could not open object: %v", err)
	}

	return download, nil

}

func (s *StorjClient) Put(key string, in io.Reader, getters ...AttrGetter) error {
	var upload *uplink.Upload

	backoff := retry.NewExponential(1 * time.Second)
	backoff = retry.WithMaxRetries(3, backoff)

	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		var innerErr error
		upload, innerErr = s.project.UploadObject(ctx, s.bucket, key, &uplink.UploadOptions{})

		// If there are too many requests, try again
		if innerErr != nil && errors.Is(innerErr, uplink.ErrTooManyRequests) {
			return retry.RetryableError(innerErr)
		}

		return innerErr
	})

	if err != nil {
		return fmt.Errorf("could not initiate upload: %v", err)
	}

	_, err = io.Copy(upload, in)

	if err != nil {
		_ = upload.Abort()
		return fmt.Errorf("could not upload data: %v", err)
	}

	err = retry.Do(ctx, backoff, func(ctx context.Context) error {
		innerErr := upload.Commit()

		// If there are too many requests, try again
		if innerErr != nil && errors.Is(innerErr, uplink.ErrTooManyRequests) {
			return retry.RetryableError(innerErr)
			// Sometimes it seems to complete a request but doesn't tell us until we try to commit again
			// It is weird, but we will just return no error if this happens
		} else if innerErr != nil && strings.HasSuffix(innerErr.Error(), "already committed") {
			return nil
		}

		return innerErr
	})

	if err != nil {
		return fmt.Errorf("could not commit uploaded object: %v", err)
	}

	return nil
}

func (s *StorjClient) Copy(dst, src string) error {
	// TODO implement me
	// TODO: Hilal Copy an object from src to dst.
	panic("implement me")
}

func (s *StorjClient) Delete(key string, getters ...AttrGetter) error {
	_, err := s.project.DeleteObject(ctx, s.bucket, key)

	return err
}

func (s *StorjClient) Head(key string) (Object, error) {
	// TODO implement me
	// TODO: Hilal Head returns some information about the object or an error if not found
	download, err := s.project.DownloadObject(ctx, s.bucket, key, &uplink.DownloadOptions{Length: -1, Offset: -1})
	objinfo := download.Info()
	// return &obj{key: key, size: }, err
	// panic("implement me")
	return &obj{key: key, size: 0, mtime: objinfo.System.Created, isDir: objinfo.IsPrefix, sc: ""}, err
}

// Storj prefix only will take a folder path so we need to split it into the folder path and if it has a file path
type PrefixSpecifics struct {
	FullPrefix    string
	Folder        string
	HasFilePrefix bool
}

func setPrefixSpecifics(prefixSplit *PrefixSpecifics, prefix string) {
	prefixSplit.FullPrefix = prefix

	if strings.HasSuffix(prefix, "/") {
		prefixSplit.Folder = prefix
		prefixSplit.HasFilePrefix = false
		return
	}

	lastIndex := strings.LastIndex(prefix, "/")

	if lastIndex == -1 {
		prefixSplit.Folder = ""
		prefixSplit.HasFilePrefix = true
		return
	}

	prefixSplit.Folder = prefix[:lastIndex+1]
	prefixSplit.HasFilePrefix = true
}

func (s *StorjClient) List(prefix, marker, delimiter string, limit int64, followLink bool) ([]Object, error) {
	if delimiter != "/" {
		// Right now we only support the "/" delimiter
		return nil, notSupported
	}

	var prefixSpecifics PrefixSpecifics
	setPrefixSpecifics(&prefixSpecifics, prefix)

	// Storj prefix only accepts a folder path. If we want to include the prefix to filter down the objects themselves,
	// we need to do it in the below for loop
	objs := s.project.ListObjects(ctx, s.bucket, &uplink.ListObjectsOptions{Prefix: prefixSpecifics.Folder})

	l := make([]Object, 0)

	for objs.Next() {
		thing := objs.Item()
		// If it has a file prefix but the item doesn't have that prefix, skip it
		if prefixSpecifics.HasFilePrefix && !strings.HasPrefix(thing.Key, prefixSpecifics.FullPrefix) {
			continue
		}

		l = append(l, &obj{
			key:   thing.Key,
			isDir: thing.IsPrefix,
			sc:    "",
			mtime: thing.System.Created,
			size:  thing.System.ContentLength,
		})
	}

	// Expected alphabetical order
	sort.Slice(l, func(i, j int) bool {
		return l[i].Key() < l[j].Key()
	})

	return l, nil

}

func (s *StorjClient) ListAll(prefix, marker string, followLink bool) (<-chan Object, error) {
	// TODO implement me
	// TODO: Hilal returns all the objects as an channel.
	// panic("implement me")
	// return nil, errors.New("not supported")
	return nil, notSupported
}

func (s *StorjClient) CreateMultipartUpload(key string) (*MultipartUpload, error) {
	// We aren't supporting setting an expiration or custom metadata so pass in nil upload options
	uploadInfo, err := s.project.BeginUpload(ctx, s.bucket, key, nil)

	if err != nil {
		return nil, fmt.Errorf("unable to create multipart upload: %v", err)
	}

	return &MultipartUpload{MinPartSize: minPartSize, MaxCount: maxPartCount, UploadID: uploadInfo.UploadID}, nil
}

func (s *StorjClient) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	partUpload, err := s.project.UploadPart(ctx, s.bucket, key, uploadID, uint32(num))

	if err != nil {
		return nil, fmt.Errorf("invalid upload part: %v", err)
	}

	numBytes, err := partUpload.Write(body)

	if err != nil {
		return nil, fmt.Errorf("invalid write to part: %v", err)
	}

	err = partUpload.Commit()

	if err != nil {
		return nil, fmt.Errorf("invalid commit of part: %v", err)
	}

	return &Part{Num: num, Size: numBytes, ETag: string(partUpload.Info().ETag[:])}, nil
}

func (s *StorjClient) UploadPartCopy(_ string, _ string, _ int, _ string, _, _ int64) (*Part, error) {
	return nil, notSupported
}

func (s *StorjClient) AbortUpload(key string, uploadID string) {
	_ = s.project.AbortUpload(ctx, s.bucket, key, uploadID)
}

func (s *StorjClient) CompleteUpload(key string, uploadID string, parts []*Part) error {
	// Just commit the upload itself since the parts were already committed
	_, err := s.project.CommitUpload(ctx, s.bucket, key, uploadID, nil)

	if err != nil {
		return fmt.Errorf("issue commiting multipart upload: %v", err)
	}

	return nil
}

func (s *StorjClient) ListUploads(marker string) ([]*PendingPart, string, error) {
	parts := make([]*PendingPart, 0)

	iterator := s.project.ListUploads(ctx, s.bucket, nil)

	for iterator.Next() {
		item := iterator.Item()
		parts = append(parts, &PendingPart{Key: item.Key, UploadID: item.UploadID, Created: item.System.Created})
	}

	return parts, "", nil
}

func newStorj(bucket, accessGrant, _, _ string) (ObjectStorage, error) {
	// Parse access grant, which contains necessary credentials and permissions.
	access, err := uplink.ParseAccess(accessGrant)
	if err != nil {
		return nil, fmt.Errorf("invalid access grant: %v", err)
	}

	uplinkConfig := uplink.Config{}

	// TODO: Hannah how do we allow users to configure the capacity?
	pool := rpcpool.New(rpcpool.Options{
		Capacity:       100,
		KeyCapacity:    5,
		IdleExpiration: 2 * time.Minute,
	})

	err = transport.SetConnectionPool(ctx, &uplinkConfig, pool)

	if err != nil {
		return nil, fmt.Errorf("invalid access grant: %v", err)
	}

	project, err := uplinkConfig.OpenProject(ctx, access)
	if err != nil {
		return nil, fmt.Errorf("invalid project: %v", err)
	}

	return &StorjClient{bucket: bucket, project: project}, nil
}

func init() {
	Register("storj", newStorj)
}
