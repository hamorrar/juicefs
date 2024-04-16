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
	"bytes"
	"context"
	"fmt"
	"io"

	"storj.io/uplink"
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
type StorjClient struct {
	bucket  string
	project *uplink.Project
}

func (s *StorjClient) String() string {
	//TODO implement me
	panic("implement me")
}

func (s *StorjClient) Limits() Limits {
	//return Limits{
	//	IsSupportMultipartUpload:	true,
	//	IsSupportUploadPartCopy:	,
	//	MinPartSize:				,
	//	MaxPartSize:				,
	//	MaxPartCount:				,
	//}
	//TODO implement me
	panic("implement me")
}

func (s *StorjClient) Create() error {
	_, err := s.project.EnsureBucket(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("could not create bucket: %v", err)
	}

	return nil
}

func (s *StorjClient) Get(key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	// TODO implement me
	// TODO: Hilal Get the data for the given object specified by key.
	panic("implement me")
}

func (s *StorjClient) Put(key string, in io.Reader, getters ...AttrGetter) error {
	// TODO implement me
	// TODO: Hilal Put data read from a reader to an object specified by key.
	// panic("implement me")

	upload, err := s.project.UploadObject(ctx, s.bucket, key, &uplink.UploadOptions{})
	if err != nil {
		return fmt.Errorf("could not initiate upload: %v", err)
	}

	// Copy the data to the upload.
	var body io.ReadSeeker
	if b, ok := in.(io.ReadSeeker); ok {
		body = b
	} else {
		data, err := io.ReadAll(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	_, err = io.Copy(upload, body)

	if err != nil {
		_ = upload.Abort()
		return fmt.Errorf("could not upload data: %v", err)
	}

	// Commit the uploaded object.
	err = upload.Commit()
	if err != nil {
		return fmt.Errorf("could not commit uploaded object: %v", err)
	}

	return nil

}

func (s *StorjClient) Copy(dst, src string) error {
	// TODO implement me
	panic("implement me")
}

func (s *StorjClient) Delete(key string, getters ...AttrGetter) error {
	// TODO implement me
	// TODO: Hilal Delete a object.
	// panic("implement me")

	_, err := s.project.DeleteObject(ctx, s.bucket, key)

	return err
}

func (s *StorjClient) Head(key string) (Object, error) {
	// TODO implement me
	// TODO: Hilal Head returns some information about the object or an error if not found
	panic("implement me")
}

func (s *StorjClient) List(prefix, marker, delimiter string, limit int64, followLink bool) ([]Object, error) {
	// TODO implement me
	// TODO: Hilal List returns a list of objects.
	// panic("implement me")

	objs := s.project.ListObjects(ctx, s.bucket, &uplink.ListObjectsOptions{Prefix: prefix})

	l := make([]Object, 0)

	for objs.Next() {
		// &obj{prefix, 0, time.Now(), true, ""}
		thing := objs.Item()
		l = append(l, &obj{key: thing.Key, isDir: thing.IsPrefix, sc: "", mtime: thing.System.Created, size: thing.System.ContentLength})
	}

	return l, nil

}

func (s *StorjClient) ListAll(prefix, marker string, followLink bool) (<-chan Object, error) {
	// TODO implement me
	// TODO: Hilal returns all the objects as an channel.
	panic("implement me")
}

func (s *StorjClient) CreateMultipartUpload(key string) (*MultipartUpload, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StorjClient) UploadPart(key string, uploadID string, num int, body []byte) (*Part, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StorjClient) UploadPartCopy(key string, uploadID string, num int, srcKey string, off, size int64) (*Part, error) {
	//TODO implement me
	panic("implement me")
}

func (s *StorjClient) AbortUpload(key string, uploadID string) {
	//TODO implement me
	panic("implement me")
}

func (s *StorjClient) CompleteUpload(key string, uploadID string, parts []*Part) error {
	//TODO implement me
	panic("implement me")
}

func (s *StorjClient) ListUploads(marker string) ([]*PendingPart, string, error) {
	//TODO implement me
	panic("implement me")
}

func newStorj(bucket, accessGrant, _, _ string) (ObjectStorage, error) {
	// TODO
	// Parse access grant, which contains necessary credentials and permissions.
	access, err := uplink.ParseAccess(accessGrant)
	if err != nil {
		return nil, fmt.Errorf("invalid access grant: %v", err)
	}

	ctx := context.Background()
	project, err := uplink.OpenProject(ctx, access)
	if err != nil {
		return nil, fmt.Errorf("invalid project: %v", err)
	}

	// TODO anything else?

	return &StorjClient{bucket: bucket, project: project}, nil
}

func init() {
	Register("storj", newStorj)
}
