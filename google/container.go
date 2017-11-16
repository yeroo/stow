package google

import (
	"io"
	"github.com/graymeta/stow"
	"github.com/pkg/errors"

	"cloud.google.com/go/storage"
	"context"
	"google.golang.org/api/iterator"
	"fmt"
)

type Container struct {
	// Name is needed to retrieve items.
	name string

	// Client is responsible for performing the requests.
	client *storage.Client
}

// ID returns a string value which represents the name of the container.
func (c *Container) ID() string {
	return c.name
}

// Name returns a string value which represents the name of the container.
func (c *Container) Name() string {
	return c.name
}

func (c *Container) Bucket() (*storage.BucketHandle) {
	return c.client.Bucket(c.name)
}

// Item returns a stow.Item instance of a container based on the
// name of the container
func (c *Container) Item(id string) (stow.Item, error) {
	obj := c.client.Bucket(c.name).Object(id)
	res, err := obj.Attrs(context.Background())
	if err != nil {
		return nil, stow.ErrNotFound
	}

	u, err := prepUrl(res.MediaLink)
	if err != nil {
		return nil, err
	}

	mdParsed, err := parseMetadata(res.Metadata)
	if err != nil {
		return nil, err
	}

	i := &Item{
		name:         id,
		container:    c,
		client:       c.client,
		size:         int64(res.Size),
		hash:         fmt.Sprintf("%x", res.MD5),
		lastModified: res.Updated,
		url:          u,
		metadata:     mdParsed,
		object:       obj,
	}

	return i, nil
}

// Items retrieves a list of items that are prepended with
// the prefix argument. The 'cursor' variable facilitates pagination.
func (c *Container) Items(prefix string, cursor string, count int) ([]stow.Item, string, error) {
	// List all objects in a bucket using pagination

	bkt := c.client.Bucket(c.name)
	var q *storage.Query = nil
	if len(prefix) > 0 {
		q = &storage.Query{
			Prefix: prefix,
		}
	}
	iter := bkt.Objects(context.Background(), q)
	pager := iterator.NewPager(iter, count, cursor)
	objs := make([]*storage.ObjectHandle, 0)

	nextToken, err := pager.NextPage(objs)
	if err != nil {
		return nil, "", err
	}
	containerItems := make([]stow.Item, 0, len(objs))

	for _, o := range objs {
		oAttrs, err := o.Attrs(context.Background())
		u, err := prepUrl(oAttrs.MediaLink)
		if err != nil {
			return nil, "", err
		}

		mdParsed, err := parseMetadata(oAttrs.Metadata)
		if err != nil {
			return nil, "", err
		}

		containerItems = append(containerItems, &Item{
			name:         oAttrs.Name,
			container:    c,
			client:       c.client,
			size:         int64(oAttrs.Size),
			hash:         fmt.Sprintf("%x", oAttrs.MD5),
			lastModified: oAttrs.Updated,
			url:          u,
			metadata:     mdParsed,
			object:       o,
		})
	}

	return containerItems, nextToken, nil
}

func (c *Container) RemoveItem(id string) error {
	return c.client.Bucket(c.name).Object(id).Delete(context.Background())
}

// Put sends a request to upload content to the container. The arguments
// received are the name of the item, a reader representing the
// content, and the size of the file.
func (c *Container) Put(name string, r io.Reader, size int64, metadata map[string]interface{}) (stow.Item, error) {
	obj := c.client.Bucket(c.name).Object(name)
	wc := obj.NewWriter(context.Background())

	size, err := io.Copy(wc, r)
	if err != nil {
		return nil, err
	}

	if err := wc.Close(); err != nil {
		return nil, err
	}

	res, err := obj.Attrs(context.Background())
	if err != nil {
		return nil, err
	}

	u, err := prepUrl(res.MediaLink)
	if err != nil {
		return nil, err
	}

	mdParsed, err := parseMetadata(res.Metadata)
	if err != nil {
		return nil, err
	}

	newItem := &Item{
		name:         name,
		container:    c,
		client:       c.client,
		size:         size,
		hash:         fmt.Sprintf("%x", res.MD5),
		lastModified: res.Updated,
		url:          u,
		metadata:     mdParsed,
		object:       obj,
	}
	return newItem, nil
}

func parseMetadata(metadataParsed map[string]string) (map[string]interface{}, error) {
	metadataParsedMap := make(map[string]interface{}, len(metadataParsed))
	for key, value := range metadataParsed {
		metadataParsedMap[key] = value
	}
	return metadataParsedMap, nil
}

func prepMetadata(metadataParsed map[string]interface{}) (map[string]string, error) {
	returnMap := make(map[string]string, len(metadataParsed))
	for key, value := range metadataParsed {
		str, ok := value.(string)
		if !ok {
			return nil, errors.Errorf(`value of key '%s' in metadata must be of type string`, key)
		}
		returnMap[key] = str
	}
	return returnMap, nil
}
