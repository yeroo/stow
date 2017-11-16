package google

import (
	"errors"
	"net/url"
	"strings"

	"github.com/graymeta/stow"

	"cloud.google.com/go/storage"
	"context"
	"google.golang.org/api/iterator"
)

// A Location contains a client + the configurations used to create the client.
type Location struct {
	config stow.Config
	client *storage.Client
}

func (l *Location) Service() *storage.Client {
	return l.client
}

// Close simply satisfies the Location interface. There's nothing that
// needs to be done in order to satisfy the interface.
func (l *Location) Close() error {
	return nil // nothing to close
}

// CreateContainer creates a new container, in this case a bucket.
func (l *Location) CreateContainer(containerName string) (stow.Container, error) {
	ctx := context.Background()
	projId, _ := l.config.Config(ConfigProjectId)

	bkt := l.client.Bucket(containerName)
	// Create a bucket.
	err := bkt.Create(ctx, projId, nil)
	if err != nil {
		return nil, err
	}

	newContainer := &Container{
		name:   containerName,
		client: l.client,
	}

	return newContainer, nil
}

// Containers returns a slice of the Container interface, a cursor, and an error.
func (l *Location) Containers(prefix string, cursor string, count int) ([]stow.Container, string, error) {
	ctx := context.Background()
	projId, _ := l.config.Config(ConfigProjectId)

	// List all objects in a bucket using pagination
	iter := l.client.Buckets(ctx, projId)

	if len(prefix) > 0 {
		iter.Prefix = prefix
	}

	pager := iterator.NewPager(iter, count, cursor)
	bucketAttrs := make([]*storage.BucketAttrs, 0)

	nextToken, err := pager.NextPage(bucketAttrs)
	if err != nil {
		return nil, "", err
	}
	containers := make([]stow.Container, len(bucketAttrs))
	for _, bucketAttr := range bucketAttrs {
		containers = append(containers, &Container{
			name:   bucketAttr.Name,
			client: l.client,
		})
	}

	return containers, nextToken, nil
}

// Container retrieves a stow.Container based on its name which must be
// exact.
func (l *Location) Container(id string) (stow.Container, error) {

	_, err := l.client.Bucket(id).Attrs(context.Background())

	if err != nil {
		return nil, stow.ErrNotFound
	}

	c := &Container{
		name:   id,
		client: l.client,
	}

	return c, nil
}

// RemoveContainer removes a container simply by name.
func (l *Location) RemoveContainer(id string) error {

	bkt := l.client.Bucket(id)

	if err := bkt.Delete(context.Background()); err != nil {
		return err
	}

	return nil
}

// ItemByURL retrieves a stow.Item by parsing the URL, in this
// case an item is an object.
func (l *Location) ItemByURL(url *url.URL) (stow.Item, error) {

	if url.Scheme != Kind {
		return nil, errors.New("not valid google storage URL")
	}

	// /download/storage/v1/b/stowtesttoudhratik/o/a_first%2Fthe%20item
	pieces := strings.SplitN(url.Path, "/", 8)

	c, err := l.Container(pieces[5])
	if err != nil {
		return nil, stow.ErrNotFound
	}

	i, err := c.Item(pieces[7])
	if err != nil {
		return nil, stow.ErrNotFound
	}

	return i, nil
}
