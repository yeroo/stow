package google

import (
	"errors"
	"net/url"

	"github.com/graymeta/stow"
	"golang.org/x/net/context"
	"cloud.google.com/go/storage"
)

// Kind represents the name of the location/storage type.
const Kind = "google"

const (
	ConfigProjectId = "project_id"
)

func init() {

	makefn := func(config stow.Config) (stow.Location, error) {
		_, ok := config.Config(ConfigProjectId)
		if !ok {
			return nil, errors.New("missing Project ID")
		}

		// Create a new client
		client, err := newGoogleStorageClient()
		if err != nil {
			return nil, err
		}

		// Create a location with given config and client
		loc := &Location{
			config: config,
			client: client,
		}

		return loc, nil
	}

	kindfn := func(u *url.URL) bool {
		return u.Scheme == Kind
	}

	stow.Register(Kind, makefn, kindfn)
}

// Attempts to create a session based on the information given.
func newGoogleStorageClient() (*storage.Client, error) {

	service, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, err
	}

	return service, nil
}
