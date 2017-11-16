# Google Cloud Storage Stow Implementation

Location = Google Cloud Storage

Container = Bucket

Item = File

## How to access underlying service types

Use a type conversion to extract the underlying `Location`, `Container`, or `Item` implementations. Then use the Google-specific getters to access the internal Google Cloud Storage `Service`, `Bucket`, and `Object` values.

```go
import (
  "log"
  "github.com/graymeta/stow"
  stowgs "github.com/yeroo/stow/google"
)

stowLoc, err := stow.Dial(stowgs.Kind, stow.ConfigMap{
	stowgs.ConfigProjectId: "<project id>",
})
if err != nil {
  log.Fatal(err)
}

stowBucket, err = stowLoc.Container("mybucket")
if err != nil {
  log.Fatal(err)
}

if gsBucket, ok := stowBucket.(*stowgs.Bucket); ok {
  if gsLoc, ok := stowLoc.(*stowgs.Location); ok {

    googleService := gsLoc.Service()
    googleBucket, err := gsBucket.Bucket()

    // < Send platform-specific commands here >

  }
}
```

Current config supports only google project id setting. Default google credentials will be used (very useful for Container Engine deployments)
By default, Stow uses `https://www.googleapis.com/auth/devstorage.read_write` scope. New implementtion of google storage has no configurable scopes yet
```go
stowLoc, err := stow.Dial(stowgs.Kind, stow.ConfigMap{
	stowgs.ConfigProjectId: "<project id>",
})
```

Concerns:

- Google's storage plaform is more _eventually consistent_ than other platforms. Sometimes, the tests appear to be flaky because of this. One example is when deleting files from a bucket, then immediately deleting the bucket...sometimes the bucket delete will fail saying that the bucket isn't empty simply because the file delete messages haven't propagated through Google's infrastructure. We may need to add some delay into the test suite to account for this.
