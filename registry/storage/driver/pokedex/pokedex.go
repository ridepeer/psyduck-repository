package pokedex

import (
	"fmt"
	"io"
	goPath "path"
	"reflect"
	"strconv"
	"strings"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const driverName = "pokedex"

// The actual driver type along with associated params
type Driver struct {
	Client        *PokedexClient
	RootDirectory string
}

// pokedexDriverFactory implements the factory.StorageDriverFactory interface
type pokedexDriverFactory struct{}

func (factory *pokedexDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

func init() {
	factory.Register(driverName, &pokedexDriverFactory{})
}

// PokedexStorageDriver is a storagedriver.StorageDriver implementation backed by Pokedex
// Objects are stored at absolute keys in the provided bucket.
type BaseEmbed struct{ base.Base }
type PokedexStorageDriver struct{ BaseEmbed }

func FromParameters(parameters map[string]interface{}) (*PokedexStorageDriver, error) {
	host, ok := parameters["host"]
	if !ok {
		return nil, fmt.Errorf("No host parameter provided")
	}

	portParam, ok := parameters["port"]
	if !ok {
		return nil, fmt.Errorf("No port parameter provided")
	}

	port := int(0)
	switch v := portParam.(type) {
	case string:
		vv, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("Port parameter must be an integer, %v invalid", portParam)
		}
		port = vv
	case int:
		port = v
	case int64, uint, int32, uint32, uint64:
		port = int(reflect.ValueOf(v).Convert(reflect.TypeOf(port)).Int())
	default:
		return nil, fmt.Errorf("invalid value for port: %#v", portParam)
	}

	rootDir, ok := parameters["rootDir"]
	if !ok {
		return nil, fmt.Errorf("No rootDir parameter provided")
	}

	driver := Driver{
		&PokedexClient{host.(string), port},
		fmt.Sprint(rootDir),
	}

	return New(&driver)
}

func New(driver *Driver) (*PokedexStorageDriver, error) {
	return &PokedexStorageDriver{
		BaseEmbed: BaseEmbed{
			Base: base.Base{
				StorageDriver: driver,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *Driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *Driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	fullPath := d.makePath(path)
	key, err := d.Client.GetKey(fullPath)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	return key.GetContentsAsBytes()
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *Driver) PutContent(ctx context.Context, path string, content []byte) error {
	fullPath := d.makePath(path)
	key, err := d.Client.CreateKey(fullPath)
	if err != nil {
		return err
	}
	if key == nil {
		return fmt.Errorf("Failed to create key at path: %s", fullPath)
	}
	return key.SetContentsFromBytes(content, d.getContentType())
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *Driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	fullPath := d.makePath(path)
	key, err := d.Client.GetKey(fullPath)
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}
	return key.GetContentsAsStream(offset)
}

// WriteStream stores the contents of the provided io.ReadCloser at a
// location designated by the given path.
// May be used to resume writing a stream by providing a nonzero offset.
// The offset must be no larger than the CurrentSize for this path.
func (d *Driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (nn int64, err error) {
	fullPath := d.makePath(path)
	key, err := d.Client.CreateKey(fullPath)
	if err != nil {
		return 0, err
	}
	if key == nil {
		return 0, storagedriver.PathNotFoundError{Path: path}
	}
	return key.SetContentsFromStream(offset, reader, d.getContentType())
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *Driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fullPath := d.makePath(path)

	fi := storagedriver.FileInfoFields{Path: path}

	key, err := d.Client.GetKey(fullPath)
	if err != nil {
		return nil, err
	}
	if key == nil {
		// check to see if it's a directory
		subKeys, err := d.Client.List(fullPath)
		if err != nil {
			return nil, err
		}
		if len(subKeys) > 0 {
			fi.IsDir = true
		} else {
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
	} else {
		fi.Size = *key.ContentLength
		fi.ModTime = key.Modified.Time
	}
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the
//given path.
func (d *Driver) List(ctx context.Context, path string) ([]string, error) {
	fullPath := d.makePath(path)

	keys, err := d.Client.List(fullPath)
	if err != nil {
		return nil, err
	}

	files := []string{}
	dirs := []string{}

	// first we iterate through all the keys, and grab the name of each key
	// which is a direct descendant of `path`
	for _, k := range keys {
		// remove our search prefix from the name
		rawPath := strings.Replace(k.Name, fullPath, "", 1)
		// split into parts
		parts := strings.Split(strings.TrimLeft(rawPath, "/"), "/")
		if len(parts) == 1 {
			// this is just a file
			files = append(files, goPath.Join(path, parts[0]))
		} else {
			// this is a directory
			newDir := goPath.Join(path, parts[0])
			if !StringSliceContains(dirs, newDir) {
				dirs = append(dirs, newDir)
			}
		}
	}

	if path != "/" {
		if len(files) == 0 && len(dirs) == 0 {
			// Treat empty response as missing directory, since we don't actually
			// have dirs in pokedex.
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
	}

	return append(files, dirs...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
// Note: This may be no more efficient than a copy followed by a delete for
// many implementations.
func (d *Driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	fullSourcePath := d.makePath(sourcePath)
	fullDestPath := d.makePath(destPath)

	sourceKey, err := d.Client.GetKey(fullSourcePath)
	if err != nil {
		return err
	}
	if sourceKey == nil {
		return storagedriver.PathNotFoundError{Path: sourcePath}
	}

	// Pokedex doesn't support moving over an existing path - so we need to
	// delete destPath before moving sourcePath over it

	destKey, err := d.Client.GetKey(fullDestPath)
	if err != nil {
		return err
	}
	if destKey != nil {
		// dest exists - delete it!
		if err = destKey.Delete(); err != nil {
			return err
		}
	}

	return sourceKey.Move(fullDestPath)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *Driver) Delete(ctx context.Context, path string) error {
	fullPath := d.makePath(path)
	numDeleted, err := d.Client.Delete(fullPath)
	if err != nil {
		return err
	} else if numDeleted == 0 {
		return storagedriver.PathNotFoundError{Path: path}
	}
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
func (d *Driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	fullPath := d.makePath(path)
	key, err := d.Client.GetKey(fullPath)
	if err != nil {
		return "", err
	}
	if key == nil {
		return "", storagedriver.PathNotFoundError{Path: path}
	}
	return key.DataUrl(), nil
}

func (d *Driver) makePath(path string) string {
	return strings.TrimLeft(
		strings.TrimRight(d.RootDirectory, "/")+path, "/")
}

func (d *Driver) getContentType() string {
	return "application/octet-stream"
}
