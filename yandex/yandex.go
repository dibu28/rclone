// Package yandex provides an interface to the Yandex Disk storage.
//
// dibu28 <dibu28@gmail.com> github.com/dibu28

package yandex

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"time"

	yandex "./yandex_disk_api"
	"github.com/ncw/rclone/fs"
	"github.com/ncw/rclone/oauthutil"
	"golang.org/x/oauth2"
)

//oAuth
const (
	rcloneClientID     = "4515bc338b134492aedbc16efde1c3af" //TODO: replace with rclone ID
	rcloneClientSecret = "95b36c4a0345468ea35ed383c2944570" //TODO: replace with rclone Secret
)

//mkdir cache states
const mkdirCreate = 1
const mkdirExist = 2

// Globals
var (
	// Description of how to auth for this app
	oauthConfig = &oauth2.Config{
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://oauth.yandex.com/authorize", //same as https://oauth.yandex.ru/authorize
			TokenURL: "https://oauth.yandex.com/token",     //same as https://oauth.yandex.ru/token
		},
		ClientID:     rcloneClientID,
		ClientSecret: rcloneClientSecret,
		RedirectURL:  oauthutil.RedirectLocalhostURL,
	}
)

// Register with Fs
func init() {
	fs.Register(&fs.Info{
		Name:  "yandex",
		NewFs: NewFs,
		Config: func(name string) {
			err := oauthutil.Config(name, oauthConfig)
			if err != nil {
				log.Fatalf("Failed to configure token: %v", err)
			}
		},
		Options: []fs.Option{{
			Name: oauthutil.ConfigClientID,
			Help: "Yandex Client Id - leave blank normally.",
		}, {
			Name: oauthutil.ConfigClientSecret,
			Help: "Yandex Client Secret - leave blank normally.",
		}},
	})
}

// Fs represents a remote yandex
type Fs struct {
	name       string
	yd         *yandex.Client // client for rest api
	root       string
	disk_root  string
	mkdircache map[string]int
}

// Object describes a swift object
type Object struct {
	fs      *Fs                         // what this object is part of
	remote  string                      // The remote path
	md5sum  string                      // The MD5Sum of the object
	bytes   uint64                      // Bytes in the object
	modTime time.Time                   // Modified time of the object
	item    yandex.ResourceInfoResponse // file metadata from yandex disk object
}

// ------------------------------------------------------------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Yandex %s", f.root)
}

// read access token from ConfigFile string
func getAccessToken(name string) (*oauth2.Token, error) {
	// Read the token from the config file
	token_config := fs.ConfigFile.MustValue(name, "token")
	//Get access token from config string
	decoder := json.NewDecoder(strings.NewReader(token_config))
	var result *oauth2.Token
	err := decoder.Decode(&result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NewFs constructs an Fs from the path, container:path
func NewFs(name, root string) (fs.Fs, error) {
	//read access token from config
	token, err := getAccessToken(name)
	if err != nil {
		return nil, err
	}

	//create new client
	yandexDisk := yandex.NewClient(token.AccessToken)

	f := &Fs{
		yd: yandexDisk,
	}

	f.setRoot(root)

	//create map to cache paths
	f.mkdircache = make(map[string]int)

	//TODO limiter
	// if f.root != "" {
	// 	f.root += "/"
	// 	fs.Log("root2: ", f.root)
	// 	// Check to see if the object exists
	// 	//_, err = f.svc.Objects.Get(bucket, directory).Do()
	// 	//if err == nil {
	// 		remote := path.Base(root)
	// 		fs.Log("remote: ", remote)
	// 		f.root = path.Dir(root)
	// 		fs.Log("root3: ", f.root)
	// 		if f.root == "." {
	// 			f.root = ""
	// 		} else {
	// 			f.root += "/"
	// 		}
	// 		fs.Log("root5: ", f.root)
	// 		obj := f.NewFsObject(remote) //remote
	// 		// return a Fs Limited to this object
	// 		return fs.NewLimited(f, obj), nil
	// 	//}
	// }

	return f, nil
}

// Sets root in f
func (f *Fs) setRoot(root string) {
	//Set root path
	f.root = strings.Trim(root, "/")
	//Set disk root path.
	//Adding "disk:" to root path as all paths on disk start with it
	var disk_root = ""
	if f.root == "" {
		disk_root = "disk:/"
	} else {
		disk_root = "disk:/" + f.root + "/"
	}
	f.disk_root = disk_root
}

// list the objects into the function supplied
//
// If directories is set it only sends directories
func (f *Fs) list(directories bool, fn func(string, yandex.ResourceInfoResponse)) {
	//request files list. list is divided into pages. We send request for each page
	//items per page is limited by limit
	//TODO may be add config parameter for the items per page limit
	var limit uint32 = 1000   // max number of object per request
	var itemsCount uint32 = 0 //number of items per page in response
	var offset uint32 = 0     //for the next page of request
	// yandex disk api request options
	var opt yandex.FlatFileListRequestOptions
	opt.Limit = &limit
	opt.Offset = &offset
	//query each page of list until itemCount is less then limit
	for {
		//send request
		info, err := f.yd.NewFlatFileListRequest(opt).Exec()
		if err != nil {
			fs.Stats.Error()
			fs.ErrorLog(f, "Couldn't list: %s", err)
			return
		}
		itemsCount = uint32(len(info.Items))

		//list files
		for _, item := range info.Items {
			// filter file list and get only files we need
			if strings.HasPrefix(item.Path, f.disk_root) {
				//trim root folder from filename
				var name = strings.TrimPrefix(item.Path, f.disk_root)
				fn(name, item)
			}
		}

		//offset for the next page of items
		offset += itemsCount
		//check if we reached end of list
		if itemsCount < limit {
			break
		}
	}
}

// List walks the path returning a channel of FsObjects
func (f *Fs) List() fs.ObjectsChan {
	out := make(fs.ObjectsChan, fs.Config.Checkers)
	// List the objects
	go func() {
		defer close(out)
		f.list(false, func(remote string, object yandex.ResourceInfoResponse) {
			if fs := f.newFsObjectWithInfo(remote, &object); fs != nil {
				out <- fs
			}
		})
	}()
	return out
}

// NewFsObject returns an Object from a path
//
// May return nil if an error occurred
func (f *Fs) NewFsObject(remote string) fs.Object {
	return f.newFsObjectWithInfo(remote, nil)
}

// Return an FsObject from a path
//
// May return nil if an error occurred
func (f *Fs) newFsObjectWithInfo(remote string, info *yandex.ResourceInfoResponse) fs.Object {
	o := &Object{
		fs:     f,
		remote: remote,
		item:   *info,
	}
	if info != nil {
		o.setMetaData(info)
	}
	//TODO we have received all metadata in the file list may be no need to call readMetaData
	return o
}

// setMetaData sets the fs data from a storage.Object
func (o *Object) setMetaData(info *yandex.ResourceInfoResponse) {
	o.bytes = info.Size
	o.md5sum = info.Md5
	//modTime
	t, err := time.Parse(time.RFC3339Nano, info.Modified)
	if err != nil {
		return
	}
	o.modTime = t
}

// readMetaData gets the info if it hasn't already been fetched
func (o *Object) readMetaData() (err error) {
	//TODO we have received all metadata in the file list may be no need to query it again
	return nil
}

// ListDir walks the path returning a channel of FsObjects
func (f *Fs) ListDir() fs.DirChan {
	out := make(fs.DirChan, fs.Config.Checkers)
	go func() {
		defer close(out)

		//TODO not implemeted

	}()
	return out
}

// Put the object
//
// Copy the reader in to the new object which is returned
//
// The new object may have been created if an error is returned
func (f *Fs) Put(in io.Reader, remote string, modTime time.Time, size int64) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
		bytes:  uint64(size),
	}
	//TODO maybe read metadata after upload to check if file uploaded successfully
	return o, o.Update(in, modTime, size)
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir() error {
	f.mkdircache = MkDirFullPathCached(f.yd, f.root, f.mkdircache)
	return nil
}

// Rmdir deletes the container
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir() error {

	//TODO not implemented

	return nil
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// ------------------------------------------------------------

// Fs returns the parent Fs
func (o *Object) Fs() fs.Fs {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Md5sum returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Md5sum() (string, error) {
	return o.md5sum, nil
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	var size int64 = int64(o.bytes) //need to cast from uint64 in yandex disk to int64 in rclone. can cause overflow
	return size
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime() time.Time {
	err := o.readMetaData()
	if err != nil {
		fs.Log(o, "Failed to read metadata: %s", err)
		return time.Now()
	}
	return o.modTime
}

// Open an object for read
func (o *Object) Open() (in io.ReadCloser, err error) {
	return o.fs.yd.Download(o.remotePath())
}

// Remove an object
func (o *Object) Remove() error {
	return o.fs.yd.Delete(o.remotePath(), true)
}

// SetModTime sets the modification time of the local fs object
//
// Commits the datastore
func (o *Object) SetModTime(modTime time.Time) {
	//TODO not implemented
	return
}

// Storable returns whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// Returns the remote path for the object
func (o *Object) remotePath() string {
	return o.fs.disk_root + o.remote
}

// Update the already existing object
//
// Copy the reader into the object updating modTime and size
//
// The new object may have been created if an error is returned
func (o *Object) Update(in io.Reader, modTime time.Time, size int64) error {
	remote := o.remotePath()
	//create full path to file before upload.
	o.fs.mkdircache = MkDirFullPathCached(o.fs.yd, remote, o.fs.mkdircache)
	//upload file
	overwrite := true //overwrite existing file
	return o.fs.yd.Upload(in, remote, overwrite)
}

// utility funcs-------------------------------------------------------------------

// execute mkdir
func MkDirCached(client *yandex.Client, path string, mkdircache map[string]int) {
	if err, statusCode := client.Mkdir(path); err != nil {
		if statusCode == 409 { // dir already exist
			mkdircache[path] = mkdirExist
		} else {
			log.Fatalf("Failed to create folder: %v", err)
		}
	} else {
		if statusCode == 201 { // dir was created
			mkdircache[path] = mkdirExist
		}
	}
}

//Creates Each Directory in the path if needed. But only send request once and cache
// paths of created or existing directories.
//Check if some directories need to be created and cache them in the map
func MkDirFullPathCached(client *yandex.Client, path string, mkdircache map[string]int) map[string]int {
	//trim filename from path
	dirString := strings.TrimSuffix(path, filepath.Base(path))
	//trim "disk:/" from path
	dirString = strings.TrimPrefix(dirString, "disk:/")
	dirs := strings.Split(dirString, "/") //path separator /
	var mkdirpath string = "/"            //path separator /
	for _, element := range dirs {
		if element != "" {
			mkdirpath += element + "/" //path separator /
			if mkdircache[mkdirpath] != mkdirExist {
				MkDirCached(client, mkdirpath, mkdircache)
			}
		}
	}
	return mkdircache
}

// Check the interfaces are satisfied
var (
	_ fs.Fs = (*Fs)(nil)
	//_ fs.Purger = (*Fs)(nil)
	//_ fs.Copier = (*Fs)(nil)
	_ fs.Object = (*Object)(nil)
)