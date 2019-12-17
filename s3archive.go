package s3utils

import (
	"archive/zip"
	"fmt"
	"github.com/minio/minio-go/v6"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

func S3PrefixZip(s3Client *minio.Client, bucketName, objectPrefix, outZipName string) {

	// 创建ZIP包
	newZipFile, err := os.Create(outZipName)
	if err != nil {
		log.Fatalln(err)
	}
	defer newZipFile.Close()
	zipWriter := zip.NewWriter(newZipFile)
	defer zipWriter.Close()

	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%#v\n", s3Client) // s3Client is now setup

	buckets, err := s3Client.ListBuckets()
	if err != nil {
		log.Fatalln(err)
	}
	for _, bucket := range buckets {
		log.Println(bucket)
	}

	doneCh := make(chan struct{})

	defer close(doneCh)

	isRecursive := true
	objectCh := s3Client.ListObjectsV2(bucketName, objectPrefix, isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			log.Fatalln(err)
		}
		fmt.Println(object)
		mObj, err := s3Client.GetObject(bucketName, object.Key, minio.GetObjectOptions{})
		if err != nil {
			log.Fatalln(err)
		}
		rel, err := filepath.Rel(objectPrefix, object.Key)
		if err != nil {
			log.Fatalln(err)
		}
		if err = AddFileToZip(zipWriter, rel, mObj); err != nil {
			log.Fatalln(err)
		}
	}
}

func AddFileToZip(zipWriter *zip.Writer, filename string, fileObject io.Reader) error {

	header := &zip.FileHeader{
		Name:         filename,
		Method:       zip.Deflate,
		ModifiedTime: uint16(time.Now().UnixNano()),
		ModifiedDate: uint16(time.Now().UnixNano()),
	}

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileObject)
	return err
}
