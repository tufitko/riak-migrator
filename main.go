package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	source        = flag.String("source", "http://riak-0.riak:8098", "")
	destination   = flag.String("destination", "http://riak-0.riak:8098", "")
	bucketTypes   = flag.String("bucket-types", "default,sets,maps", "")
	parallel      = flag.Int("parallel", 10, "")
	timeout       = flag.Duration("timeout", time.Minute*5, "")
	backup        = flag.Bool("backup", false, "Backup mode")
	skipExisting  = flag.Bool("skip-existing", false, "Skip existing files")
	backupDir     = flag.String("backup-dir", "./backup", "Dir for backups")
	restoreBackup = flag.Bool("restore-backup", false, "Restore from backup")
	backupStdout  = flag.Bool("backup-stdout", false, "Backup to stdout instead of file")
	restoreStdin  = flag.Bool("restore-stdin", false, "Restore from stdin")
)

func main() {
	flag.Parse()
	http.DefaultClient.Timeout = *timeout

	if *restoreStdin {
		try(restoreFromStdin())
		return
	}

	if *restoreBackup {
		try(restoreFromBackup())
		return
	}

	if *backup && !*backupStdout {
		try(os.Mkdir(*backupDir, 0o777))
	}

	for _, bType := range strings.Split(*bucketTypes, ",") {
		try(syncBuckets(bType))
	}

	log.Println("INFO: finish!")
}

func try(err error) {
	if err != nil {
		log.Println("ERR: ", err.Error())
		os.Exit(1)
	}
}

func syncBuckets(bucketType string) error {
	res, err := http.Get(*source + fmt.Sprintf("/types/%s/buckets?buckets=true", bucketType))
	if err != nil {
		return fmt.Errorf("get list of bucket err: %w", err)
	}
	defer res.Body.Close()

	if *backup && !*backupStdout {
		try(os.Mkdir(filepath.Join(*backupDir, bucketType), 0o777))
	}

	var buckets struct {
		Buckets []string `json:"buckets"`
	}
	if err = json.NewDecoder(res.Body).Decode(&buckets); err != nil {
		return fmt.Errorf("decode bucket list err: %w", err)
	}

	for _, bucket := range buckets.Buckets {
		if *skipExisting && *backup && !*backupStdout {
			if _, err := os.Stat(filepath.Join(*backupDir, bucketType)); !os.IsNotExist(err) {
				continue
			}
		}

		if err = syncBucket(bucketType, bucket); err != nil {
			return fmt.Errorf("sync bucket %s err: %w", bucket, err)
		}
		log.Println("INFO: finish sync bucket: ", bucket)
	}
	return nil
}

func syncBucket(bucketType, bucket string) error {
	log.Printf("INFO: start sync bucket '%s'\n", bucket)

	if *backup {
		if !*backupStdout {
			try(os.Mkdir(filepath.Join(*backupDir, bucketType, bucket), 0o777))
		}
	} else {
		if err := syncProperties(bucketType, bucket); err != nil {
			return fmt.Errorf("props: %w", err)
		}
	}

	res, err := http.Get(*source + fmt.Sprintf("/types/%s/buckets/%s/keys?keys=true", bucketType, bucket))
	if err != nil {
		return fmt.Errorf("list keys: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		log.Printf("WARN: bucket %s haven't keys", bucket)
		return nil
	}

	var keys struct {
		Keys []string `json:"keys"`
	}
	if err = json.NewDecoder(res.Body).Decode(&keys); err != nil {
		return fmt.Errorf("decode keys list err: %w", err)
	}

	keysC := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for key := range keysC {
				if err := syncKey(bucketType, bucket, key); err != nil {
					try(fmt.Errorf("ERR(%s): sync key '%s' err: %w", bucket, key, err))
				}
			}
		}()
	}

	tick := time.NewTicker(time.Second * 5)
	defer tick.Stop()

	total := len(keys.Keys)
	for i := 0; i < total; {
		select {
		case <-tick.C:
			log.Printf("INFO: bucket '%s' progress: %d/%d\n", bucket, i, total)
		case keysC <- keys.Keys[i]:
			i++
		}
	}
	close(keysC)

	wg.Wait()
	return nil
}

func syncKey(bucketType, bucket, key string) error {
	key = url.QueryEscape(key)
	res, err := http.Get(*source + fmt.Sprintf("/types/%s/buckets/%s/keys/%s", bucketType, bucket, key))
	if err != nil {
		return fmt.Errorf("get key: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return fmt.Errorf("status code is %d", res.StatusCode)
	}

	if *backup && !*backupStdout {
		buf, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(*backupDir, bucketType, bucket, key), buf, 0o666)
	}

	if *backup && *backupStdout {
		buf, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}

		data, err := json.Marshal(struct {
			BucketType string `json:"bucket_type"`
			Bucket     string `json:"bucket"`
			Key        string `json:"key"`
			Value      []byte `json:"value"`
		}{
			bucketType, bucket, key, buf,
		})
		if err != nil {
			return err
		}

		_, err = os.Stdout.Write(data)
		if err != nil {
			return err
		}
		_, err = os.Stdout.WriteString("\n")
		return err
	}

	req, err := http.NewRequest("PUT", *destination+fmt.Sprintf("/types/%s/buckets/%s/keys/%s", bucketType, bucket, key), res.Body)
	if err != nil {
		return fmt.Errorf("new request err: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 201 && resp.StatusCode != 204 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("got unexpected status: %d, %s", resp.StatusCode, body)
	}
	return nil
}

func syncProperties(bucketType, bucket string) error {
	res, err := http.Get(*source + fmt.Sprintf("/types/%s/buckets/%s/props", bucketType, bucket))
	if err != nil {
		return fmt.Errorf("get properties: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 404 {
		log.Printf("WARN: bucket '%s' not found props", bucket)
		return nil
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("status code is %d", res.StatusCode)
	}

	req, err := http.NewRequest("PUT", *destination+fmt.Sprintf("/types/%s/buckets/%s/props", bucketType, bucket), res.Body)
	if err != nil {
		return fmt.Errorf("new request err: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 && resp.StatusCode != 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("got unexpected status: %d, %s", resp.StatusCode, body)
	}
	return nil
}

func restoreFromBackup() error {
	allKeys := make([]string, 0)
	count := 0

	err := filepath.WalkDir(*backupDir, func(path string, file fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if file.IsDir() {
			return nil
		}

		allKeys = append(allKeys, path)
		return nil
	})
	if err != nil {
		return err
	}

	err = filepath.WalkDir(*backupDir, func(path string, file fs.DirEntry, err error) error {
		count += 1
		if count%1000 == 0 {
			fmt.Println("Now I sync ", path)
			fmt.Printf("Progress: %d/%d\n", count, len(allKeys))
		}

		var kv struct {
			BucketType string `json:"bucket_type"`
			Bucket     string `json:"bucket"`
			Key        string `json:"key"`
			Value      []byte `json:"value"`
		}
		if err != nil {
			return err
		}

		if file.IsDir() {
			return nil
		}

		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		pathSegments := strings.Split(path, "/")

		kv.Key = pathSegments[len(pathSegments)-1]
		kv.Bucket = pathSegments[len(pathSegments)-2]
		kv.BucketType = pathSegments[len(pathSegments)-3]
		kv.Value = b

		req, err := http.NewRequest("PUT", *destination+fmt.Sprintf("/types/%s/buckets/%s/keys/%s", kv.BucketType, kv.Bucket, kv.Key), bytes.NewBuffer(kv.Value))
		if err != nil {
			fmt.Println(fmt.Errorf("new request err: %w", err))
			return err
		}
		req.Header.Add("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println(err)
			return err
		}
		if resp.StatusCode != 200 && resp.StatusCode != 201 && resp.StatusCode != 204 {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			fmt.Println(fmt.Errorf("got unexpected status: %d, %s", resp.StatusCode, body))
			return fmt.Errorf("got unexpected status: %d, %s", resp.StatusCode, body)
		}
		_ = resp.Body.Close()

		return err
	})
	return nil
}

func restoreFromStdin() error {
	stdin := NewLineIterator(os.Stdin)
	for {
		line, err := stdin.Next()
		if err == io.EOF {
			break
		}

		var kv struct {
			BucketType string `json:"bucket_type"`
			Bucket     string `json:"bucket"`
			Key        string `json:"key"`
			Value      []byte `json:"value"`
		}

		err = json.Unmarshal(line, &kv)
		if err != nil {
			return err
		}

		req, err := http.NewRequest("PUT", *destination+fmt.Sprintf("/types/%s/buckets/%s/keys/%s", kv.BucketType, kv.Bucket, kv.Key), bytes.NewBuffer(kv.Value))
		if err != nil {
			return fmt.Errorf("new request err: %w", err)
		}
		req.Header.Add("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 && resp.StatusCode != 201 && resp.StatusCode != 204 {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return fmt.Errorf("got unexpected status: %d, %s", resp.StatusCode, body)
		}
		_ = resp.Body.Close()
	}
	log.Println("finish!")
	return nil
}

type LineIterator struct {
	reader *bufio.Reader
}

func NewLineIterator(rd io.Reader) *LineIterator {
	return &LineIterator{
		reader: bufio.NewReader(rd),
	}
}

func (ln *LineIterator) Next() ([]byte, error) {
	var bytes []byte
	for {
		line, isPrefix, err := ln.reader.ReadLine()
		if err != nil {
			return nil, err
		}
		bytes = append(bytes, line...)
		if !isPrefix {
			break
		}
	}
	return bytes, nil
}
