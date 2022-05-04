package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	source      = flag.String("source", "http://content-riak-ams-1.int.avs.io:8098", "")
	destination = flag.String("destination", "http://riak-0.riak:8098", "")
	bucketTypes = flag.String("bucket-types", "default,sets,maps", "")
	parallel    = flag.Int("parallel", 10, "")
	timeout     = flag.Duration("timeout", time.Minute*5, "")
)

func main() {
	flag.Parse()
	http.DefaultClient.Timeout = *timeout
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

	var buckets struct {
		Buckets []string `json:"buckets"`
	}
	if err = json.NewDecoder(res.Body).Decode(&buckets); err != nil {
		return fmt.Errorf("decode bucket list err: %w", err)
	}

	for _, bucket := range buckets.Buckets {
		if err = syncBucket(bucketType, bucket); err != nil {
			return fmt.Errorf("sync bucket %s err: %w", bucket, err)
		}
		log.Println("INFO: finish sync bucket: ", bucket)
	}
	return nil
}

func syncBucket(bucketType, bucket string) error {
	log.Printf("INFO: start sync bucket '%s'\n", bucket)

	if err := syncProperties(bucketType, bucket); err != nil {
		return fmt.Errorf("props: %w", err)
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
