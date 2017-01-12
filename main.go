package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type config struct {
	Paths      []string
	Properties []string
	Server     string
	Port       string
}

type metadata struct {
	Path         string
	Filesize     int64
	LastModified time.Time
	Filename     string
	Extension    string
	Checksum     string
}

func main() {
	cfg := loadConfig()
	for _, path := range cfg.Paths {
		files, err := extractMetadata(path, cfg.Properties)
		if err != nil {
			panic(err)
		}
		printProperties(files, cfg.Properties)
		uploadMetadata(files, *cfg)
	}
}

func uploadMetadata(meta []metadata, cfg config) {
	url := "http://" + cfg.Server + ":" + cfg.Port + "/metadata"
	js, err := json.Marshal(meta)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(js))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	fmt.Println("Upload Response status", resp.Status)
}

func extractMetadata(path string, props []string) (meta []metadata, err error) {
	var results []metadata

	err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		meta := new(metadata)
		for _, prop := range props {
			if prop == "path" {
				meta.Path = path
			}
			if prop == "filesize" {
				meta.Filesize = info.Size()
			}
			if prop == "lastmodified" {
				meta.LastModified = info.ModTime()
			}
			if prop == "extension" {
				meta.Extension = filepath.Ext(path)
			}
			if prop == "checksum" {
				if !info.IsDir() {
					meta.Checksum = checksum(path)
				}
			}
		}
		results = append(results, *meta)
		return nil
	})
	return results, nil
}

func printProperties(files []metadata, props []string) {
	for _, file := range files {
		for _, prop := range props {
			if prop == "path" {
				fmt.Println(file.Path)
			}
			if prop == "filesize" {
				fmt.Println(strconv.FormatInt(file.Filesize, 10))
			}
			if prop == "lastmodified" {
				fmt.Println("LastModified: " + file.LastModified.Format(time.RFC3339))
			}
			if prop == "extension" {
				fmt.Println("Extension: " + file.Extension)
			}
			if prop == "checksum" {
				fmt.Println("Checksum: " + file.Checksum)
			}
		}
	}
}

func loadConfig() *config {
	file, err := ioutil.ReadFile("config.json")
	if err != nil {
		panic(err)
	}
	cfg := new(config)
	err = json.Unmarshal(file, &cfg)
	if err != nil {
		panic(err)
	}
	return cfg
}

func checksum(path string) string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	h := md5.New()
	h.Write(data)
	cs := hex.EncodeToString(h.Sum(nil))
	return cs
}
