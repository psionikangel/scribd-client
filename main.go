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
	"time"

	"github.com/psionikangel/uuid"
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
	RunID        string
}

type run struct {
	ID          string
	Machinename string
	Start       time.Time
	End         time.Time
}

func main() {
	// Initialize the unique id library
	uuid.Init()
	cfg := loadConfig()
	runid := uuid.NewV4().String()
	startRun(runid, *cfg)
	for _, path := range cfg.Paths {
		files, err := extractMetadata(path, cfg.Properties, runid)
		if err != nil {
			panic(err)
		}
		uploadMetadata(files, *cfg)
		endRun(runid, *cfg)
	}
}

func startRun(runid string, cfg config) {
	r := new(run)
	r.ID = runid
	r.Start = time.Now()
	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	r.Machinename = host
	url := "http://" + cfg.Server + ":" + cfg.Port + "/run"
	js, err := json.Marshal(r)
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
	fmt.Println("Start Run Response status", resp.Status)
}

func endRun(runid string, cfg config) {
	r := new(run)
	r.ID = runid
	r.End = time.Now()
	url := "http://" + cfg.Server + ":" + cfg.Port + "/run"
	js, err := json.Marshal(r)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(js))
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
	fmt.Println("End Run Response status", resp.Status)
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

func extractMetadata(path string, props []string, runid string) (meta []metadata, err error) {
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
				if !info.IsDir() {
					meta.Extension = filepath.Ext(path)
				}
			}
			if prop == "checksum" {
				if !info.IsDir() {
					meta.Checksum = checksum(path)
				}
			}
			meta.RunID = runid
		}
		results = append(results, *meta)
		return nil
	})
	return results, nil
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
