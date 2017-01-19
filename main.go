package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
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
	FilesCount  int64
}

func main() {
	// Initialize the unique id library
	uuid.Init()
	cfg := loadConfig()
	runid := uuid.NewV4().String()
	startRun(runid, *cfg)
	var totalCount int64
	for _, path := range cfg.Paths {
		files, count, err := extractMetadata(path, cfg.Properties, runid)
		totalCount = totalCount + count
		if err != nil {
			panic(err)
		}
		uploadMetadata(files, *cfg)
	}
	endRun(runid, totalCount, *cfg)
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
}

func endRun(runid string, count int64, cfg config) {
	r := new(run)
	r.ID = runid
	r.End = time.Now()
	r.FilesCount = count
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
}

func extractMetadata(path string, props []string, runid string) (meta []metadata, count int64, err error) {
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
			if prop == "filename" {
				if !info.IsDir() {
					meta.Filename = info.Name()
				}
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
		if !info.IsDir() { //count only files
			count++
		}
		return nil
	})
	return results, count, nil
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
	if len(cfg.Paths) == 0 {
		log.Fatal("Config Error: No paths are defined")
	}
	if len(cfg.Properties) == 0 {
		log.Fatal("Config Error: No properties are defined")
	}
	for _, p := range cfg.Paths {
		if !path.IsAbs(p) {
			log.Fatalf("Config Error: %q is not an absolute path", p)
		}
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
