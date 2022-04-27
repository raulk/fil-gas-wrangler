package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/raulk/fil-gas-wrangler/pkg/model"
)

func main() {
	flag.Parse()

	pathOrig := flag.Arg(0)
	orig, err := os.Open(pathOrig)
	if err != nil {
		panic(err)
	}
	defer orig.Close()

	dir, file := filepath.Split(pathOrig)
	ext := filepath.Ext(file)

	var (
		spansPath    = filepath.Join(dir, fmt.Sprintf("%s.spans%s", file, ext))
		contextsPath = filepath.Join(dir, fmt.Sprintf("%s.contexts%s", file, ext))
		pointsPath   = filepath.Join(dir, fmt.Sprintf("%s.points%s", file, ext))
	)

	var spansFile, contextsFile, pointsFile *os.File
	if spansFile, err = os.Create(spansPath); err != nil {
		panic(err)
	}
	defer spansFile.Close()

	if contextsFile, err = os.Create(contextsPath); err != nil {
		panic(err)
	}
	defer contextsFile.Close()

	if pointsFile, err = os.Create(pointsPath); err != nil {
		panic(err)
	}
	defer pointsFile.Close()

	var (
		spansEnc    = json.NewEncoder(spansFile)
		contextsEnc = json.NewEncoder(contextsFile)
		pointsEnc   = json.NewEncoder(pointsFile)
	)

	type SpanOut struct {
		Message     uint              `json:"msg"`
		Context     uint              `json:"ctx"`
		Point       uint              `json:"p"`
		Consumption model.Consumption `json:"c"`
		Timing      model.Timing      `json:"t"`
	}

	type ContextOut struct {
		model.Context
		Id uint `json:"id"`
	}

	type PointOut struct {
		model.Point
		Id uint `json:"id"`
	}

	var (
		contexts = make(map[model.Context]uint)
		points   = make(map[model.Point]uint)
	)

	var msg uint
	scanner := bufio.NewScanner(orig)
	scanner.Buffer(make([]byte, 64*1024), 512*1024*1024)

	for scanner.Scan() {
		fmt.Printf("processing line %d\n", msg)

		scanner.Bytes()
		var traces model.Spans
		if err := json.Unmarshal(scanner.Bytes(), &traces); err != nil {
			fmt.Printf("skipping message %d: %s\n", msg, err)
			continue
		}

		for _, t := range traces {
			var (
				contextId, pointId uint
				ok                 bool
			)
			if contextId, ok = contexts[t.Context]; !ok {
				contextId = uint(len(contexts))
				contexts[t.Context] = contextId

				fmt.Printf("added context %d\n", contextId)

				if err := contextsEnc.Encode(ContextOut{
					Context: t.Context,
					Id:      contextId,
				}); err != nil {
					panic(err)
				}
			}
			if pointId, ok = points[t.Point]; !ok {
				pointId = uint(len(points))
				points[t.Point] = pointId

				fmt.Printf("added point %d\n", pointId)

				if err := pointsEnc.Encode(PointOut{
					Point: t.Point,
					Id:    pointId,
				}); err != nil {
					panic(err)
				}
			}

			span := SpanOut{
				Message:     msg,
				Consumption: t.Consumption,
				Timing:      t.Timing,
				Context:     contextId,
				Point:       pointId,
			}
			if err := spansEnc.Encode(&span); err != nil {
				panic(err)
			}

			fmt.Printf("wrote span\n")
		}
		msg++
	}
}
