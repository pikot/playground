package main

import (
	"log"
	"os"

	"flag"
	"key_counter/external_sort"
	"runtime"
	"runtime/pprof"
)

func main() {
	N := flag.Int("n", 1000, "")
	var fname, resFname, cpuprofile, memprofile string
	flag.StringVar(&fname, "i", "input_data.txt", "")
	flag.StringVar(&resFname, "o", "output_data.txt", "")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile to `file`")
	flag.StringVar(&memprofile, "memprofile", "", "write memory profile to `file`")

	flag.Parse()

	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}

	external.FreeChunkDir()

	log.Printf("split")
	cntFiles, err := external.SplitInputFile(fname, *N)
	if nil != err {
		log.Fatal(err)
	}
	log.Printf("sort")
	sortedFile, err := external.Sort(cntFiles)
	if nil != err {
		log.Fatal(err)
	}
	log.Printf("merte")
	err = external.CreateTsvFile(sortedFile, resFname)
	if nil != err {
		log.Fatal("error %v", err)
	}
	log.Printf("res in %s", resFname)
}
