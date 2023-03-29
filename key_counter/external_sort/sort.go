package external

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func mergeChunks(il1 *StatFileIn, il2 *StatFileIn, oi *StatFileOut) {
	v1, err1 := il1.GetString()
	v2, err2 := il2.GetString()

	for {
		if nil != err1 || nil != err2 {
			break
		}
		cmpRes := strings.Compare(v1, v2)
		if 0 == cmpRes {
			oi.Write(v1)
			oi.Write(v2)
			v1, err1 = il1.GetString()
			v2, err2 = il2.GetString()
			if nil != err1 || nil != err2 {
				break
			}
		} else if 1 > cmpRes {
			oi.Write(v1)
			v1, err1 = il1.GetString()
			if nil != err1 {
				break
			}
		} else {
			oi.Write(v2)
			v2, err2 = il2.GetString()
			if nil != err2 {
				break
			}
		}
	}
	if nil != err1 {
		oi.Write(v2)
	}

	if nil != err2 {
		oi.Write(v1)
	}

	il2.WriteRest(oi)
	il1.WriteRest(oi)
}

func copyChunk(level, cntFiles, iter uint32) {
	Original_Path := StatFilePath(level, cntFiles-1)
	New_Path := StatFilePath(cntFiles, iter)

	os.Rename(Original_Path, New_Path)
}

func processChunks(level uint32, cntFiles uint32) (levelNext uint32, cntFilesNext uint32, err error) {
	levelNext = cntFiles
	cntFilesNext = 0
	if cntFiles >= 2 {
		var il1, il2 *StatFileIn
		var oi *StatFileOut
		for i := uint32(0); i < cntFiles && (cntFiles-i) > 1; i += 2 {
			il1, err = NewStatFileIn(level, i)
			if nil != err {
				return
			}
			il2, err = NewStatFileIn(level, i+1)
			if nil != err {
				return
			}
			oi, err = NewStatFileOut(cntFiles, cntFilesNext)
			if nil != err {
				return
			}

			mergeChunks(il1, il2, oi)

			il1.Close()
			il2.Close()
			oi.Close()
			cntFilesNext += 1
		}
	}
	if cntFiles%2 != 0 {
		copyChunk(level, cntFiles, cntFilesNext)
		cntFilesNext += 1
	}
	return
}

func SplitInputFile(fname string, maxICnt int) (uint32, error) {
	file, err := os.Open(fname)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	cntr := 0
	cntFiles := uint32(0)
	scanner := bufio.NewScanner(file)
	arr := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		arr = append(arr, line)
		if cntr == maxICnt {
			iter, err := NewStatFileOut(uint32(0), cntFiles)
			if nil != err {
				return 0, err
			}
			iter.WriteArr(arr)
			iter.Close()

			arr = []string{}
			cntFiles += 1
			cntr = 0
		} else {
			cntr += 1
		}
	}
	if cntr > 0 {
		iter, err := NewStatFileOut(uint32(0), cntFiles)
		if nil != err {
			return 0, err
		}
		iter.WriteArr(arr)
		iter.Close()
		cntFiles += 1
	}

	if err := scanner.Err(); err != nil {
		return 0, err
	}
	return cntFiles, nil
}

func Sort(cntSplitedFiles uint32) (sortedFname string, err error) {

	level := uint32(0)
	cntFiles := cntSplitedFiles
	sortedFname = ""
	err = nil
	for {
		if cntFiles <= 1 {
			sortedFname = StatFilePath(level, cntFiles-1)
			break
		}
		level, cntFiles, err = processChunks(level, cntFiles)
		if nil != err {
			return
		}
	}
	return
}

func CreateTsvFile(sortedFname string, resFname string) error {
	file, err := os.Open(sortedFname)
	if err != nil {
		return err
	}
	defer file.Close()

	fOut, err := os.Create(resFname)
	if err != nil {
		return err
	}
	defer fOut.Close()

	w := bufio.NewWriter(fOut)
	cnt := 0
	prev := ""
	line := ""

	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		line = scanner.Text()
		prev = line
		cnt = 1
	}
	for scanner.Scan() {
		line = scanner.Text()
		if line == prev {
			cnt += 1
		} else {
			_, err = fmt.Fprintf(w, "%s\t%d\n", prev, cnt)
			if nil != err {
				return err
			}

			prev = line
			cnt = 1
		}
	}
	if cnt > 0 {
		_, err = fmt.Fprintf(w, "%s\t%d\n", line, cnt)
		if nil != err {
			return err
		}
	}
	w.Flush()
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}
