package external

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"

	"path/filepath"
)

const chunkDir = "./chunks"

func FreeChunkDir() {
	os.RemoveAll(chunkDir)
	os.MkdirAll(chunkDir, 0777)
}

func StatFilePath(level, fidx uint32) string {
	file := strconv.Itoa(int(level)) + "_" + strconv.Itoa(int(fidx)) + ".txt"
	return filepath.Join(chunkDir, file)
}

type StatFileIn struct {
	Path   string
	File   *os.File
	Reader *bufio.Reader
}

func (si StatFileIn) GetString() (string, error) {
	line, err := si.Reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return line, err
	}
	if err == io.EOF {
		return line, err
	}
	line = line[:len(line)-1]
	return line, err
}

func (si StatFileIn) Close() {
	si.File.Close()
}

func NewStatFileIn(level, fidx uint32) (*StatFileIn, error) {
	fpath := StatFilePath(level, fidx)
	f, err := os.Open(fpath)
	if nil != err {
		return nil, err
	}
	r := bufio.NewReader(f)
	return &StatFileIn{Path: fpath, File: f, Reader: r}, nil
}

type StatFileOut struct {
	path   string
	file   *os.File
	writer *bufio.Writer
}

func (si StatFileIn) WriteRest(so *StatFileOut) {
	for {
		str, err := si.GetString()

		if io.EOF == err {
			break
		}
		if str == "" {
			fmt.Println("WriteRes ", str, ", ", err)
		}

		so.writer.WriteString(str + "\n")
	}
}

func (so *StatFileOut) Write(str string) {
	so.writer.WriteString(str + "\n")
}

func (so StatFileOut) WriteArr(arr []string) {
	sort.Strings(arr)
	for _, str := range arr {
		so.writer.WriteString(str + "\n")
	}
}

func (so *StatFileOut) Close() {
	so.writer.Flush()
	so.file.Close()
}

func NewStatFileOut(level, fidx uint32) (*StatFileOut, error) {
	fpath := StatFilePath(level, fidx)

	f, err := os.Create(fpath)
	if nil != err {
		return nil, err
	}

	bufferedWriter := bufio.NewWriter(f)
	return &StatFileOut{path: fpath, file: f, writer: bufferedWriter}, nil
}
