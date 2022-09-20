package gzipio

import (
	"context"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/direct"
	"path"
	"path/filepath"
	"runtime"
	"testing"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	assert := assert.New(t)

	_, filename, _, _ := runtime.Caller(0)
	inpath := filepath.Join(path.Dir(filename), "test", "sample.txt.gz")
	outpath := filepath.Join(path.Dir(filename), "test", "output.read.txt")

	tests := map[string]struct {
		input  string
		output string
		isErr  bool
	}{
		"success": {input: inpath, output: outpath},
	}

	for _, tc := range tests {
		p, root := beam.NewPipelineWithRoot()
		co := Read(root, tc.input)
		textio.Write(root, tc.output, co)
		_, err := direct.Execute(context.Background(), p)
		assert.Equal(tc.isErr, err != nil)
	}
}

func TestWrite(t *testing.T) {
	assert := assert.New(t)

	_, filename, _, _ := runtime.Caller(0)
	inpath := filepath.Join(path.Dir(filename), "test", "sample.txt")
	outpath := filepath.Join(path.Dir(filename), "test", "output.write.gz")
	routpath := filepath.Join(path.Dir(filename), "test", "output.write.txt")

	tests := map[string]struct {
		input   string
		output  string
		routput string
		isErr   bool
	}{
		"success": {input: inpath, output: outpath, routput: routpath},
	}

	for _, tc := range tests {
		// write gzip
		p, root := beam.NewPipelineWithRoot()
		co := textio.Read(root, tc.input)
		Write(root, tc.output, co)
		_, err := direct.Execute(context.Background(), p)
		assert.Equal(tc.isErr, err != nil)

		// write ungzip
		p, root = beam.NewPipelineWithRoot()
		co = Read(root, tc.output)
		textio.Write(root, tc.routput, co)
		_, err = direct.Execute(context.Background(), p)
		assert.Equal(tc.isErr, err != nil)

	}
}
