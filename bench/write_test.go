package write_test

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

const testFilename = "test.txt"
const payload = "123456789012345\n"

func setup(b *testing.B) *os.File {
	file, err := os.Create(testFilename)

	if err != nil {
		b.Error("Error creating:", testFilename, "=>", err)
	}

	return file
}

func teardown(b *testing.B, file *os.File) {
	err := file.Close()

	if err != nil {
		b.Error("Error closing:", testFilename, "=>", err)
	}

	err = os.Remove(testFilename)

	if err != nil {
		b.Error("Error removing:", testFilename, "=>", err)
	}
}

func BenchmarkWriteAndSync(b *testing.B) {
	file := setup(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		file.WriteString(payload)
		file.Sync()
	}

	teardown(b, file)
}

func BenchmarkWriteThenSync(b *testing.B) {
	file := setup(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		file.WriteString(payload)
	}

	file.Sync()

	teardown(b, file)
}

func BenchmarkWriteMultipleThenSync(b *testing.B) {
	file := setup(b)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 50000; j++ {
			file.WriteString(payload)
		}

		file.Sync()
	}

	teardown(b, file)
}

func BenchmarkWriteOnceThenSyncSlice(b *testing.B) {
	file := setup(b)

	b.ResetTimer()

	buffer := []string{}

	for i := 0; i < b.N; i++ {
		for j := 0; j < 50000; j++ {
			buffer = append(buffer, payload)
		}

		file.WriteString(strings.Join(buffer, ""))
		file.Sync()

		buffer = []string{}
	}

	teardown(b, file)
}

func BenchmarkWriteOnceThenSyncBuffer(b *testing.B) {
	file := setup(b)

	b.ResetTimer()

	var buffer bytes.Buffer

	for i := 0; i < b.N; i++ {
		for j := 0; j < 50000; j++ {
			buffer.WriteString(payload)
		}

		file.WriteString(buffer.String())
		file.Sync()

		buffer.Reset()
	}

	teardown(b, file)
}

func BenchmarkWriteOnceThenSyncBufferBytes(b *testing.B) {
	file := setup(b)

	b.ResetTimer()

	var buffer bytes.Buffer

	for i := 0; i < b.N; i++ {
		for j := 0; j < 50000; j++ {
			buffer.WriteString(payload)
		}

		file.Write(buffer.Bytes())
		file.Sync()

		buffer.Reset()
	}

	teardown(b, file)
}
