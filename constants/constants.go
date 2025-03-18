package constants

import (
	"FranzMQ/mem_key_generator"

	"go.opentelemetry.io/otel/trace"
)

const FilesDir = "./files/topics/"

var OffsetMap = mem_key_generator.NewSafeMap()
var LogSizeMap = mem_key_generator.NewSafeMap()
var Tracer trace.Tracer // Exported variable
