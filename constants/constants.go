package constants

import "FranzMQ/mem_key_generator"

const FilesDir = "./files/topics/"

var OffsetMap = mem_key_generator.NewSafeMap()
var LogSizeMap = mem_key_generator.NewSafeMap()
