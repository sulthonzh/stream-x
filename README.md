# stream-x - Zero-Dependency Node.js Stream Utilities

> 🚀 **Why another stream library?** Because Node.js streams are powerful but painful. Common patterns should be dead simple, not require 3 dependencies and 50 lines of boilerplate.

Zero-dependency Node.js stream utilities library that makes working with streams actually pleasant. Transform streams, pipeline management, error handling, and utilities that should've been built-in.

## Why stream-x?

Building robust stream pipelines shouldn't require a PhD in stream internals. When all you want is:
- Transform JSON data without error-prone manual parsing
- Handle backpressure properly without callback hell
- Create reusable stream components
- Debug stream pipelines that mysteriously fail
- Convert streams to promises for async/await goodness

...you shouldn't need to pull in `through2`, `pipeline`, `stream-chain`, and `readable-wrap` just to get basic functionality.

## Features

### 🔄 **Transform Utilities**
- **Transform**: Simple transform stream with async support
- **JSON Transform**: Auto-parsing/stringifying JSON with error handling
- **Line Transform**: Process text line by line
- **Buffer Transform**: Handle buffer/string conversions
- **Map/Filter**: Stream equivalents of Array.prototype.map/filter

### 🔗 **Pipeline Management**
- **Pipeline**: Robust pipeline with error handling and cleanup
- **Parallel**: Process multiple streams concurrently
- **Series**: Process streams sequentially
- **Batch**: Collect chunks and process in batches
- **Throttle**: Control stream emission rate

### 🛡️ **Error Handling**
- **Safe Transform**: Wraps transforms to prevent crashing pipelines
- **Retry Transform**: Automatically retry failed chunks
- **Timeout Transform**: Timeout long-running stream operations
- **Error Collector**: Collect all errors without stopping pipeline

### 📊 **Utilities**
- **Wait For**: Convert stream to Promise
- **Collect**: Stream all data into array/buffer
- **Count**: Count items/bytes in stream
- **Inspect**: Debug stream events and data flow
- **Split/Join**: Split streams by delimiter or join multiple streams

### 🔧 **Advanced**
- **Switch**: Switch between streams based on conditions
- **Broadcast**: Duplicate stream to multiple consumers
- **Rate Limit**: Control data flow rate
- **Buffer Management**: Smart buffer handling for backpressure

## Quick Start

```javascript
import { Transform, pipeline, collect } from './index.js'

// Simple transform stream
const upper = new Transform({
  transform(chunk, encoding, callback) {
    this.push(chunk.toString().toUpperCase())
    callback()
  }
})

// JSON transformation with error handling
const jsonProcessor = new Transform({
  objectMode: true,
  transform(chunk, encoding, callback) {
    try {
      const data = JSON.parse(chunk.toString())
      const processed = { ...data, processed: true }
      this.push(JSON.stringify(processed))
      callback()
    } catch (error) {
      callback(error)
    }
  }
})

// Pipeline with automatic error handling
pipeline(
  getSomeDataStream(),
  upper,
  jsonProcessor,
  process.stdout,
  (error) => {
    if (error) console.error('Pipeline failed:', error)
    else console.log('Pipeline completed successfully')
  }
)

// Convert stream to async iterable
for await (const chunk of getSomeDataStream()) {
  console.log('Got chunk:', chunk)
}

// Collect stream into array
const allData = await collect(getSomeDataStream())
console.log('Total data:', allData.length, 'items')
```

## CLI Usage

```bash
# Transform JSON data
echo '{"name": "test"}' | node cli.js json --map '({name}) => ({name: name.toUpperCase()})'

# Process files line by line
cat data.txt | node cli.js lines --map 'line => line.trim()'

# Count lines in a file
cat data.txt | node cli.js count --lines

# Stream with batching
cat large-file.json | node cli.js batch --size 100 --process 'batch => processBatch(batch)'

# Debug stream events
cat data.txt | node cli.js debug --events
```

## API Reference

### Transform Streams

#### `new Transform(options)`
Base transform stream with simplified API.

```javascript
import { Transform } from './index.js'

const transformer = new Transform({
  transform(chunk, encoding, callback) {
    // Process chunk
    this.push(transformedChunk)
    callback()
  }
})
```

#### `new JSONTransform(options)`
JSON parsing/stringifying transform with error handling.

```javascript
import { JSONTransform } from './index.js'

// Auto-parse JSON input, stringify output
const jsonTransform = new JSONTransform({
  objectMode: true,
  transform(data, encoding, callback) {
    // Process data object
    this.push(processedData)
    callback()
  }
})
```

#### `new LineTransform(options)`
Process text streams line by line.

```javascript
import { LineTransform } from './index.js'

const lineProcessor = new LineTransform({
  transform(line, encoding, callback) {
    // Process line (without newline)
    this.push(processedLine + '\n')
    callback()
  }
})
```

### Pipeline Utilities

#### `pipeline(...streams, callback)`
Robust pipeline with error handling and cleanup.

```javascript
import { pipeline, Transform } from './index.js'

pipeline(
  readableStream,
  transform1,
  transform2,
  writableStream,
  (error) => {
    if (error) console.error('Pipeline error:', error)
  }
)
```

#### `collect(stream)`
Collect all data from a stream into an array/buffer.

```javascript
import { collect } from './index.js'

const data = await collect(readableStream)
console.log('Collected:', data)
```

#### `waitFor(stream)`
Convert a stream to a Promise that resolves when the stream ends.

```javascript
import { waitFor } from './index.js'

try {
  await waitFor(readableStream)
  console.log('Stream completed successfully')
} catch (error) {
  console.error('Stream failed:', error)
}
```

### Error Handling

#### `new SafeTransform(transformFn)`
Wrap a transform function to prevent pipeline crashes.

```javascript
import { SafeTransform } from './index.js'

const safeTransformer = new SafeTransform(async (chunk) => {
  // This function can throw without crashing the pipeline
  if (chunk.invalid) throw new Error('Invalid chunk')
  return processedChunk
})
```

#### `new RetryTransform(options)`
Retry failed chunks with configurable backoff.

```javascript
import { RetryTransform } from './index.js'

const retryTransformer = new RetryTransform({
  maxRetries: 3,
  delay: 100,
  retryIf: (error) => error.code === 'EAGAIN'
})
```

## Real-World Examples

### File Processing Pipeline

```javascript
import { pipeline, JSONTransform, LineTransform, collect } from './index.js'
import { createReadStream, createWriteStream } from 'fs'

// Process JSON lines file, filter and transform
pipeline(
  createReadStream('input.jsonl'),
  new LineTransform(),
  new JSONTransform({ objectMode: true }),
  new Transform({
    objectMode: true,
    transform(data, encoding, callback) {
      // Filter and transform
      if (data.active && data.score > 100) {
        this.push({
          ...data,
          processed: true,
          score: data.score * 2
        })
      }
      callback()
    }
  }),
  createWriteStream('output.jsonl'),
  (error) => {
    if (error) console.error('Processing failed:', error)
    else console.log('Processing completed')
  }
)
```

### Stream to Database

```javascript
import { pipeline, JSONTransform, BatchTransform } from './index.js'

// Batch database inserts
pipeline(
  getDataStream(),
  new JSONTransform({ objectMode: true }),
  new BatchTransform({
    size: 100,
    delay: 1000,
    process: async (batch) => {
      await database.insertMany(batch)
      console.log(`Inserted ${batch.length} records`)
    }
  }),
  (error) => {
    if (error) console.error('Database import failed:', error)
    else console.log('Database import completed')
  }
)
```

### Error Recovery Pipeline

```javascript
import { pipeline, SafeTransform, RetryTransform } from './index.js'

pipeline(
  getUnreliableDataStream(),
  new RetryTransform({
    maxRetries: 5,
    backoff: 'exponential',
    transform: async (chunk) => {
      // Process unreliable data
      if (Math.random() < 0.1) {
        throw new Error('Random failure')
      }
      return processedChunk
    }
  }),
  new SafeTransform(async (chunk) => {
    // This transform won't crash the pipeline
    const result = await externalService.process(chunk)
    return { ...chunk, external: result }
  }),
  finalDestination,
  (error) => {
    console.log('Pipeline completed with:', error || 'no errors')
  }
)
```

## Installation

```bash
npm install stream-x
# or
yarn add stream-x
```

## Development

```bash
git clone https://github.com/sulthonzh/stream-x.git
cd stream-x
npm install
npm test
```

## Testing

```bash
npm test
```

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`npm test`)
6. Submit a pull request

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Related Projects

- [array-x](https://github.com/sulthonzh/array-x) - Zero-dependency array utilities
- [object-x](https://github.com/sulthonzh/object-x) - Zero-dependency object utilities
- [promise-x](https://github.com/sulthonzh/promise-x) - Zero-dependency promise utilities
- [function-x](https://github.com/sulthonzh/function-x) - Zero-dependency higher-order functions