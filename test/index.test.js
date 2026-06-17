import { test } from 'node:test'
import assert from 'node:assert'
import { Transform as NativeTransform } from 'stream'
import * as StreamX from '../index.js'
const { 
  Transform: StreamTransform,
  JSONTransform,
  LineTransform,
  mapStream,
  filterStream,
  pipeline,
  collect,
  waitFor,
  parallel,
  SafeTransform,
  RetryTransform,
  BatchTransform,
  throttleStream,
  splitStream,
  joinStream,
  debugStream
} = StreamX

// Basic Transform Tests
test('Transform stream works correctly', async () => {
  const transform = new StreamTransform({
    encoding: 'utf8',
    transform(chunk, encoding, callback) {
      this.push(chunk.toString().toUpperCase())
      callback()
    }
  })

  const input = ['hello', 'world']
  const result = await collect(createTestStream(input).pipe(transform))
  
  assert.deepEqual(result, ['HELLO', 'WORLD'])
})

test('JSONTransform handles parsing and stringifying', async () => {
  const jsonTransform = new JSONTransform({
    objectMode: true,
    transform(data, encoding, callback) {
      // Parse data if it's a Buffer, then add processed flag
      const dataString = (data instanceof Buffer || Buffer.isBuffer(data)) 
        ? data.toString() 
        : (typeof data === 'string' ? data : data)
      const parsedData = JSON.parse(dataString)
      this.push({ ...parsedData, processed: true })
      callback()
    }
  })

  const input = ['{"name": "test"}', '{"name": "test2"}']
  const testStream = createTestStream(input)
  
  const result = await collect(
    testStream
      .pipe(jsonTransform)
  )
  
  assert.deepEqual(result, [
    { name: 'test', processed: true },
    { name: 'test2', processed: true }
  ])
})

test('LineTransform processes lines correctly', async () => {
  const lineTransform = new LineTransform({
    transform(line, encoding, callback) {
      this.push(Buffer.from(line.toString().trim() + '\n'))
      callback()
    }
  })

  const input = ['line 1\n', 'line 2\n', 'line 3']
  const result = await collect(createTestStream(input).pipe(lineTransform))
  
  // Convert Buffers to strings for comparison
  const stringResult = result.map(buf => buf.toString())
  assert.deepEqual(stringResult, ['line 1\n', 'line 2\n', 'line 3\n'])
})

// Map and Filter Stream Tests
test('mapStream transforms stream items', async () => {
  const mapper = mapStream(item => item * 2)
  const result = await collect(createTestStream([1, 2, 3, 4]).pipe(mapper))
  
  assert.deepEqual(result, [2, 4, 6, 8])
})

test('filterStream filters stream items', async () => {
  const filter = filterStream(item => item % 2 === 0)
  const result = await collect(createTestStream([1, 2, 3, 4, 5]).pipe(filter))
  
  assert.deepEqual(result, [2, 4])
})

// Pipeline Tests
test('pipeline connects streams correctly', async () => {
  const transform1 = new StreamTransform({
    transform(chunk, encoding, callback) {
      this.push(chunk.toString() + '-transformed')
      callback()
    }
  })

  const transform2 = new StreamTransform({
    transform(chunk, encoding, callback) {
      this.push(chunk.toString() + '-final')
      callback()
    }
  })

  return new Promise((resolve, reject) => {
    pipeline(
      createTestStream(['hello', 'world']),
      transform1,
      transform2,
      createTestCollector(),
      (error) => {
        if (error) reject(error)
        else resolve()
      }
    )
  })
})

// Collect and Wait For Tests
test('collect collects all stream data', async () => {
  const result = await collect(createTestStream(['a', 'b', 'c']))
  assert.deepEqual(result, ['a', 'b', 'c'])
})

test('waitFor waits for stream completion', async () => {
  const testStream = createTestStream(['a', 'b', 'c'])
  await waitFor(testStream)
})

// Parallel Processing Tests
test('parallel processes streams with concurrency', async () => {
  const processor = parallel(
    createTestStream([1, 2, 3, 4, 5]),
    2,
    async (item) => {
      return item * 2
    }
  )
  
  const result = await collect(processor)
  assert.deepEqual(result, [2, 4, 6, 8, 10])
})

// Error Handling Tests
test('SafeTransform handles errors gracefully', async () => {
  const safeTransform = new SafeTransform(async (chunk) => {
    if (chunk === 'error') throw new Error('Test error')
    return chunk + '-processed'
  })

  const result = await collect(
    createTestStream(['ok', 'error', 'more']).pipe(safeTransform)
  )
  
  // Should have processed items and error chunks
  assert.ok(result.length >= 2)
})

test('RetryTransform retries failed operations', async () => {
  let attemptCount = 0
  const retryTransform = new RetryTransform({
    maxRetries: 3,
    transform: async (chunk) => {
      attemptCount++
      if (attemptCount < 3) throw new Error('Retry me')
      return chunk + '-success'
    }
  })

  const result = await collect(
    createTestStream(['test']).pipe(retryTransform)
  )
  
  assert.equal(result[0], 'test-success')
  assert.equal(attemptCount, 3)
})

// Batch Processing Tests
test('BatchTransform processes chunks in batches', async () => {
  const batchTransform = new BatchTransform({
    size: 2,
    process: async (batch) => {
      return `processed-${batch.join('-')}`
    }
  })

  const result = await collect(
    createTestStream(['a', 'b', 'c', 'd', 'e']).pipe(batchTransform)
  )
  
  assert.deepEqual(result, [
    'processed-a-b',
    'processed-c-d',
    'processed-e'
  ])
})

// Utility Function Tests
test('throttleStream limits emission rate', async () => {
  const start = Date.now()
  const throttle = throttleStream(2) // 2 items per second
  const result = await collect(createTestStream([1, 2, 3, 4]).pipe(throttle))
  const duration = Date.now() - start
  
  assert.deepEqual(result, [1, 2, 3, 4])
  assert.ok(duration >= 1500, `Should take at least 1.5s for 4 items at 2/sec, took ${duration}ms`)
})

test('splitStream splits by delimiter', async () => {
  const split = splitStream(',')
  const result = await collect(createTestStream(['a,b', 'c,d,e']).pipe(split))
  
  assert.deepEqual(result, ['a,', 'b,', 'c,', 'd,e'])
})

test('joinStream joins multiple streams', async () => {
  const stream1 = createTestStream(['a', 'b'])
  const stream2 = createTestStream(['c', 'd'])
  const join = joinStream([stream1, stream2], { separator: '-' })
  const result = await collect(join)
  
  assert.deepEqual(result, ['a', '-', 'b', '-', 'c', '-', 'd'])
})

test('debugStream logs stream events', async () => {
  // This test just ensures debugStream doesn't crash
  const debug = debugStream('test')
  const result = await collect(createTestStream(['a', 'b']).pipe(debug))
  
  assert.deepEqual(result, ['a', 'b'])
})

// Helper Functions
import { Readable, Writable } from 'stream'

function createTestStream(data) {
  return Readable.from(data)
}

function createTestCollector() {
  const chunks = []
  
  return new Writable({
    write(chunk, encoding, callback) {
      chunks.push(chunk)
      callback()
    },
    final(callback) {
      // Push all chunks for testing
      for (const chunk of chunks) {
        process.stdout.write(chunk)
      }
      callback()
    }
  })
}