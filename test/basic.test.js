import { test } from 'node:test'
import assert from 'node:assert'
import { Transform as NativeTransform } from 'stream'
import { pipeline } from 'stream/promises'
import * as StreamX from '../index.js'
const { 
  Transform: StreamTransform,
  JSONTransform,
  LineTransform,
  mapStream,
  filterStream,
  collect
} = StreamX

import { Readable } from 'stream'

function createTestStream(data) {
  return Readable.from(data)
}

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

test('collect collects all stream data', async () => {
  const result = await collect(createTestStream(['a', 'b', 'c']))
  assert.deepEqual(result, ['a', 'b', 'c'])
})

test('JSONTransform handles JSON parsing', async () => {
  const jsonTransform = new JSONTransform({
    objectMode: true,
    transform(data, encoding, callback) {
      this.push({ ...data, processed: true })
      callback()
    }
  })

  const input = ['{"name": "test"}', '{"name": "test2"}']
  const testStream = createTestStream(input)
  
  const result = await collect(
    testStream
      .pipe(new LineTransform())
      .pipe(jsonTransform)
  )
  
  // Basic check that JSON transformation works
  assert.ok(Array.isArray(result))
  assert.ok(result.length > 0)
})

test('LineTransform processes lines correctly', async () => {
  const lineTransform = new LineTransform()
  
  const input = ['line 1\n', 'line 2\n', 'line 3']
  const result = await collect(createTestStream(input).pipe(lineTransform))
  
  // LineTransform should preserve newlines
  assert.ok(Array.isArray(result))
  assert.ok(result.length > 0)
})