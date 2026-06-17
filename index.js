import { Transform as NativeTransform, PassThrough, pipeline as streamPipeline } from 'stream'

// Transform Utilities

/**
 * Base transform stream with simplified API
 */
export class Transform extends NativeTransform {
  constructor(options = {}) {
    super({ ...options })
    this._transform = options.transform || this._defaultTransform
  }

  _defaultTransform(chunk, encoding, callback) {
    this.push(chunk)
    callback()
  }
}

/**
 * JSON parsing/stringifying transform with error handling
 */
export class JSONTransform extends Transform {
  constructor(options = {}) {
    super({
      objectMode: options.objectMode !== undefined ? options.objectMode : true,
      ...options
    })
    this._stringify = options.stringify !== false
    
    // If a custom transform is provided, don't do automatic JSON parsing
    if (options.transform) {
      this._transform = options.transform
    }
  }

  _transform(chunk, encoding, callback) {
    try {
      // Convert Buffer to string if needed
      const data = (chunk instanceof Buffer || Buffer.isBuffer(chunk)) 
        ? JSON.parse(chunk.toString()) 
        : (typeof chunk === 'string' ? JSON.parse(chunk) : chunk)
      const result = this._stringify ? JSON.stringify(data) : data
      this.push(result)
      callback()
    } catch (error) {
      callback(error)
    }
  }
}

/**
 * Process text streams line by line
 */
export class LineTransform extends Transform {
  constructor(options = {}) {
    super({ 
      ...options,
      objectMode: false
    })
    this._remaining = ''
    this._separator = options.separator || '\n'
    this._transform = options.transform || this._defaultTransform
  }

  _defaultTransform(chunk, encoding, callback) {
    const data = this._remaining + chunk.toString()
    const lines = data.split(this._separator)
    this._remaining = lines.pop() || ''

    for (const line of lines) {
      // Push as Buffer when in objectMode: false
      this.push(Buffer.from(line + this._separator))
    }
    callback()
  }

  _flush(callback) {
    if (this._remaining) {
      this.push(Buffer.from(this._remaining))
      this._remaining = ''
    }
    callback()
  }
}

/**
 * Stream equivalent of Array.prototype.map
 */
export function mapStream(transformFn, options = {}) {
  return new Transform({
    objectMode: true,
    ...options,
    transform(chunk, encoding, callback) {
      try {
        const result = transformFn(chunk, encoding)
        this.push(result)
        callback()
      } catch (error) {
        callback(error)
      }
    }
  })
}

/**
 * Stream equivalent of Array.prototype.filter
 */
export function filterStream(predicateFn, options = {}) {
  return new Transform({
    objectMode: true,
    ...options,
    transform(chunk, encoding, callback) {
      try {
        if (predicateFn(chunk, encoding)) {
          this.push(chunk)
        }
        callback()
      } catch (error) {
        callback(error)
      }
    }
  })
}

// Pipeline Utilities

/**
 * Enhanced pipeline with better error handling and cleanup
 */
export function pipeline(...streams) {
  const callback = typeof streams[streams.length - 1] === 'function' 
    ? streams.pop() 
    : () => {}

  if (streams.length < 2) {
    throw new Error('Pipeline requires at least 2 streams')
  }

  let errorOccurred = false
  const cleanupStreams = []

  const handleError = (error, streamIndex) => {
    if (errorOccurred) return
    errorOccurred = true

    // Close all streams that were successfully opened
    for (let i = streamIndex; i < cleanupStreams.length; i++) {
      if (cleanupStreams[i] && !cleanupStreams[i].destroyed) {
        cleanupStreams[i].destroy()
      }
    }

    callback(error)
  }

  // Track which streams are open for cleanup
  streams.forEach((stream, index) => {
    if (index === 0) {
      // First stream (readable)
      cleanupStreams[0] = stream
      stream.on('error', (error) => handleError(error, 0))
    } else if (index === streams.length - 1) {
      // Last stream (writable)
      cleanupStreams[index] = stream
      stream.on('error', (error) => handleError(error, index))
      stream.on('finish', () => callback(null))
    } else {
      // Transform streams
      cleanupStreams[index] = stream
      stream.on('error', (error) => handleError(error, index))
    }
  })

  // Connect streams
  for (let i = 0; i < streams.length - 1; i++) {
    streams[i].pipe(streams[i + 1], { end: false })
  }

  // Ensure the last stream gets the end event
  if (streams.length > 0) {
    streams[streams.length - 1].on('end', () => {
      if (!errorOccurred) {
        callback(null)
      }
    })
  }
}

/**
 * Collect all data from a stream into an array/buffer
 */
export function collect(stream, options = {}) {
  return new Promise((resolve, reject) => {
    const chunks = []
    let size = 0

    stream.on('data', (chunk) => {
      chunks.push(chunk)
      size += chunk.length
    })

    stream.on('error', reject)
    stream.on('end', () => {
      if (options.encoding === 'buffer') {
        resolve(Buffer.concat(chunks, size))
      } else if (options.encoding === 'string') {
        resolve(chunks.join(''))
      } else {
        resolve(chunks)
      }
    })
  })
}

/**
 * Convert a stream to a Promise that resolves when the stream ends
 */
export function waitFor(stream, options = {}) {
  return new Promise((resolve, reject) => {
    const timeout = options.timeout
    const timeoutId = timeout ? setTimeout(() => {
      reject(new Error(`Stream timeout after ${timeout}ms`))
    }, timeout) : null

    stream.on('error', (error) => {
      if (timeoutId) clearTimeout(timeoutId)
      reject(error)
    })

    stream.on('end', () => {
      if (timeoutId) clearTimeout(timeoutId)
      resolve()
    })

    stream.on('close', () => {
      if (timeoutId) clearTimeout(timeoutId)
      // Don't resolve here, 'end' should have been emitted first
    })
  })
}

/**
 * Process streams in parallel with concurrency limit
 */
export function parallel(streams, concurrency = 1, processFn) {
  const Transform = require('stream').Transform
  const results = []
  let completed = 0
  let error = null

  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      if (error) {
        callback(error)
        return
      }

      if (completed < concurrency) {
        completed++
        Promise.resolve(processFn(chunk))
          .then(result => {
            results.push(result)
            this.push(result)
            completed--
            callback()
          })
          .catch(err => {
            error = err
            callback(err)
          })
      } else {
        // Need to wait for a slot to open up
        const processChunk = () => {
          Promise.resolve(processFn(chunk))
            .then(result => {
              results.push(result)
              this.push(result)
              callback()
            })
            .catch(err => {
              error = err
              callback(err)
            })
        }

        // Store the chunk for later processing
        if (!this._queue) this._queue = []
        this._queue.push({ chunk, process: processChunk })
        
        // If we have a queue, we'll process when slots become available
        if (this._queue.length === 1) {
          this._processQueue()
        }
      }
    },

    _flush(callback) {
      if (this._queue && this._queue.length > 0) {
        // Process any remaining items in the queue
        const processRemaining = () => {
          if (this._queue.length === 0) {
            callback()
          } else {
            const { chunk, process } = this._queue.shift()
            process()
          }
        }
        
        const checkQueue = () => {
          if (completed < concurrency && this._queue.length > 0) {
            const { chunk, process } = this._queue.shift()
            completed++
            process()
          } else if (this._queue.length > 0) {
            setTimeout(checkQueue, 0)
          } else {
            callback()
          }
        }
        
        checkQueue()
      } else {
        callback()
      }
    }
  })
}

// Error Handling

/**
 * Wrap a transform function to prevent pipeline crashes
 */
export class SafeTransform extends Transform {
  constructor(transformFn, options = {}) {
    super({
      objectMode: true,
      ...options,
      transform(chunk, encoding, callback) {
        Promise.resolve()
          .then(() => transformFn(chunk, encoding))
          .then(result => {
            if (result !== undefined) {
              this.push(result)
            }
            callback()
          })
          .catch(error => {
            // Push the error as a special chunk for handling
            this.push({ __error: error })
            callback()
          })
      }
    })
  }
}

/**
 * Retry failed chunks with configurable backoff
 */
export class RetryTransform extends Transform {
  constructor(options = {}) {
    super({
      objectMode: true,
      ...options,
      _retries: new Map(),
      _maxRetries: options.maxRetries || 3,
      _delay: options.delay || 100,
      _backoff: options.backoff || 'constant',
      _retryIf: options.retryIf || (() => true)
    })

    // Bind methods
    this._transform = this._transform.bind(this)
    this._flush = this._flush.bind(this)
  }

  _transform(chunk, encoding, callback) {
    const chunkKey = this._getChunkKey(chunk)
    
    if (this._retries.has(chunkKey)) {
      const retryCount = this._retries.get(chunkKey)
      
      if (retryCount >= this._maxRetries) {
        // Max retries exceeded, push the chunk as is
        this.push(chunk)
        this._retries.delete(chunkKey)
        callback()
        return
      }

      // Retry with backoff
      const delay = this._calculateBackoff(retryCount)
      setTimeout(() => {
        this._processChunk(chunk, encoding, callback)
      }, delay)
      return
    }

    // First attempt
    this._processChunk(chunk, encoding, callback)
  }

  _processChunk(chunk, encoding, callback) {
    const chunkKey = this._getChunkKey(chunk)
    const retryCount = (this._retries.get(chunkKey) || 0) + 1
    
    try {
      const result = this._transformChunk(chunk, encoding)
      if (result !== undefined) {
        this.push(result)
      }
      this._retries.delete(chunkKey)
      callback()
    } catch (error) {
      if (this._retryIf(error) && retryCount <= this._maxRetries) {
        this._retries.set(chunkKey, retryCount)
        setTimeout(() => {
          this._transform(chunk, encoding, callback)
        }, this._calculateBackoff(retryCount - 1))
      } else {
        this.push(chunk) // Push original chunk if not retrying
        this._retries.delete(chunkKey)
        callback(error)
      }
    }
  }

  _getChunkKey(chunk) {
    // Simple hash for chunk identification
    return typeof chunk === 'string' ? chunk : JSON.stringify(chunk)
  }

  _calculateBackoff(retryCount) {
    switch (this._backoff) {
      case 'linear':
        return this._delay * (retryCount + 1)
      case 'exponential':
        return this._delay * Math.pow(2, retryCount)
      case 'full jitter':
        return Math.random() * this._delay * Math.pow(2, retryCount)
      default:
        return this._delay
    }
  }

  _transformChunk(chunk, encoding) {
    // Override in subclass or pass as option
    return chunk
  }

  _flush(callback) {
    // Wait for any pending retries to complete
    if (this._retries.size > 0) {
      const checkRetries = () => {
        if (this._retries.size === 0) {
          callback()
        } else {
          setTimeout(checkRetries, this._delay)
        }
      }
      checkRetries()
    } else {
      callback()
    }
  }
}

// Batch Processing

/**
 * Collect chunks and process in batches
 */
export class BatchTransform extends Transform {
  constructor(options = {}) {
    super({
      objectMode: true,
      ...options
    })
    this._size = options.size || 100
    this._delay = options.delay || 0
    this._batch = []
    this._timeoutId = null
    this._processFn = options.process || this._defaultProcess
  }

  _transform(chunk, encoding, callback) {
    this._batch.push(chunk)
    
    if (this._batch.length >= this._size) {
      this._processBatch()
    } else if (this._delay > 0 && !this._timeoutId) {
      this._timeoutId = setTimeout(() => {
        this._processBatch()
      }, this._delay)
    }
    
    callback()
  }

  _flush(callback) {
    if (this._batch.length > 0) {
      this._processBatch()
    }
    callback()
  }

  _processBatch() {
    if (this._timeoutId) {
      clearTimeout(this._timeoutId)
      this._timeoutId = null
    }

    const batch = [...this._batch]
    this._batch = []

    Promise.resolve(this._processFn(batch))
      .then(result => {
        if (result !== undefined) {
          this.push(result)
        }
      })
      .catch(error => {
        // Push batch as individual items on error
        for (const item of batch) {
          this.push(item)
        }
      })
  }

  _defaultProcess(batch) {
    return batch
  }
}

// Utility Functions

/**
 * Throttle stream emissions
 */
export function throttleStream(rate, options = {}) {
  const Transform = require('stream').Transform
  let lastEmit = 0
  const queue = []

  return new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      queue.push({ chunk, callback })
      this._processQueue()
    },

    _processQueue() {
      const now = Date.now()
      const timeSinceLastEmit = now - lastEmit
      
      if (timeSinceLastEmit >= 1000 / rate && queue.length > 0) {
        const { chunk, callback } = queue.shift()
        this.push(chunk)
        lastEmit = now
        callback()
        
        // Process next item
        if (queue.length > 0) {
          setTimeout(() => this._processQueue(), 1000 / rate - timeSinceLastEmit)
        }
      }
    }
  })
}

/**
 * Split a stream by delimiter
 */
export function splitStream(delimiter, options = {}) {
  const Transform = require('stream').Transform
  const buffer = []
  let remaining = ''

  return new Transform({
    objectMode: false,
    transform(chunk, encoding, callback) {
      remaining += chunk.toString()
      const parts = remaining.split(delimiter)
      
      // Keep the last part (which might be incomplete)
      remaining = parts.pop()
      
      for (const part of parts) {
        this.push(part + delimiter)
      }
      callback()
    },

    flush(callback) {
      if (remaining) {
        this.push(remaining)
      }
      callback()
    }
  })
}

/**
 * Join multiple streams together
 */
export function joinStream(streams, options = {}) {
  const Transform = require('stream').Transform
  const separator = options.separator || ''
  let currentStream = 0

  return new Transform({
    objectMode: false,
    transform(chunk, encoding, callback) {
      this.push(chunk)
      callback()
    },

    flush(callback) {
      const processNextStream = () => {
        if (currentStream < streams.length) {
          const stream = streams[currentStream++]
          
          stream.on('data', (chunk) => {
            if (separator && currentStream > 1) {
              this.push(separator)
            }
            this.push(chunk)
          })

          stream.on('end', processNextStream)
          stream.on('error', callback)
        } else {
          callback()
        }
      }

      processNextStream()
    }
  })
}

/**
 * Debug stream events and data flow
 */
export function debugStream(name, options = {}) {
  const Transform = require('stream').Transform
  const events = options.events || ['data', 'end', 'error', 'close']
  let chunkCount = 0
  let byteCount = 0

  return new Transform({
    objectMode: options.objectMode !== false,
    transform(chunk, encoding, callback) {
      chunkCount++
      if (!options.objectMode) {
        byteCount += chunk.length
      }

      for (const event of events) {
        if (event === 'data') {
          console.log(`[${name}] Data chunk #${chunkCount}:`, 
            options.objectMode ? chunk : `${chunk.length} bytes`)
        }
      }

      this.push(chunk)
      callback()
    },

    flush(callback) {
      console.log(`[${name}] Stream ended. Total: ${chunkCount} chunks${!options.objectMode ? `, ${byteCount} bytes` : ''}`)
      callback()
    }
  })
}

// Export all utilities for backward compatibility
export default {
  Transform,
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
}