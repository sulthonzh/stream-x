#!/usr/bin/env node

import { createReadStream, createWriteStream } from 'fs'
import { createInterface } from 'readline'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import {
  JSONTransform,
  LineTransform,
  mapStream,
  filterStream,
  collect,
  BatchTransform,
  debugStream,
  splitStream,
  joinStream,
  throttleStream
} from './index.js'
import { fileURLToPath } from 'url'
import { dirname, join } from 'path'

const __dirname = dirname(fileURLToPath(import.meta.url))

const commands = {
  json: {
    description: 'Process JSON data with transformations',
    usage: 'json [--map <function>] [--filter <function>]',
    run: async (args) => {
      const options = parseArgs(args)
      const mapFn = options.map ? eval(`(${options.map})`) : null
      const filterFn = options.filter ? eval(`(${options.filter})`) : null

      await pipeline(
        process.stdin,
        new JSONTransform({ objectMode: true }),
        new LineTransform(),
        filterStream ? (filterFn ? filterStream(filterFn) : new LineTransform()) : new LineTransform(),
        mapStream ? (mapFn ? mapStream(mapFn) : new LineTransform()) : new LineTransform(),
        process.stdout
      )
    }
  },

  lines: {
    description: 'Process text lines',
    usage: 'lines [--map <function>] [--filter <function>]',
    run: async (args) => {
      const options = parseArgs(args)
      const mapFn = options.map ? eval(`(${options.map})`) : null
      const filterFn = options.filter ? eval(`(${options.filter})`) : null

      await pipeline(
        process.stdin,
        new LineTransform(),
        filterFn ? filterStream(filterFn) : new LineTransform(),
        mapFn ? mapStream(mapFn) : new LineTransform(),
        process.stdout
      )
    }
  },

  count: {
    description: 'Count items/bytes in stream',
    usage: 'count [--lines] [--words] [--bytes]',
    run: async (args) => {
      const options = parseArgs(args)
      let count = 0
      let byteCount = 0
      let wordCount = 0
      let inWord = false

      await pipeline(
        process.stdin,
        new Transform({
          transform(chunk, encoding, callback) {
            const data = chunk.toString()
            
            if (options.bytes) {
              byteCount += data.length
            }
            
            if (options.lines) {
              const lines = data.split('\n')
              count += lines.length - 1
              if (data.endsWith('\n')) count++
            }
            
            if (options.words) {
              for (const char of data) {
                if (/[a-zA-Z0-9]/.test(char)) {
                  if (!inWord) {
                    wordCount++
                    inWord = true
                  }
                } else {
                  inWord = false
                }
              }
            }
            
            callback()
          },
          flush(callback) {
            if (options.lines) count++
            console.log(`Count: ${count}`)
            if (options.words) console.log(`Words: ${wordCount}`)
            if (options.bytes) console.log(`Bytes: ${byteCount}`)
            callback()
          }
        })
      )
    }
  },

  batch: {
    description: 'Process data in batches',
    usage: 'batch --size <size> --process <function>',
    run: async (args) => {
      const options = parseArgs(args)
      if (!options.size || !options.process) {
        console.error('Error: --size and --process are required')
        process.exit(1)
      }

      const batchSize = parseInt(options.size)
      const processFn = eval(`(${options.process})`)

      const batchTransform = new BatchTransform({
        size: batchSize,
        process: async (batch) => {
          const result = await processFn(batch)
          console.log(JSON.stringify(result))
        }
      })

      await pipeline(
        process.stdin,
        new JSONTransform({ objectMode: true }),
        new LineTransform(),
        batchTransform,
        process.stdout
      )
    }
  },

  debug: {
    description: 'Debug stream events and data flow',
    usage: 'debug [--events <events>]',
    run: async (args) => {
      const options = parseArgs(args)
      const events = options.events ? options.events.split(',') : ['data', 'end', 'error']

      await pipeline(
        process.stdin,
        debugStream('cli-debug', { events }),
        process.stdout
      )
    }
  },

  split: {
    description: 'Split stream by delimiter',
    usage: 'split --delimiter <delimiter>',
    run: async (args) => {
      const options = parseArgs(args)
      if (!options.delimiter) {
        console.error('Error: --delimiter is required')
        process.exit(1)
      }

      await pipeline(
        process.stdin,
        splitStream(options.delimiter),
        process.stdout
      )
    }
  },

  throttle: {
    description: 'Throttle stream emissions',
    usage: 'throttle --rate <rate>',
    run: async (args) => {
      const options = parseArgs(args)
      if (!options.rate) {
        console.error('Error: --rate is required')
        process.exit(1)
      }

      const rate = parseInt(options.rate)
      await pipeline(
        process.stdin,
        throttleStream(rate),
        process.stdout
      )
    }
  },

  demo: {
    description: 'Run demonstration of stream-x features',
    usage: 'demo',
    run: async () => {
      console.log('🚀 stream-x Demo\n')

      console.log('1. Basic JSON transformation:')
      await demoJSONTransform()

      console.log('\n2. Stream processing with map/filter:')
      await demoMapFilter()

      console.log('\n3. Batch processing:')
      await demoBatch()

      console.log('\n4. Error handling with SafeTransform:')
      await demoErrorHandling()

      console.log('\n5. Pipeline composition:')
      await demoPipeline()
    }
  }
}

function parseArgs(args) {
  const result = {}
  
  for (let i = 0; i < args.length; i++) {
    const arg = args[i]
    if (arg.startsWith('--')) {
      const key = arg.slice(2)
      if (i + 1 < args.length && !args[i + 1].startsWith('--')) {
        result[key] = args[i + 1]
        i++
      } else {
        result[key] = true
      }
    }
  }
  
  return result
}

async function demoJSONTransform() {
  console.log('Transforming JSON data...')
  const testData = [
    { name: 'Alice', age: 25 },
    { name: 'Bob', age: 30 },
    { name: 'Charlie', age: 35 }
  ]

  const transform = new JSONTransform({
    objectMode: true,
    transform(data, callback) {
      this.push({
        ...data,
        age: data.age + 1,
        processed: true
      })
      callback()
    }
  })

  await pipeline(
    Readable.from(testData.map(d => JSON.stringify(d))),
    new LineTransform(),
    transform,
    new JSONTransform({ stringify: true }),
    process.stdout
  )
}

async function demoMapFilter() {
  console.log('Processing stream with map and filter...')
  
  const pipeline = async () => {
    await pipeline(
      Readable.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
      filterStream(x => x % 2 === 0),
      mapStream(x => x * 2),
      process.stdout
    )
  }

  await pipeline()
}

async function demoBatch() {
  console.log('Processing in batches of 3...')
  
  const batchTransform = new BatchTransform({
    size: 3,
    process: async (batch) => {
      const sum = batch.reduce((a, b) => a + b, 0)
      return `Batch sum: ${sum}`
    }
  })

  await pipeline(
    Readable.from([1, 2, 3, 4, 5, 6, 7, 8, 9]),
    batchTransform,
    process.stdout
  )
}

async function demoErrorHandling() {
  console.log('Demonstrating error handling...')
  
  const safeTransform = new SafeTransform(async (chunk) => {
    if (chunk === 'error') {
      throw new Error('This is a test error')
    }
    return chunk + '-processed'
  })

  await pipeline(
    Readable.from(['ok', 'error', 'more']),
    safeTransform,
    process.stdout
  )
}

async function demoPipeline() {
  console.log('Composing a complex pipeline...')
  
  await pipeline(
    Readable.from(['hello world', 'foo bar', 'baz qux']),
    new LineTransform(),
    mapStream(line => line.toUpperCase()),
    filterStream(line => line.includes('O')),
    process.stdout
  )
}

// Main execution
async function main() {
  const args = process.argv.slice(2)
  
  if (args.length === 0 || args[0] === '--help' || args[0] === '-h') {
    showHelp()
    return
  }

  const command = args[0]
  const commandConfig = commands[command]

  if (!commandConfig) {
    console.error(`Error: Unknown command '${command}'`)
    showHelp()
    process.exit(1)
  }

  try {
    await commandConfig.run(args.slice(1))
  } catch (error) {
    console.error(`Error: ${error.message}`)
    process.exit(1)
  }
}

function showHelp() {
  console.log('stream-x - Zero-dependency Node.js stream utilities CLI')
  console.log('\nUsage: stream-x <command> [options]')
  console.log('\nCommands:')
  
  for (const [name, config] of Object.entries(commands)) {
    console.log(`  ${name.padEnd(12)} ${config.description}`)
    console.log(`    ${config.usage}`)
    console.log()
  }
  
  console.log('Examples:')
  console.log('  echo \'{"name": "test"}\' | stream-x json --map \'(d) => ({...d, upper: d.name.toUpperCase()})\'')
  console.log('  cat data.txt | stream-x lines --filter \'(line) => line.length > 0\'')
  console.log('  echo "a,b,c,d" | stream-x split --delimiter ","')
  console.log('  stream-x demo')
}

// Export commands for testing
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
}

export { commands }