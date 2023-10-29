import BandwidthThrottle from './BandwidthThrottle'
import { BaseTransformStream } from './Platform'

/**
 * Pushes one or more chunks to a stream stream while handling backpressure.
 * 
 * @example
 * [1, 2, 3, 4, 5, 6, 7, 8, 9 , 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
 * pushWithBackpressure(stream, chunks, () => ...)
 * pushWithBackpressure(stream, ['hi', 'hello'], 'utf8', () => ...)
 * @param {Duplex} stream The Duplex stream the chunks will be pushed to
 * @param {any} chunks The chunk or array of chunks to push to a stream stream
 * @param {string|Function} [encoding] The encoding of each string chunk or callback function
 * @param {Function} [callback] Callback function called after all chunks have been pushed to the stream
 * @retrn {Duplex}
 */
const pushWithBackpressure = (stream: BandwidthThrottle|BaseTransformStream, chunks: Uint8Array[], encoding: string|undefined, callback: Function|undefined, { $index = 0 } = {}) => {
    if (!(stream instanceof BandwidthThrottle) && !(stream instanceof BaseTransformStream)) {
      throw new TypeError('Argument "stream" must be an instance of BandwidthThrottle or BaseTransformStream. Got ' + typeof stream + ' instead.')
    }
    chunks = [...chunks].filter(x => x !== undefined)
    if (typeof encoding === 'function') {
      callback = encoding
      encoding = undefined
    }
    if ($index >= chunks.length) {
      if (typeof callback === 'function') {
        callback()
      }
      return stream
    } else if (!stream.push(chunks[$index], ...([encoding].filter(Boolean)))) {
      console.error('BACKPRESSURE', $index)
      return stream.once('drain', () => {
        console.error('DRAIN')
        pushWithBackpressure(stream, chunks, encoding, callback, { $index: $index + 1 })
      })
    }
    return pushWithBackpressure(stream, chunks, encoding, callback, { $index: $index + 1 })
  }

  export default pushWithBackpressure;