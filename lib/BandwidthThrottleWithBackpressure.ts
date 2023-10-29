import { resolve } from "https://deno.land/std@0.200.0/path/resolve.ts";
import Config from './Config.ts';
import CallbackWithSelf from './Types/CallbackWithSelf.ts';
import deferred from './Util/deferred.ts';


/**
 * Clamps a number between a minimum and maximum value.
 * @param number The number to clamp.
 * @param min The minimum value.
 * @param max The maximum value.
 */
const ClampNum = (number:number, min:number, max:number) => {
    return Math.max(min, Math.min(number, max));
}

/**
 * A duplex stream transformer implementation, extending Node's built-in
 * `Transform` class. Receives input via a writable stream from a data
 * buffer (e.g. an HTTP request), and throttles output to a defined maximum
 * number of bytes per a defined interval.
 *
 * Configuration is received from a parent `BandwidthThrottleGroup` instance,
 * ensuring that available bandwidth is distributed evenly between all streams within
 * the group, mimicing the behaviour of overlapping network requests.
 */

class BandwidthThrottleWithBackpressure extends TransformStream<any, any> {
    private controller: TransformStreamDefaultController<
    Uint8Array
> | null = null;
    
    /**
     * A callback to be invoked when bytes are written
     * to the underlying `readable` stream. Used as a hook
     * for testing to confirm output rate.
     */

    public onBytesWritten: ((chunk: Uint8Array) => void) | null = null;

    private pendingBytesBuffer: Uint8Array | null;
    private pendingBytesCount = 0;
    private pendingBytesReadIndex = 0;
    private pendingBytesBufferExpandedFactor = 1;
    private pendingBytesBufferExpandedSize: number = 0;
    private config: Readonly<Config>;
    private isDataBeingWritten = false;
    private handleRequestStart: CallbackWithSelf;
    private handleRequestStop: CallbackWithSelf;
    private handleRequestDestroy: CallbackWithSelf;
    private done = deferred<void>();
    private chunkCounter: number = 0;
    public resolver_backpressure: Function | undefined = undefined;
    

    constructor(
        /**
         * An object of configuration values provided by the
         * parent group.
         */

        config: Readonly<Config>,

        /**
         * The total number of bytes in the request to be throttled, to be used to define memory
         * allocation.
         */

        contentLength: number,

        /**
         * A handler to be invoked whenever a request starts processing data,
         * so that the parent group can increment of the total number of
         * requests in flight across the group.
         */

        handleRequestStart: CallbackWithSelf,

        /**
         * A handler to be invoked whenever a request stops processing
         * data, so that the parent group can decrement of the total
         * number of requests in flight across the group.
         */

        handleRequestEnd: CallbackWithSelf,

        /**
         * A handler to be invoked when a request has finished processing all
         * data for a request, and the throttle is no longer needed.
         */

        handleRequestDestroy: CallbackWithSelf,
    ) {
        super({
            transform: (chunk: Uint8Array,controller:TransformStreamDefaultController) => {
                this.controller = controller;
                console.log("BandwidthThrottleWithBackpressure.ts: transform called");
                this.chunkCounter+=1;
               // console.log("BandwidthThrottle.ts: chunkCounter: ",this.chunkCounter);
               
                return new Promise((resolver_backpressure,rejecter_backpressure) => {
                    this.resolver_backpressure = resolver_backpressure;
                    console.log("BandwidthThrottleWithBackpressure.ts: transform promise function called. chunk#",this.chunkCounter);
                    if(this.chunkCounter <= 1000) {
                        setTimeout(() => {
                            this.transform(chunk);
                        }, 1000);
                    }
                });
            },
            flush: () => this.flush()
        });

        this.config = config;
        this.pendingBytesBufferExpandedSize = this.config.bytesPerSecond * this.pendingBytesBufferExpandedFactor;
        this.pendingBytesBuffer = new Uint8Array(this.pendingBytesBufferExpandedSize);
        this.handleRequestStart = handleRequestStart;
        this.handleRequestStop = () => {
            //console.log("handleRequestStop called from BandwidthThrottle.ts")
            return handleRequestEnd(this);
        };
        this.handleRequestDestroy = handleRequestDestroy;
        this.resolver_backpressure = undefined;
    }

    /**
     * To be called when the request being throttled is aborted in
     * order to rebalance the available bandwidth.
     */

    public abort(): void {
        this.handleRequestStop(this);
        this.destroy();
    }

    protected push(chunk: Uint8Array,backpressure_resolver?:Function): void {
        this.controller!.enqueue(chunk);
        if( backpressure_resolver ) {
            backpressure_resolver();
        }
        else
            console.log("no backpressure resolver!")
    }

    public trim_buffer = ():Promise<unknown> => {
        return new Promise((resolve) => {
            console.log("trim_buffer called");
            console.log("before trim_buffer: pending Bytes Count: ",this.pendingBytesCount);
            console.log("before trim_buffer: pending Bytes Read Index: ",this.pendingBytesReadIndex);
            const transferredBuffer = this.pendingBytesBuffer?.subarray(this.pendingBytesReadIndex,this.pendingBytesCount)??new Uint8Array();
            this.pendingBytesBuffer = new Uint8Array(this.pendingBytesBufferExpandedSize);
            this.pendingBytesBuffer.set(transferredBuffer);
            this.pendingBytesReadIndex = 0;
            this.pendingBytesCount = transferredBuffer.length;
            console.log("after trim_buffer: pending Bytes Count: ",this.pendingBytesCount);
            console.log("after trim_buffer: pending Bytes Read Index: ",this.pendingBytesReadIndex);
            resolve(true);
        });
    }

    /**
     * Extracts a number of bytes from the pending bytes queue and
     * pushes it out to a piped writable stream.
     *
     * @returns The number of bytes processed through the throttle
     */

    public async process(maxBytesToProcess: number|undefined = Infinity): number {
        //console.log("process call: maxBytesToProcess: ",maxBytesToProcess, "this.pendingBytesCount: ",this.pendingBytesCount);
        const startReadIndex = this.pendingBytesReadIndex;

        const endReadIndex = Math.min(
            this.pendingBytesReadIndex +  Math.round(maxBytesToProcess),
            this.pendingBytesCount
        );

        const bytesToPushLength = endReadIndex - startReadIndex;

        if (bytesToPushLength > 0) {
            const bytesToPush = this.pendingBytesBuffer.subarray(
                startReadIndex,
                endReadIndex
            );

            this.pendingBytesReadIndex = endReadIndex;

            console.log("calling trim from process")
            await this.trim_buffer();
            const formerresolver = this.resolver_backpressure;
            this.resolver_backpressure = undefined;
            this.push(bytesToPush,formerresolver);

            if (typeof this.onBytesWritten === 'function') {
                this.onBytesWritten(bytesToPush);
            }
        }

        // Do not complete if:
        // - additional data is available to be processed,
        // - or, no additional data is available, but not all data has been written yet
        // - or, we are unthrottled

        if (
            this.pendingBytesReadIndex < this.pendingBytesCount ||
            this.isDataBeingWritten ||
            !this.config.isThrottled
        )
            return bytesToPushLength;

        // End the request

        this.done.resolve();

        this.handleRequestStop(this);
        this.destroy();

        return bytesToPushLength;
    }

    /**
         * To be called when a properly throttled write stream is needed.
         * The write stream will call the throttle's transform method
         * and wait for the promise to resolve before processing the next chunk.
         * @returns WritableStream<any>
    */

    public throttleInputValve = (): WritableStream<any> => {
        let WriteStreamChunkNumber = 0;
        const ThrottledStreamWriter = this.writable.getWriter();

        const WriteStream = new WritableStream ({
            write(chunk,controller) {
                return new Promise(async (resolve_write,reject_write) => {
                    console.log("writing chunk #",WriteStreamChunkNumber);
                    WriteStreamChunkNumber+=1;
                    const thisChunkNumber = WriteStreamChunkNumber;

                    const returnedpromise = await ThrottledStreamWriter.write(chunk);
                    console.log("returnedpromise:",returnedpromise)
                    console.log("chunk #",thisChunkNumber,"written. resolving write.")
                    //resolve_write();
                    reject_write();
                });
            },
            abort(controller) {
                WriteStream.abort();
            }
        },{
            highWaterMark: 64,
            size(chunk) {
                return chunk.length;
            }
        });

        return WriteStream;
    }

    /**
     * To be called when the request being throttled is aborted in
     * order to rebalance the available bandwidth. Resolves a promise
     */

    public gracefulAbort(): void {
        this.done.resolve()
        this.handleRequestStop(this);
        this.destroy();
    }

    /**
     * Informs the parent group that the throttle is no longer needed and can
     * be released. Once a throttle is destroyed, it can not be used again.
     */

    public destroy(): void {
        this.handleRequestDestroy(this);
        this.controller?.terminate();
    }

    /**
     * Invoked internally whenever data is received from the underlying
     * writeable stream. Resolves a promise when done.
     *
     * @param chunk A chunk of data in the form of a typed array of arbitrary length.
     */

    private async transform(chunk: Uint8Array): Promise<void> {
        if (!this.isDataBeingWritten) {
            // If this is the first chunk of data to be processed, or
            // if is processing was previously paused due to a lack of
            // input signal that the request is in flight.

            this.handleRequestStart(this);

            this.isDataBeingWritten = true;
        }

        if((chunk.length + this.pendingBytesCount) > this.pendingBytesBufferExpandedSize){
            const remainingContentInBufferLength = (this.pendingBytesCount - this.pendingBytesReadIndex);
            if(chunk.length + remainingContentInBufferLength <= this.config.maxBufferSize) {
                if( remainingContentInBufferLength + chunk.length > this.pendingBytesBufferExpandedSize) {
                    console.log("bufferlengthcheck: maxbuffer not reached, first if");
                    this.pendingBytesBufferExpandedFactor = Math.ceil((chunk.length + remainingContentInBufferLength)/this.config.bytesPerSecond);
                    if((this.config.bytesPerSecond*this.pendingBytesBufferExpandedFactor) > this.config.maxBufferSize){
                        this.pendingBytesBufferExpandedSize = this.config.maxBufferSize;
                    } else {
                        this.pendingBytesBufferExpandedSize = this.config.bytesPerSecond*this.pendingBytesBufferExpandedFactor;
                    }
                    console.log("Expanding buffer to " + this.pendingBytesBufferExpandedSize/1024/1024+" MB.");
                }
                await this.trim_buffer();
            } else {
                console.log("maxBufferSize reached. Aborting request as output is too slow or maxBufferSize is too small.");
                this.handleRequestDestroy(this);
                return;
            }
            console.log("bufferlengthcheck: this.pendingBytesCount",this.pendingBytesCount);
            console.log("bufferlengthcheck: this.pendingBytesReadIndex",this.pendingBytesReadIndex);
            console.log("bufferlengthcheck: this.pendingBytesBufferExpandedSize",this.pendingBytesBufferExpandedSize);
            console.log("bufferlengthcheck: chunk.length",chunk.length);
            this.pendingBytesBuffer.set(chunk, this.pendingBytesCount);
            this.pendingBytesCount += chunk.length;
        } else {
            this.pendingBytesBuffer?.set(chunk, this.pendingBytesCount);
            this.pendingBytesCount += chunk.length;
        }
       
        // If no throttling is applied, avoid any initial latency by immediately
        // processing the queue on the next frame.

        if (!this.config.isThrottled) this.process(undefined);
    }

    /**
     * Invoked once all data has been passed to the stream, and resolving a promise
     * when all data has been processed.
     */

    private async flush(): Promise<void> {
        // If an empty request was passed through the throttle, end immediately

        this.isDataBeingWritten = false;

        if (this.pendingBytesCount === 0) return;

        if (!this.config.isThrottled) {
            // If the throttle is unbounded, then all data has been
            // processed and request can be completed

            this.handleRequestStop(this);
            this.destroy();

            return;
        }

        // Else, wait for the processing cycle to compelte the request

        return this.done;
    }
}

export default BandwidthThrottleWithBackpressure;