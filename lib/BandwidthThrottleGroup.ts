import BandwidthThrottle from './BandwidthThrottle.ts';
import BandwidthThrottleWithBackpressure from './BandwidthThrottleWithBackpressure.ts';
import Config from './Config.ts';
import IConfig from './Interfaces/IConfig.ts';
import IThroughputData from './Interfaces/IThroughputData.ts';
import {setInterval, Timeout} from './Platform/mod.ts';
import getPartitionedIntegerPartAtIndex from './Util/getPartitionedIntegerPartAtIndex.ts';

/**
 * A class used to configure and bridge between one or more
 * `BandwidthThrottle` instances, ensuring that the defined
 * available bandwidth is distributed as equally as possible
 * between any simultaneous requests.
 *
 * A `BandwidthThrottleGroup` instance must always be created
 * before attaching individual `throttle` instances to it via
 * its `createBandwidthThrottle()` method.
 */

class BandwidthThrottleGroup {
    public config: Readonly<Config> = new Config();

    /**
     * An optional callback providing the consumer with metrics pertaining to the throttle
     * group's average throughput and utlization percentage.
     */

    public onThroughputMetrics:
        | ((throughputData: IThroughputData) => void)
        | null = null;

    private inFlightRequests: BandwidthThrottle[] = [];
    private bandwidthThrottles: (BandwidthThrottle|BandwidthThrottleWithBackpressure)[] = [];
    private clockIntervalId: Timeout | null = null;
    private pollThroughputIntervalId: Timeout = this.pollThroughput();
    private lastTickTime: number = -1;
    private tickIndex: number = 0;
    private secondIndex: number = 0;
    private totalBytesProcessed = 0;

    private get hasTicked(): boolean {
        return this.lastTickTime > -1;
    }

    private get isTicking(): boolean {
        return this.clockIntervalId !== null;
    }

    /**
     * @param options Consumer-provided options defining the
     *  throttling behaviour.
     */

    constructor(options: IConfig) {
        Object.assign(this.config, options);

        this.createBandwidthThrottle = this.createBandwidthThrottle.bind(this);
        this.handleRequestStop = this.handleRequestStop.bind(this);
        this.handleRequestStart = this.handleRequestStart.bind(this);
        this.handleRequestDestroy = this.handleRequestDestroy.bind(this);
        this.processInFlightRequests = this.processInFlightRequests.bind(this);
    }

    public configure(options: IConfig): void {
        Object.assign(this.config, options);
    }

    /**
     * Creates and returns a pipeable `BandwidthThrottle` transform stream,
     * and attaches it to the group.
     *
     * @param contentLength The total number of bytes for the request to be throttled.
     */

    public createBandwidthThrottle(contentLength: number,backpressure?:boolean): BandwidthThrottle|BandwidthThrottleWithBackpressure {
        let bandwidthThrottle;
        if( backpressure ) {
            bandwidthThrottle = new BandwidthThrottleWithBackpressure(
                this.config,
                contentLength,
                this.handleRequestStart,
                this.handleRequestStop,
                this.handleRequestDestroy
            );
        } else {
            bandwidthThrottle = new BandwidthThrottle(
                this.config,
                contentLength,
                this.handleRequestStart,
                this.handleRequestStop,
                this.handleRequestDestroy
            );
        }

        this.bandwidthThrottles.push(bandwidthThrottle);

        return bandwidthThrottle;
    }

    /**
     * Destroys all bandwidth throttle instances in the group, terminating
     * any running intervals, such that the entire group may be garbage
     * collected.
     */

    public destroy(): void {
        clearInterval(this.pollThroughputIntervalId);

        while (this.bandwidthThrottles.length)
            this.bandwidthThrottles.pop()!.destroy();
    }

    public getThisThrottleIndex(throttle: BandwidthThrottle): number {
        return this.bandwidthThrottles.indexOf(throttle);
    }

    /**
     * Increments the number of "in-flight" requests when a request in any
     * attached `BandwidthThrottle` instance starts.
     */

    private handleRequestStart(bandwidthThrottle: BandwidthThrottle): void {
        this.inFlightRequests.push(bandwidthThrottle);

        if (this.clockIntervalId) return;

        // Start the processing clock when the first request starts

        this.clockIntervalId = this.startClock();
    }

    /**
     * Removes the reference of a throttle from the `inFlightRequests` array
     * in order to redistribute bandwidth while a request is inactive or after
     * it has ended.
     *
     * If no other in flight requets are active at that point, the internal
     * clock is stopped to save resources.
     */

    private handleRequestStop(bandwidthThrottle: BandwidthThrottle): void {
        console.log("handleRequestStop, inFlightRequests:",this.inFlightRequests.length)
        console.log("handleRequestStop, bandwidthThrottles:",this.bandwidthThrottles.length)

        if( bandwidthThrottle.pendingBytesBuffer ) {
            console.log("handleRequestStop, bandwidthThrottle.pendingBytesBuffer is not null, releasing memory");
            bandwidthThrottle.pendingBytesBuffer = null;
        } else
            console.log("handleRequestStop, bandwidthThrottle.pendingBytesBuffer is null or inaccessible");
        
        this.inFlightRequests.splice(
            this.inFlightRequests.indexOf(bandwidthThrottle),
            1
        );
        if( this.inFlightRequests.length === 1 ) {
            console.log("handleRequestStop, inFlightRequests.length === 1.")
            console.log("handleRequestStop, inFlightRequests:",this.inFlightRequests)
        }
        if (this.inFlightRequests.length === 0) {
            console.log("handleRequestStop, inFlightRequests.length === 0. Stopping Clock.")
            this.stopClock();
        }
    }

    /**
     * Releases a destroyed throttle from memory.
     */

    private handleRequestDestroy(bandwidthThrottle: BandwidthThrottle): void {
        if( bandwidthThrottle.pendingBytesBuffer ) {
            console.log("handleRequestDestroy, bandwidthThrottle.pendingBytesBuffer is not null, releasing memory");
            bandwidthThrottle.pendingBytesBuffer = null;
        } else {
            console.log("handleRequestDestroy, bandwidthThrottle.pendingBytesBuffer is null or inaccessible");
        }
        
        this.bandwidthThrottles.splice(
            this.bandwidthThrottles.indexOf(bandwidthThrottle),
            1
        );
    }

    /**
     * Starts the "clock" ensuring that all incoming data will be processed at
     * a constant rate, defined by `config.resolutionHz`.
     */

    private startClock(): Timeout {
        // NB: We iterate at a rate 5x faster than the desired tick duration.
        // This seems to greatly increase the likelyhood of the actual ticks
        // occuring at the intended time.

        return setInterval(
            this.processInFlightRequests,
            this.config.tickDurationMs / 5
        );
    }

    /**
     * Stops the clock and resets counters while no requests are active.
     */

    private stopClock(): void {
        clearInterval(this.clockIntervalId!);

        this.clockIntervalId = null;
        this.tickIndex = 0;
        this.secondIndex = 0;
        this.lastTickTime = -1;
    }

    /**
     * On each tick, processes the maximum allowable amount of data
     * through each active request.
     */

    private processInFlightRequests(): void {
        try {
            // Check the time since data was last processed

            const now = Date.now();
            const elapsedTime = this.hasTicked ? now - this.lastTickTime : 0;

            // If throttling active and not the first tick and
            // the time elapsed is less than the provided interval
            // duration, do nothing.

            if (
                this.config.isThrottled &&
                this.hasTicked &&
                elapsedTime < this.config.tickDurationMs
            )
                return;

            // If we have not achieved our `tickDurationMs` goal, then create a multiplier
            // to augment the amount of data sent for the tick, proportional to the delay

            const delayMultiplier = Math.max(
                1,
                elapsedTime / this.config.tickDurationMs
            );
            const period = this.secondIndex % this.inFlightRequests.length;

            this.inFlightRequests.forEach((bandwidthThrottle,i,inFlightRequestsArray) => {
                // Step 1 - evenly destribute bytes between active requests. If cannot
                // be evenly divided, use per second rotation to balance
                // Step 2 - for each individual request, distribute over resolution

                if(!bandwidthThrottle.readable.locked || !bandwidthThrottle.writable.locked){
                    //this.emergencyAbort(this.inFlightRequests[i]);
                    console.log("graceful abort")
                    bandwidthThrottle.gracefulAbort();
                    return;
                }
                    const rotatedIndex = (i + period) % inFlightRequestsArray.length;

                    const bytesPerRequestPerSecond = getPartitionedIntegerPartAtIndex(
                        this.config.bytesPerSecond,
                        inFlightRequestsArray.length,
                        rotatedIndex
                    );
                    const bytesPerRequestPerTick = getPartitionedIntegerPartAtIndex(
                        bytesPerRequestPerSecond,
                        this.config.ticksPerSecond,
                        this.tickIndex
                    );
                    
                    const bytesProcessed = bandwidthThrottle.process(
                        bytesPerRequestPerTick * delayMultiplier
                    );
                this.totalBytesProcessed += bytesProcessed;
            });
        
            // If the clock has been stopped because a call to `.process()`
            // completed the last active request, then do not update state.

            if (!this.isTicking) return;

            this.tickIndex++;

            // Increment the tick index, or reset it to 0 whenever it surpasses
            // the desired resolution

            if (this.tickIndex === this.config.ticksPerSecond) {
                this.tickIndex = 0;
                this.secondIndex++;
            }

            // Increment the last push time, and return

            this.lastTickTime = this.hasTicked
                ? this.lastTickTime + elapsedTime
                : now;
        }  catch (e) {
            console.log("***** error in processInFlightRequests")
           console.error(e);
        }
    }

    private pollThroughput(): Timeout {
        const bytesPerSecondSamples: number[] = [];
        const perSecondMultipler =
            1000 / this.config.throughputSampleIntervalMs;

        let lastHeapRead = 0;

        return setInterval(() => {
            const bytesSinceLastSample = Math.max(
                0,
                this.totalBytesProcessed - lastHeapRead
            );

            lastHeapRead = this.totalBytesProcessed;

            bytesPerSecondSamples.push(bytesSinceLastSample);

            if (bytesSinceLastSample === 0) this.totalBytesProcessed = 0;

            if (bytesPerSecondSamples.length > this.config.throughputSampleSize)
                bytesPerSecondSamples.shift();

            const averageBytesPerSecond =
                (bytesPerSecondSamples.reduce(
                    (sum, sample) => sum + sample,
                    0
                ) /
                    bytesPerSecondSamples.length) *
                perSecondMultipler;

            if (typeof this.onThroughputMetrics === 'function') {
                this.onThroughputMetrics({
                    averageBytesPerSecond,
                    utilization: Math.min(
                        1,
                        averageBytesPerSecond / this.config.bytesPerSecond
                    )
                });
            }
        }, this.config.throughputSampleIntervalMs);
    }
}

export default BandwidthThrottleGroup;
