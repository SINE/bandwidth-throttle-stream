import BandwidthThrottle from '../BandwidthThrottle.ts';
import BandwidthThrottleWithBackpressure from '../BandwidthThrottleWithBackpressure.ts';

type CallbackWithSelf = (bandwidthThrottle: BandwidthThrottle|BandwidthThrottleWithBackpressure) => void;

export default CallbackWithSelf;
