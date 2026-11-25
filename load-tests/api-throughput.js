import http from 'k6/http';
import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';

/*
 * Custom metrics beyond k6's built-ins.
 * rate_limited tracks 429s separately — at high RPS the rate limiter
 * starts rejecting, and those are not failures, they are the system
 * working correctly. Counting them as errors would be misleading.
 */
const rateLimited = new Rate('rate_limited');
const submitDuration = new Trend('submit_duration', true);

/*
 * PEAK is the target request rate of the final ramp stage, in req/s.
 *
 *   PEAK=800  k6 run load-tests/api-throughput.js   # sustainable-load profile
 *   PEAK=2500 k6 run load-tests/api-throughput.js   # find the ceiling (default)
 *
 * The four ramp stages are fixed fractions of PEAK, so every profile has the
 * same shape and runs at different PEAK values stay comparable. At PEAK=2500
 * the stages are exactly 400 -> 800 -> 1500 -> 2500 req/s.
 *
 * A ceiling run is expected to FAIL the thresholds below - it deliberately
 * overloads the system. Use PEAK=800 when the thresholds should gate a build.
 */
const PEAK = parseInt(__ENV.PEAK || '2500', 10);
const at = (fraction) => Math.max(1, Math.round(PEAK * fraction));

export const options = {
  /*
   * Ramping arrival rate: k6 maintains a target REQUESTS PER SECOND,
   * adding virtual users as needed to sustain it.
   *
   * The alternative (ramping-vus) maintains a target number of concurrent
   * users, which means throughput drops as latency rises - you end up
   * measuring the system's comfort zone rather than finding its ceiling.
   * Arrival rate keeps the pressure on regardless of how slow it gets.
   */
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: Math.min(50, at(0.16)),
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
      stages: [
        { duration: '30s', target: at(0.16) },
        { duration: '60s', target: at(0.32) },
        { duration: '60s', target: at(0.60) },
        { duration: '60s', target: at(1.00) },
        { duration: '30s', target: 0 },   // ramp down
      ],
    },
  },

  /*
   * Thresholds are pass/fail criteria. k6 exits non-zero if any fail,
   * which makes this usable in CI as a performance regression gate.
   */
  thresholds: {
    'http_req_failed': ['rate<0.01'],           // under 1% genuine errors
    'http_req_duration': ['p(95)<500', 'p(99)<1000'],
  },
};

export function setup() {
  console.log(`Ramp profile: peak ${PEAK} req/s, stages ${at(0.16)} -> ${at(0.32)} -> ${at(0.60)} -> ${at(1.00)}`);
}

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const API_KEY = __ENV.COURIER_API_KEY || 'dev-client-key-change-me';

export default function () {
  /*
   * Unique correlationId per request. Reusing one would hit the
   * idempotency check and return the cached record without doing
   * any real work — you would be load testing a DB lookup.
   *
   * __VU is the virtual user ID, __ITER the iteration number.
   * Together they are unique across the whole run.
   */
  const correlationId = `load-${__VU}-${__ITER}-${Date.now()}`;

  const payload = JSON.stringify({
    correlationId: correlationId,
    channel: 'EMAIL',
    recipient: `user${__VU}@example.com`,
    subject: 'Load test',
    content: 'Measuring API throughput',
  });

  const res = http.post(`${BASE_URL}/api/v1/notifications`, payload, {
    headers: { 'Content-Type': 'application/json', 'X-API-Key': API_KEY },
    tags: { name: 'submit' },
  });

  rateLimited.add(res.status === 429);
  submitDuration.add(res.timings.duration);

  check(res, {
    'accepted (202)': (r) => r.status === 202,
    'has notificationId': (r) => {
      if (r.status !== 202) return true;   // only assert on successes
      try {
        return JSON.parse(r.body).notificationId !== undefined;
      } catch {
        return false;
      }
    },
  });
}