import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const e2eLatency = new Trend('e2e_delivery_latency', true);
const delivered = new Rate('delivered_successfully');

export const options = {
  /*
   * Deliberately modest load. Two reasons:
   *
   * 1. This test polls for status, so every in-flight iteration adds
   *    requests to the dashboard. At 50 iters/s with 100ms polling it
   *    generated ~1,500 req/s against courier-dashboard — measuring the
   *    dashboard under duress rather than measuring delivery latency.
   *
   * 2. The worker delivers ~141 notifications/s on a single instance.
   *    Submitting faster than that measures queue depth, not pipeline
   *    latency. 20/s stays comfortably inside the worker's capacity.
   */
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: 20,
      timeUnit: '1s',
      duration: '2m',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    'delivered_successfully': ['rate>0.99'],
    'e2e_delivery_latency': ['p(95)<3000', 'p(99)<5000'],
  },
};

const API = __ENV.API_URL || 'http://localhost:8080';
const DASHBOARD = __ENV.DASHBOARD_URL || 'http://localhost:8084';
const API_KEY = __ENV.COURIER_API_KEY || 'dev-client-key-change-me';

/*
 * Fail fast if the dashboard is not reachable. Without this, an
 * unreachable endpoint produces a "successful" run reporting
 * p(95)=0s and 0% delivered, which looks like a system failure
 * rather than a test setup failure.
 */
export function setup() {
  const res = http.get(`${DASHBOARD}/actuator/health`);
  if (res.status !== 200) {
    throw new Error(
      `Dashboard not reachable at ${DASHBOARD} (status ${res.status}). ` +
      `Check the host port mapping in docker-compose.yml.`
    );
  }
}

export default function () {
  const start = Date.now();
  const correlationId = `e2e-${__VU}-${__ITER}-${start}`;

  const submit = http.post(
    `${API}/api/v1/notifications`,
    JSON.stringify({
      correlationId: correlationId,
      channel: 'EMAIL',
      recipient: `e2e${__VU}@example.com`,
      subject: 'E2E latency test',
      content: 'Measuring submission to delivery',
    }),
    { headers: { 'Content-Type': 'application/json', 'X-API-Key': API_KEY }, tags: { name: 'submit' } }
  );

  if (submit.status !== 202) {
    delivered.add(false);
    return;
  }

  const id = JSON.parse(submit.body).notificationId;

  /*
   * Poll until the status reaches a terminal state.
   * 50ms interval, 8 second ceiling.
   *
   * Polling quantizes the measurement: a notification delivered just
   * after a poll is not observed until the next one, so the interval
   * sets the measurement floor. At 250ms nearly every delivery was
   * recorded at ~260ms regardless of its true latency. 50ms resolves
   * the real distribution while keeping poll traffic near 60 req/s
   * at this arrival rate.
   */
  let status = null;
  const deadline = start + 8000;

  while (Date.now() < deadline) {
    const res = http.get(`${DASHBOARD}/api/v1/dashboard/notifications/${id}`, {
      headers: { 'X-API-Key': API_KEY },
      tags: { name: 'poll_status' },
    });

    if (res.status === 200) {
      status = JSON.parse(res.body).status;
      if (status === 'DELIVERED' || status === 'DLQ' || status === 'FAILED') break;
    }
    sleep(0.05);
  }

  const isDelivered = status === 'DELIVERED';
  delivered.add(isDelivered);

  if (isDelivered) {
    e2eLatency.add(Date.now() - start);
  }

  check(null, { 'reached DELIVERED': () => isDelivered });
}
