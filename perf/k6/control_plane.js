import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';

const client = new grpc.Client();
client.load(['/workspace/proto/dispatchd/v1'], 'dispatchd.proto');

export const options = {
  vus: Number(__ENV.K6_VUS || 10),
  duration: __ENV.K6_DURATION || '30s',
};

export default function () {
  client.connect(__ENV.CONTROL_PLANE_TARGET || '127.0.0.1:8080', {
    plaintext: true,
  });

  const unique = `${__VU}-${__ITER}-${Date.now()}`;
  const submitResponse = client.invoke('dispatchd.v1.JobService/SubmitJob', {
    jobType: 'perf-smoke',
    payload: 'AQIDBA==',
    priority: 10,
    idempotencyKey: `k6-${unique}`,
  });

  check(submitResponse, {
    'submit status ok': (response) => response && response.status === grpc.StatusOK,
    'submit returned job': (response) => response && response.message && response.message.job && response.message.job.jobId !== '',
  });

  if (submitResponse && submitResponse.status === grpc.StatusOK) {
    const jobId = submitResponse.message.job.jobId;
    const getResponse = client.invoke('dispatchd.v1.JobService/GetJob', {
      jobId,
    });

    check(getResponse, {
      'get status ok': (response) => response && response.status === grpc.StatusOK,
      'get echoes job': (response) => response && response.message && response.message.job && response.message.job.jobId === jobId,
    });
  }

  client.close();
  sleep(Number(__ENV.K6_SLEEP_SECONDS || 0.05));
}
