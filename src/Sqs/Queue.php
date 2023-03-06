<?php

namespace Dusterio\PlainSqs\Sqs;

use Dusterio\PlainSqs\Jobs\DispatcherJob;
use Illuminate\Queue\SqsQueue;
use Illuminate\Support\Facades\Config;
use Illuminate\Queue\Jobs\SqsJob;

/**
 * Class CustomSqsQueue
 * @package App\Services
 */
class Queue extends SqsQueue
{
    /**
     * Create a payload string from the given job and data.
     *
     * @param  string  $job
     * @param  mixed   $data
     * @param  string  $queue
     * @return string
     */
    protected function createPayload($job, $data = '', $queue = null)
    {
        if (!$job instanceof DispatcherJob) {
            return parent::createPayload($job, $data, $queue);
        }

        $handlerJob = $this->getClass($queue) . '@handle';

        return $job->isPlain() ? $this->encodeJson($job->getPayload()) : $this->encodeJson(['job' => $handlerJob, 'data' => $job->getPayload()]);
    }

    /**
     * @param $queue
     * @return string
     */
    private function getClass($queue = null)
    {
        if (!$queue) return Config::get('sqs-plain.default-handler');

        $queue = end(explode('/', $queue));

        return (array_key_exists($queue, Config::get('sqs-plain.handlers')))
            ? Config::get('sqs-plain.handlers')[$queue]
            : Config::get('sqs-plain.default-handler');
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string  $queue
     * @return \Illuminate\Contracts\Queue\Job|null
     */
    public function pop($queue = null)
    {
        $queue = $this->getQueue($queue);

        $response = $this->sqs->receiveMessage([
            'QueueUrl' => $queue,
            'AttributeNames' => ['All'],
            'MessageAttributeNames' => ['All'],
        ]);

        if (isset($response['Messages']) && count($response['Messages']) > 0) {
            $queueId = explode('/', $queue);
            $queueId = array_pop($queueId);

            $class = (array_key_exists($queueId, $this->container['config']->get('sqs-plain.handlers')))
                ? $this->container['config']->get('sqs-plain.handlers')[$queueId]
                : $this->container['config']->get('sqs-plain.default-handler');

            $response = $this->modifyPayload($response['Messages'][0], $class);

            if (preg_match('/(5\.[4-8]\..*)|(6\.[0-9]*\..*)|(7\.[0-9]*\..*)|(8\.[0-9]*\..*)|(9\.[0-9]*\..*)/', $this->container->version())) {
                return new SqsJob($this->container, $this->sqs, $response, $this->connectionName, $queue);
            }

            return new SqsJob($this->container, $this->sqs, $queue, $response);
        }
    }

    /**
     * @param string|array $payload
     * @param string $class
     * @return array
     */
    private function modifyPayload($payload, $class)
    {
        if (! is_array($payload)) $payload = json_decode($payload, true);

        $body = json_decode($payload['Body'], true);

        $body = [
            'job' => $class . '@handle',
            'data' => isset($body['data']) ? $body['data'] : $body,
            'attributes' => array_merge($payload['Attributes'], $payload['MessageAttributes']),
            'raw' => $payload['Body'],
            'uuid' => $payload['MessageId']
        ];

        $payload['Body'] = $this->encodeJson($body);

        return $payload;
    }

    private function encodeJson($data)
    {
        return json_encode($data, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
    }
}
