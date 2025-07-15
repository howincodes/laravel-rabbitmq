<?php

declare(strict_types=1);

namespace HowinCodes\RabbitMQ\Console;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use HowinCodes\RabbitMQ\Connection\ConnectionManager;

final class ConsumeQueues extends Command
{
    protected $signature   = 'rabbitmq:consume-queues';
    protected $description = 'Declare, bind & consume all queues with retry';

    public function handle(ConnectionManager $conn): int
    {
        $this->info('🔌 Connecting to RabbitMQ…');
        $ch = $conn->channel();

        // 1️⃣ Exchanges
        $exchanges = config('rabbitmq.exchanges', []);
        foreach ($exchanges as $name => $opts) {
            $this->info("📑 Declaring exchange [{$name}]");
            $ch->exchange_declare(
                $name,
                $opts['type']       ?? 'direct',
                false,
                $opts['durable']    ?? true,
                $opts['auto_delete'] ?? false
            );
        }

        // 2️⃣ Queues
        $queues = config('rabbitmq.queues', []);
        foreach ($queues as $queue => $opts) {
            $this->info("📂 Declaring queue [{$queue}]");
            $args = collect($opts['arguments'] ?? [])
                ->mapWithKeys(fn($v, $k) => [$k => is_int($v) ? ['I', $v] : ['S', (string)$v]])
                ->all();

            $ch->queue_declare(
                $queue,
                false,
                $opts['durable'] ?? true,
                false,
                false,
                false,
                $args
            );
            $this->info("  ✅ Declared [{$queue}]");

            $routingKeys = (array)($opts['routing_keys'] ?? []);
            foreach ($routingKeys as $rk) {
                $ch->queue_bind($queue, $opts['exchange'], $rk);
            }
            $this->info("  🔗 Bound [{$queue}] to [{$opts['exchange']}] (keys: " . implode(', ', $routingKeys) . ")");
        }

        // 3️⃣ QoS & Consumers
        $ch->basic_qos(null, 1, null);
        $this->info('⚙️  Setting QoS & starting consumers…');

        $handlers = config('rabbitmq.queue_handlers', []);
        foreach ($queues as $queue => $opts) {

            if (empty($opts['consume']) || $opts['consume'] === false) {
                $this->info("⏩ Skipping [{$queue}] (consume=false)");
                continue;
            }

            $handlerClass = $handlers[$queue] ?? null;
            $maxAttempts  = $opts['retry']['max_attempts'] ?? 3;

            $ch->basic_consume(
                $queue,
                '',
                false,
                false,
                false,
                false,
                $this->makeHandler($ch, $queue, $handlerClass, $maxAttempts)
            );
            $this->info("👂 Listening on [{$queue}] (max retries: {$maxAttempts})");
        }

        $this->info('🚀 Ready—press CTRL+C to quit.');
        while ($ch->is_consuming()) {
            $ch->wait();
        }

        $this->info('🔒 Connection closed.');
        return self::SUCCESS;
    }

    /**
     * @param  \PhpAmqplib\Channel\AMQPChannel  $ch
     * @param  string                           $queue
     * @param  string|null                      $handlerClass
     * @param  int                              $maxAttempts
     */
    private function makeHandler($ch, string $queue, ?string $handlerClass, int $maxAttempts): callable
    {
        return function ($msg) use ($ch, $queue, $handlerClass, $maxAttempts): void {
            $payload = json_decode($msg->body, true) ?? [];
            $rk      = $msg->delivery_info['routing_key'];

            // Count prior retries via x-death header
            $retryCount = 0;
            if ($msg->has('application_headers')) {
                $deaths = $msg->get('application_headers')->getNativeData()['x-death'] ?? [];
                foreach ($deaths as $death) {
                    if (($death['queue'] ?? '') === $queue) {
                        $retryCount = (int)($death['count'] ?? 0);
                        break;
                    }
                }
            }

            try {
                if (! $handlerClass || ! method_exists($handlerClass, 'handle')) {
                    throw new \RuntimeException("Handler missing for [{$queue}]");
                }

                $handlerClass::handle($rk, $payload);

                $this->info("📥 [{$queue}] ✅ Processed '{$rk}' (attempt #" . ($retryCount + 1) . ")");
                $ch->basic_ack($msg->delivery_info['delivery_tag']);
            } catch (\Throwable $e) {
                Log::error("[{$queue}] {$e->getMessage()}", ['payload' => $payload, 'retry' => $retryCount]);

                if ($retryCount >= $maxAttempts) {
                    $this->error("❌ [{$queue}] Dropping after {$retryCount} retries");
                    $ch->basic_ack($msg->delivery_info['delivery_tag']);
                } else {
                    $this->warn("⚠️ [{$queue}] Error on attempt #" . ($retryCount + 1) . ": {$e->getMessage()}, retrying…");
                    $ch->basic_nack($msg->delivery_info['delivery_tag'], false, false);
                }
            }
        };
    }
}
