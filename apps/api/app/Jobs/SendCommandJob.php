<?php

namespace App\Jobs;

use App\Models\Command;
use App\Models\CommandLog;
use App\Models\CommandReply;
use App\Services\CommandService;
use App\Services\SatelliteService;
use App\Jobs\DecodeTelemetryJob;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Contracts\Queue\ShouldBeUnique;  // ← ADD THIS
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

class SendCommandJob implements ShouldQueue, ShouldBeUnique  // ← ADD ShouldBeUnique
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    public int  $tries         = 1;
    public int  $timeout       = 1000;
    public int  $maxExceptions = 1;
    public bool $failOnTimeout = false;

    // ← REMOVE retryUntil() entirely. It keeps a failed job alive for 20 min,
    //   during which re-deliveries from worker restarts bypass $tries = 1.

    // ── Unique lock ───────────────────────────────────────────────────────────
    // One job per logId at a time. A second dispatch for the same log is silently
    // dropped until the lock is released (job finishes or times out).

    public function uniqueId(): string
    {
        return (string) $this->logId;
    }

    public function uniqueFor(): int
    {
        // Hold the lock for the job's full timeout + a small buffer.
        // Without this, the default (60s) releases the lock while the job
        // is still collecting GIMG chunks, allowing a new job to start.
        return $this->timeout + 60;
    }

    public function __construct(
        public readonly int    $commandId,
        public readonly int    $dest,
        public readonly array  $data,
        public readonly int    $logId,
        public readonly string $commandName = '',
    ) {
        if ($commandName === 'GIMG') {
            $this->queue = 'images';
            Log::info("SendCommandJob: GIMG routed to [images] queue, log #{$this->logId}");
        }
    }

    public function handle(CommandService $commandService, SatelliteService $satelliteService): void
    {
        $log     = CommandLog::findOrFail($this->logId);
        $command = Command::findOrFail($this->commandId);

        try {
            if ($command->name === 'GIMG' && config('services.space_keys.enabled')) {
                $workflow = $commandService->sendSpaceKeysImageWorkflow($this->data);
                foreach ($workflow['ack_frames'] as $ackFrame) {
                    CommandReply::create([
                        'command_log_id' => $log->id,
                        'reply_data' => bin2hex($ackFrame),
                    ]);
                }

                $image = $commandService->saveDownloadedImage($workflow['image_bytes'], $log->id);
                $log->update(['status' => 'image_received', 'replied_at' => now()]);
                Log::info("GIMG: Space Keys image #{$image->id} saved for log #{$log->id}.");
                return;
            }

            $binaryFrame = $commandService->buildCsspFrame($command, $this->dest, $this->data);

            $log->update([
                'raw_binary_sent' => bin2hex($binaryFrame),
                'status'          => 'pending',
                'sent_at'         => now(),
            ]);

            $response = $commandService->sendToGateway($binaryFrame, $command->name);

            // ── HI ────────────────────────────────────────────────────────────
            if ($response === 'Hi Sent Successfully') {
                $log->update(['status' => 'sent']);
                return;
            }

            if (!$response) {
                $log->update(['status' => 'error']);
                return;
            }

            // ── GIMG: image chunk reassembly ──────────────────────────────────
            if ($command->name === 'GIMG') {
                $this->handleGimgResponse($response, $command, $log, $commandService);
                return;
            }

            // ── GSTLM / single-frame commands ─────────────────────────────────
            $log->update(['status' => 'received', 'replied_at' => now()]);

            $frames      = is_array($response) ? $response : [$response];
            $satelliteId = 1;

            foreach ($frames as $index => $frameBinary) {
                $frameHex = bin2hex($frameBinary);

                CommandReply::create([
                    'command_log_id' => $log->id,
                    'reply_data'     => $frameHex,
                ]);

                if (strlen($frameHex) <= 18) {
                    // Short frame → ACK/NACK
                    $decoded = $commandService->decode($frameHex);
                    if ($decoded) {
                        $log->update(['status' => $decoded['is_ack'] ? 'ack' : 'nack']);
                    }

                    if ($command->name === 'SMODE' && ($decoded['is_ack'] ?? false)) {
                        $newMode = $this->data['mode_id'] ?? $this->data['mode'] ?? null;
                        if ($newMode) {
                            $satelliteService->updateSubsystemMode($newMode, $this->dest);
                        }
                    }
                    continue;
                }

                // Long frame → telemetry
                Log::info("Dispatching DecodeTelemetryJob for log #{$log->id}, frame {$index}: {$frameHex}");
                DecodeTelemetryJob::dispatch($frameHex, $satelliteId, $log->id);
                $log->update(['status' => 'telemetry_received']);
            }
        } catch (\Throwable $e) {
            Log::error("SendCommandJob failed for log #{$log->id}: " . $e->getMessage());
            $log->update(['status' => 'error']);
            // Do not rethrow and do not call $this->fail() — both cause Laravel
            // to increment the attempt counter and emit "attempted too many times".
            // The error is fully recorded via the log status; let the job finish cleanly.
        }
    }

    /**
     * Handles the structured response from CommandService::sendToGateway()
     * for a GIMG command.
     */
    private function handleGimgResponse(
        mixed          $response,
        Command        $command,
        CommandLog     $log,
        CommandService $commandService,
    ): void {
        if (!is_array($response) || !isset($response['image_chunks'])) {
            Log::error("GIMG: unexpected response structure for log #{$log->id}");
            $log->update(['status' => 'error']);
            return;
        }

        $ackFrame    = $response['ack_frame']    ?? null;
        $imageChunks = $response['image_chunks'] ?? [];
        $metadata    = $response['metadata']     ?? null; // 🌟 This is our clear text JSON array from the satellite

        // ── 1. Log the ACK frame ─────────────────────────────────────────────
        if ($ackFrame) {
            $ackHex = bin2hex($ackFrame);
            CommandReply::create([
                'command_log_id' => $log->id,
                'reply_data'     => $ackHex,
            ]);

            $decoded = $commandService->decode($ackHex);
            if ($decoded && !$decoded['is_ack']) {
                Log::warning("GIMG: NACK received for log #{$log->id}. No image to reconstruct.");
                $log->update(['status' => 'nack', 'replied_at' => now()]);
                return;
            }
        }

        $log->update(['status' => 'received', 'replied_at' => now()]);

        // ── 2. Guard: nothing came through ───────────────────────────────────
        if (empty($imageChunks)) {
            Log::warning("GIMG: ACK received but zero image chunks for log #{$log->id}.");
            $log->update(['status' => 'error']);
            return;
        }

        Log::info("GIMG: " . count($imageChunks) . " chunk(s) received for log #{$log->id}. Reconstructing image…");

        // ── 3. Log a summary reply entry including metadata ──────────────────
        $totalBytes = array_sum(array_map('strlen', $imageChunks));
        CommandReply::create([
            'command_log_id' => $log->id,
            'reply_data'     => json_encode([
                'type'         => 'image_chunks',
                'chunk_count'  => count($imageChunks),
                'total_bytes'  => $totalBytes,
                'sat_metadata' => $metadata, // Keeps a copy in the logs wrapper too
            ]),
        ]);

        // ── 4. Reconstruct + save the image (Passing the metadata down) ──────
        $imageId     = $this->data['image_id'] ?? 0;

        // 🌟 PASS $metadata AS THE 4TH ARGUMENT HERE:
        $imageRecord = $commandService->reconstructAndSaveImage($imageChunks, $imageId, $log->id, $metadata);

        if ($imageRecord) {
            $log->update(['status' => 'image_received']);
            Log::info("GIMG: Image record #{$imageRecord->id} linked to log #{$log->id} with metadata saved.");
        } else {
            $log->update(['status' => 'error']);
            Log::error("GIMG: failed to reconstruct/save image for log #{$log->id}.");
        }
    }
}
