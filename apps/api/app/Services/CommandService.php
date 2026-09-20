<?php

namespace App\Services;

use App\Models\Command;
use App\Models\CommandLog;
use App\Models\CommandReply;
use App\Models\Image;
use App\Models\SatelliteSubsystem;
use App\Enums\PowerLine;
use App\Enums\SatelliteMode;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Storage;
use Illuminate\Support\Facades\Http;
use WebSocket\Client;
use App\Jobs\DecodeTelemetryJob;
use Exception;


class CommandService
{
    const FLAG = 0xC0;
    private const SRC_GCS = 0xB0;
    const TYPE_ACK = 0x02;
    const TYPE_NACK = 0x03;
    const TYPE_TLM = 0x47;

    /**
     * The command ID byte used by the OBC when sending back image chunks.
     * Matches 0x0E in command2.py's GIMG handler.
     */
    const TYPE_IMG_CHUNK = 0x0E;
    const TYPE_META_CHUNK = 0x4E;

    protected string $commandUrl;

    public function __construct(
        private readonly TelemetryService $telemetryService
    ) {
        $this->commandUrl = config('services.command.url');
    }


    /**
     * Formats the 9-field CSSP frame according to ICD Rev 2.0
     */
    /**
     * Formats the 9-field CSSP frame according to ICD Rev 3.2
     */
    public function buildCsspFrame(Command $command, int $dest, array $data): string
    {
        $requiredFieldsByCommand = [
            'SON'   => ['pwrl_id'],
            'SOFF'  => ['pwrl_id'],
            'GSTLM' => ['subsystem_addr', 'tlm_frame_seq_no'],
            'DIMG'  => ['image_id'],
            'GIMG'  => ['image_id', 'sequence_number', 'window_size'],
            'STIME' => ['timer_value'],
            'SMODE' => ['mode_id'],
        ];

        $payload = '';
        $byteCount = 0;

        if (isset($requiredFieldsByCommand[$command->name])) {
            foreach ($requiredFieldsByCommand[$command->name] as $field) {
                if (isset($data[$field])) {
                    $value = $data[$field];

                    switch ($field) {
                        case 'timer_value':
                            $payload .= pack('J', $value);
                            $byteCount += 8;
                            break;

                        case 'sequence_number':
                            $payload .= pack('N', $value);
                            $byteCount += 4;
                            break;

                        case 'image_id':
                        case 'tlm_frame_seq_no':
                        case 'window_size':
                            $payload .= pack('n', $value);
                            $byteCount += 2;
                            break;

                        case 'mode_id':
                            $rawValue = $data['mode_id'] ?? $data['mode'] ?? $value;

                            $modeValue = $rawValue;

                            // 1. If it's an Enum class instance, get its backing integer value
                            if ($rawValue instanceof SatelliteMode) {
                                $modeValue = $rawValue->value;
                            }
                            // 2. If it's a hex string like "0x01", convert it to decimal safely
                            elseif (is_string($rawValue) && str_starts_with(strtolower($rawValue), '0x')) {
                                $modeValue = hexdec($rawValue);
                            }

                            elseif (is_string($rawValue) && !is_numeric($rawValue)) {
                                $modeValue = match (strtolower($rawValue)) {
                                    'initialization' => 1, // 0x01 
                                    'detumbling'     => 2, // 0x02 
                                    'normal'         => 3, // 0x03 
                                    default          => (int)$rawValue,
                                };
                            }

                            $payload .= pack('C', (int)$modeValue);
                            $byteCount += 1;
                            break;
                        case 'pwrl_id':
                            $powerValue = $value;

                            if ($value instanceof PowerLine) {
                                $powerValue = $value->value;
                            }
                            elseif (is_string($value) && str_starts_with(strtolower($value), '0x')) {
                                $powerValue = hexdec($value);
                            }

                            $payload .= pack('C', (int)$powerValue);
                            $byteCount += 1;
                            break;

                        default:
                            $payload .= pack('C', $value);
                            $byteCount += 1;
                            break;
                    }
                }
            }
        }

        $realId = $command->getRawOriginal('cmd_id');
        $headerAndData = pack('CCCC', $dest, self::SRC_GCS, $realId, $byteCount);
        $headerAndData .= $payload;

        $crc = $this->calculateCRC16IBM($headerAndData);

      /*   return pack('C', self::FLAG) . $headerAndData . pack('nC', $crc, self::FLAG); */
        return pack('C', self::FLAG) . $headerAndData . pack('vC', $crc, self::FLAG);
    }

    private function buildSpaceKeysFrame(int $commandId, string $payload): string
    {
        $destination = (int) config('services.space_keys.destination', 0x07);
        $source = (int) config('services.space_keys.source', 0x01);
        $body = pack('CCCC', $destination, $source, $commandId, strlen($payload)) . $payload;

        return pack('C', self::FLAG)
            . $body
            . pack('vC', $this->calculateCRC16Reflected($body), self::FLAG);
    }

    private function calculateCRC16Reflected(string $data): int
    {
        $crc = 0xFFFF;
        for ($i = 0; $i < strlen($data); $i++) {
            $crc ^= ord($data[$i]);
            for ($j = 0; $j < 8; $j++) {
                $crc = ($crc & 1) ? (($crc >> 1) ^ 0x8408) : ($crc >> 1);
            }
        }

        return $crc & 0xFFFF;
    }

    private function validateSpaceKeysCrc(string $binary): bool
    {
        $length = strlen($binary);
        if ($length < 8 || ord($binary[0]) !== self::FLAG || ord($binary[$length - 1]) !== self::FLAG) {
            return false;
        }

        $body = substr($binary, 1, $length - 4);
        $received = unpack('v', substr($binary, -3, 2))[1];

        return $this->calculateCRC16Reflected($body) === $received;
    }

    public function sendSpaceKeysImageWorkflow(array $data = []): array
    {
        $commandUrl = rtrim($this->commandUrl, '/');
        if (str_starts_with($commandUrl, 'http://')) {
            $commandUrl = 'ws://' . substr($commandUrl, 7);
        } elseif (str_starts_with($commandUrl, 'https://')) {
            $commandUrl = 'wss://' . substr($commandUrl, 8);
        }
        if (!str_ends_with($commandUrl, '/ws/radio')) {
            $commandUrl .= '/ws/radio';
        }

        $capturePayload = pack('C*',
            (int) ($data['resolution'] ?? 4),
            (int) ($data['quality'] ?? 9),
            (int) ($data['flash'] ?? 1),
            (int) ($data['effect'] ?? 0),
            (int) ($data['brightness'] ?? 0),
        );
        $frames = [
            $this->buildSpaceKeysFrame(0x15, "\x04"),
            $this->buildSpaceKeysFrame(0x2B, $capturePayload),
            $this->buildSpaceKeysFrame(0x28, ''),
        ];
        $ackFrames = [];

        $client = new Client($commandUrl, ['timeout' => (int) config('services.command.timeout', 15)]);
        try {
            foreach ($frames as $index => $frame) {
                $client->send($frame, 'binary');
                $ack = $client->receive();
                $ackFrames[] = $ack;
                $decoded = $this->decode(bin2hex($ack));
                $expected = [0x15, 0x2B, 0x28][$index];
                if (!$decoded || !$decoded['is_ack'] || !$this->validateSpaceKeysCrc($ack)
                    || $decoded['command_id'] !== $expected) {
                    throw new Exception(sprintf('Space Keys command 0x%02X was not acknowledged.', $expected));
                }
            }
        } finally {
            $client->close();
        }

        $response = Http::timeout((int) config('services.space_keys.http_timeout', 15))
            ->accept('*/*')
            ->get(config('services.space_keys.image_url'));
        $response->throw();
        $imageBytes = $response->body();
        if (!str_starts_with($imageBytes, "\xFF\xD8")) {
            throw new Exception('Space Keys /image did not return a JPEG.');
        }

        return ['ack_frames' => $ackFrames, 'image_bytes' => $imageBytes];
    }

    public function saveDownloadedImage(string $imageBytes, int $logId, ?array $metadata = null): Image
    {
        $path = 'satellite_images/original/image_log_' . $logId . '.jpg';
        Storage::disk('public')->makeDirectory('satellite_images/original');
        Storage::disk('public')->put($path, $imageBytes);

        return Image::create([
            'original_path' => $path,
            'command_log_id' => $logId,
            'meta_data' => $metadata,
        ]);
    }

    /**
     * CRC-16/IBM-3740 Implementation
     * Poly: 0x1021 (X^16 + X^12 + X^5 + 1), Init: 0xFFFF
     */
    private function calculateCRC16IBM(string $data): int
    {
        $crc = 0xFFFF;
        for ($i = 0; $i < strlen($data); $i++) {
            $crc ^= (ord($data[$i]) << 8);
            for ($j = 0; $j < 8; $j++) {
                if ($crc & 0x8000) {
                    $crc = ($crc << 1) ^ 0x1021;
                } else {
                    $crc <<= 1;
                }
            }
        }
        return $crc & 0xFFFF;
    }

    public function decode(string $hex): ?array
    {
        $binary = hex2bin(str_replace(' ', '', $hex));
        $bytes = array_values(unpack('C*', $binary));

        if (count($bytes) < 9 || $bytes[0] !== 0xC0) {
            return null;
        }

        $type = $bytes[3];

        if ($type !== self::TYPE_ACK && $type !== self::TYPE_NACK) {
            return null;
        }

        return [
            'is_ack'      => ($type === self::TYPE_ACK),
            'command_id'  => $bytes[5],
            'source'      => sprintf('0x%02x', $bytes[1]),
            'destination' => sprintf('0x%02x', $bytes[2]),
            'is_valid'    => $this->validateCRC($binary),
        ];
    }

    public function validateCRC(string $binary): bool
    {
        $len = strlen($binary);
        $payloadForCrc = substr($binary, 1, $len - 4);
        $calculated = $this->calculateCRC16IBM($payloadForCrc);

        $msb = ord($binary[$len - 3]);
        $lsb = ord($binary[$len - 2]);
        $received = ($msb << 8) | $lsb;

        return $calculated === $received;
    }

    public function sendToGateway(string $binary, string $commandName): mixed
    {
        Log::info("MCC SENDING CSSP FRAME: " . bin2hex($binary));
        $commandUrl = rtrim($this->commandUrl, '/');

        if (str_starts_with($commandUrl, 'http://')) {
            $commandUrl = 'ws://' . substr($commandUrl, 7);
        } elseif (str_starts_with($commandUrl, 'https://')) {
            $commandUrl = 'wss://' . substr($commandUrl, 8);
        }

        if (!str_ends_with($commandUrl, '/ws/radio')) {
            $commandUrl .= '/ws/radio';
        }

        $timeout = ($commandName === 'GIMG') ? 1000 : 10;

        try {
            $client = new Client($commandUrl, ['timeout' => $timeout]);
            $client->send($binary, 'binary');

            // HI: fire-and-forget
            if ($commandName === 'Hi') {
                $client->close();
                return "Hi Sent Successfully";
            }

            // Receive the first frame — always ACK or NACK
            $firstResponse = $client->receive();
            Log::info("Initial ACK/NACK: " . bin2hex($firstResponse));

            //GSTLM: ACK + up to 7 stored telemetry frames 
            if ($commandName === 'GSTLM') {
                $allFrames = [$firstResponse];
                for ($i = 0; $i < 8; $i++) {
                    try {
                        $telemetryFrame = $client->receive();
                        Log::info("Received Stored TLM " . ($i + 1) . ": " . bin2hex($telemetryFrame));
                        $allFrames[] = $telemetryFrame;
                    } catch (\Exception $e) {
                        Log::warning("Timed out or failed waiting for TLM frame $i — stopping.");
                        break;
                    }
                }
                $client->close();
                return $allFrames;
            }

            // ── GIMG: Collect Text Metadata AND Binary Image Chunks ──────────
            if ($commandName === 'GIMG') {
                $firstBytes = array_values(unpack('C*', $firstResponse));
                $isAck      = isset($firstBytes[3]) && $firstBytes[3] === self::TYPE_ACK;

                if (!$isAck) {
                    Log::warning("GIMG: NACK on first frame — aborting collection.");
                    $client->close();
                    return ['image_chunks' => [], 'metadata' => null, 'ack_frame' => $firstResponse];
                }

                Log::info("GIMG: ACK received — starting metadata and image chunk collection.");

                $chunks = [];
                $metadataText = ""; // 🌟 Container for incoming clear text JSON string

                $client->setTimeout(5);

                while (true) {
                    try {
                        $chunkFrame = $client->receive();
                        $chunkBytes = array_values(unpack('C*', $chunkFrame));

                        if (count($chunkBytes) < 5 || $chunkBytes[0] !== self::FLAG) {
                            Log::warning("GIMG: Malformed frame received.");
                            break;
                        }

                        $cmdId   = $chunkBytes[3];
                        $dataLen = $chunkBytes[4];
                        $payload = substr($chunkFrame, 5, $dataLen);

                        // 🌟 CASE A: Chunk is CLEAR TEXT metadata JSON snippet
                        if ($cmdId === self::TYPE_META_CHUNK) {
                            $metadataText .= $payload; // Append raw string text directly
                        }
                        // 🌟 CASE B: Chunk is binary image data
                        elseif ($cmdId === self::TYPE_IMG_CHUNK) {
                            $chunks[] = $payload;

                            if (count($chunks) % 500 === 0) {
                                Log::info("GIMG: collected " . count($chunks) . " image chunks so far…");
                            }
                        } else {
                            Log::warning("GIMG: unexpected frame cmd_id=0x" . sprintf('%02X', $cmdId) . " — stopping.");
                            break;
                        }
                    } catch (\WebSocket\ConnectionException $e) {
                        Log::info("GIMG: connection closed by OBC after stream completion.");
                        break;
                    } catch (\Exception $e) {
                        Log::info("GIMG: stream ended (idle timeout reached).");
                        break;
                    }
                }

                $client->close();

                // Convert text JSON to array if present
                $parsedMetadata = null;
                if (!empty($metadataText)) {
                    Log::info("GIMG: Successfully collected metadata text string: " . $metadataText);
                    $parsedMetadata = json_decode($metadataText, true);
                }

                return [
                    'ack_frame'    => $firstResponse,
                    'image_chunks' => $chunks,
                    'metadata'     => $parsedMetadata,
                ];
            }

            $client->close();
            return $firstResponse;
        } catch (\Exception $e) {
            Log::error("Gateway connection failed: " . $e->getMessage());
            return null;
        }
    }

    /**
     * Image reconstruction and storage
     */
    public function reconstructAndSaveImage(array $chunks, int $imageId, int $logId, ?array $metadata = null): ?Image
    {
        if (empty($chunks)) {
            Log::warning("GIMG: no chunks to reconstruct for image_id={$imageId}, log_id={$logId}");
            return null;
        }

        // ── 1. Concatenate all chunk payloads in order ────────────────────────
        $rawBytes = implode('', $chunks);

        // ── 2. Prepare storage directory ─────────────────────────────────────
        Storage::disk('public')->makeDirectory('/satellite_images/original');

        $filename = "image_{$imageId}_log_{$logId}.png";
        $path = 'satellite_images/original/' . $filename;

        // ── 3. Decode and save as PNG ─────────────────────────────────────────
        $gdImage = @imagecreatefromstring($rawBytes);

        if ($gdImage !== false) {
            ob_start();
            imagepng($gdImage);
            $pngData = ob_get_clean();
            Storage::disk('public')->put($path, $pngData);
            imagedestroy($gdImage);
            Log::info("GIMG: PNG saved → {$path}");
        } else {
            Storage::disk('public')->put($path, $rawBytes);
            Log::warning("GIMG: GD could not decode image bytes; raw bytes saved to {$path}.");
        }

        // ── 4. Persist to `images` table  ─────────────────
        $imageRecord = Image::create([
            'original_path'  => $path,
            'command_log_id' => $logId,
            'meta_data'      => $metadata,
        ]);

        Log::info("GIMG: Image record #{$imageRecord->id} created for log #{$logId}.");
        return $imageRecord;
    }

    // ── Repository helpers ───────────────────────────────────────────────────

    public function getAllCommands()
    {
        return Command::all();
    }

    public function getAllCommandsWithSubsystems()
    {
        $commands = $this->getAllCommands();
        $allDestinations = $commands->pluck('allowed_destinations')->flatten()->unique()->filter();
        $subsystems = SatelliteSubsystem::whereIn('hex_code', $allDestinations)->get(['hex_code', 'name']);
        $subsystemMap = $subsystems->keyBy('hex_code');
        $commands->transform(function ($command) use ($subsystemMap) {
            $command->subsystems = collect($command->allowed_destinations)->map(function ($id) use ($subsystemMap) {
                return $subsystemMap->get($id)?->only(['hex_code', 'name']);
            })->filter()->values();
            return $command;
        });
        return $commands;
    }

    public function getCommandById($id)
    {
        $command = Command::where('id', $id)->first();
        if (!$command) {
            throw new \Exception("Command with ID $id not found.");
        }
        return $command;
    }

    public function getAllReplies()
    {
        return CommandReply::with(['commandLog.command:id,name', 'commandLog'])
            ->whereHas('commandLog.command')
            ->orderBy('created_at', 'desc')
            ->paginate(30);
    }

    public function validateCommandData(int $commandId, array $data): bool
    {
        $command = Command::findOrFail($commandId);

        $requiredFieldsByCommand = [
            'SON'   => ['pwrl_id'],
            'SOFF'  => ['pwrl_id'],
            'GSTLM' => ['tlm_frame_seq_no'],
            'DIMG'  => ['image_id'],
            'GIMG'  => config('services.space_keys.enabled')
                ? []
                : ['image_id', 'sequence_number', 'window_size'],
            'STIME' => ['timer_value'],
            'SMODE' => ['mode_id'],
        ];

        if (isset($data['pwrl_id']) && $data['pwrl_id'] !== null) {
            $valueToValidate = $data['pwrl_id'] instanceof \App\Enums\PowerLine
                ? $data['pwrl_id']->value
                : $data['pwrl_id'];

            if (!\App\Enums\PowerLine::tryFrom($valueToValidate)) {
                throw new \InvalidArgumentException("Invalid pwrl_id: {$valueToValidate}. Must match an active PowerLine enum value.");
            }
        }
        if (isset($data['mode_id']) && !SatelliteMode::tryFrom($data['mode_id'])) {
            throw new \InvalidArgumentException("Invalid mode_id: {$data['mode_id']}. Must be a valid SatelliteMode enum value.");
        }
        if (isset($requiredFieldsByCommand[$command->name])) {
            foreach ($requiredFieldsByCommand[$command->name] as $field) {
                if (!array_key_exists($field, $data) || $data[$field] === null) {
                    throw new \InvalidArgumentException("{$field} is required for the {$command->name} command.");
                }
            }
        }

        return true;
    }
}
