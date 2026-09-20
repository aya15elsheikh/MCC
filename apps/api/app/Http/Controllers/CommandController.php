<?php

namespace App\Http\Controllers;

use App\Services\CommandService;
use App\Models\CommandLog;
use App\Models\Image;
use Illuminate\Http\Request;
use Illuminate\Http\JsonResponse;
use Illuminate\Support\Facades\Storage;
use App\Models\Command;
use App\Jobs\SendCommandJob;
use Symfony\Component\HttpFoundation\BinaryFileResponse;
use App\Models\CommandSchedule;
use App\Services\AutonomousMissionService;
use Carbon\Carbon;
use Illuminate\Support\Str;
use App\Enums\HtnGoalType;
use InvalidArgumentException;
use App\Enums\PowerLine;
use Exception;

class CommandController extends Controller
{
    protected CommandService $commandService;
    protected AutonomousMissionService $autonomousService;
    public function __construct(CommandService $commandService, AutonomousMissionService $autonomousService)
    {
        $this->commandService = $commandService;
        $this->autonomousService = $autonomousService;
    }

    /**
     * Get Command List
     */
    public function index(): JsonResponse
    {
        try {
            $commands = $this->commandService->getAllCommandsWithSubsystems();
            return response()->json($commands);
        } catch (Exception $e) {
            return response()->json(['message' => 'Failed to fetch commands: ' . $e->getMessage()], 400);
        }
    }

    /**
     * Get a command by ID
     */
    public function show($id): JsonResponse
    {
        try {
            $command = $this->commandService->getCommandById($id);
            return response()->json($command);
        } catch (Exception $e) {
            return response()->json(['message' => 'Failed to fetch command: ' . $e->getMessage()], 400);
        }
    }

    /**
     * Send a command
     */
    public function send(Request $request): JsonResponse
    {
        $request->validate([
            'command_id'           => 'required|integer|exists:commands,id',
            'dest_address'         => 'required|integer',
            'data'                 => 'nullable|array',
            'data.pwrl_id'         => 'nullable|string',
            'data.image_id'        => 'nullable|integer',
            'data.timer_value'     => 'nullable|integer',
            'data.mode_id'         => 'nullable|string',
            'data.sequence_number' => 'nullable|integer',
            'data.window_size'     => 'nullable|integer',
            'data.tlm_frame_seq_no' => 'nullable|integer',
        ]);

        try {
            $command = $this->commandService->getCommandById($request->input('command_id'));

            $payload = array_filter(
                $request->input('data', []),
                fn($value) => $value !== null
            );

            $this->commandService->validateCommandData($command->id, $payload);

            $log = CommandLog::create([
                'command_id'      => $command->id,
                'satellite_id'    => 1,// Assuming a single satellite for now
                'dest_address'    => sprintf('0x%02X', $request->input('dest_address')),
                'src_address'     => sprintf('0x%02X', 0xB0),
                'raw_binary_sent' => null,
                'status'          => 'pending',
                'data'            => $payload,
                'sent_at'         => now(),
            ]);

            $job = new SendCommandJob(
                $command->id,
                $request->input('dest_address'),
                $payload,
                $log->id,
                $command->name,
            );

            if ($command->name === 'GIMG') {
                $job->onQueue('images');
            }

            dispatch($job);

            return response()->json([
                'message' => 'Command queued successfully',
                'log_id'  => $log->id,
            ]);
        } catch (Exception $e) {
            return response()->json(['message' => 'Failed to queue command: ' . $e->getMessage()], 400);
        }
    }

    /**
     * Show command log
     */
    public function getCommandLog($id): JsonResponse
    {
        try {
            $log = CommandLog::with('command')->findOrFail($id);
            $log->makeHidden('updated_at');
            $log->command->makeHidden([
                'allowed_sources',
                'allowed_destinations',
                'created_at',
                'updated_at',
            ]);
            return response()->json($log);
        } catch (Exception $e) {
            return response()->json(['message' => 'Failed to fetch command log: ' . $e->getMessage()], 400);
        }
    }

    /**
     * Get Command History
     */
    public function history(): JsonResponse
    {
        try {
            $history = CommandLog::with('command')
                ->orderBy('sent_at', 'desc')
                ->paginate(20);
            return response()->json($history);
        } catch (Exception $e) {
            return response()->json(['message' => 'Failed to fetch command history: ' . $e->getMessage()], 400);
        }
    }

    /**
     * Get all command replies
     */
    public function getReplies(): JsonResponse
    {
        try {
            $replies = $this->commandService->getAllReplies();
            return response()->json($replies);
        } catch (Exception $e) {
            return response()->json(['message' => 'Failed to fetch command replies: ' . $e->getMessage()], 400);
        }
    }

    /**
    * Download the image received for a GIMG command log.
     *
     * Route:  GET /api/commands/logs/{id}/image
     *
     * Looks up the `images` record whose command_log_id matches the log,
     * then streams the PNG from disk.
     */
    public function downloadImage(int $id): BinaryFileResponse|JsonResponse
    {
        try {
            $log = CommandLog::findOrFail($id);

            $imageRecord = Image::where('command_log_id', $log->id)->latest()->first();

            if (!$imageRecord) {
                return response()->json([
                    'message' => 'No image available for this command log. '
                        . 'Current status: "' . $log->status . '".',
                ], 404);
            }

            // Prefer enhanced version; fall back to original
            $relativePath = $imageRecord->enhanced_path ?? $imageRecord->original_path;

            if (!Storage::disk('public')->exists($relativePath)) {
                return response()->json([
                    'message'  => 'Image file not found on disk. It may have been deleted.',
                    'expected' => Storage::disk('public')->path($relativePath),
                ], 404);
            }

            $filePath = Storage::disk('public')->path($relativePath);

            return response()->file($filePath, [
                'Content-Type'        => mime_content_type($filePath) ?: 'application/octet-stream',
                'Content-Disposition' => 'attachment; filename="' . basename($filePath) . '"',
            ]);
        } catch (Exception $e) {
            return response()->json(['message' => 'Failed to download image: ' . $e->getMessage()], 400);
        }
    }

    /**
     * Macro goal API endpoint.
     *
     * Receives a high-level HTN goal request, expands it into primitive command tasks,
     * validates the resulting plan with the digital twin simulator, and stages the
     * approved commands in the CommandSchedule queue for future execution.
     *
     * Expected input:
     * - goal_name: HTN goal identifier (string)
     * - execute_at: timestamp when the macro goal should start
     * - parameters: optional goal-specific parameters
     */

    public function sendMacroGoal(Request $request): JsonResponse
    {
        $request->validate([
            'goal_name'  => 'required|string',
            'execute_at' => 'required|date|after_or_equal:now',
            'parameters' => 'nullable|array'
        ]);

        try {

            $goalEnum = HtnGoalType::tryFrom($request->input('goal_name'))
                ?? throw new InvalidArgumentException("Invalid HTN Goal identifier provided.");

            $startTime = Carbon::parse($request->input('execute_at'));

            // 1. Run your clean Enum-driven decomposition method
            $primitives = $this->autonomousService->decomposeGoal(
                $goalEnum,
                $startTime,
                $request->input('parameters', [])
            );

            // 2. Validate using the Digital Twin simulation sandbox
            $simulation = $this->autonomousService->runDigitalTwinSimulation($primitives);
            if (!$simulation['isValid']) {
                return response()->json([
                    'status'  => 'simulation_failed',
                    'details' => $simulation['reason']
                ], 422);
            }

            $scheduledIds = [];
            $batchUuid = (string) Str::uuid(); // Group these siblings in the scratchpad

            // 3. Populate our dedicated Command Schedules scratchpad
            foreach ($primitives as $task) {
                $command = Command::where('name', $task['cmd_name'])->firstOrFail();

                $this->commandService->validateCommandData($command->id, $task['data']);

                $schedule = CommandSchedule::create([
                    'batch_uuid'   => $batchUuid,
                    'macro_name'   => $goalEnum->value,
                    'command_id'   => $command->id,
                    'dest_address' => $task['dest'],
                    'data'         => $task['data'],
                    'execute_at'   => $task['execute'],
                    'status'       => 'pending'
                ]);

                $scheduledIds[] = $schedule->id;
            }

            return response()->json([
                'status'        => 'success',
                'message'       => "HTN Goal [{$goalEnum->name}] successfully expanded and staged.",
                'batch_uuid'    => $batchUuid,
                'scheduled_ids' => $scheduledIds
            ]);
        } catch (Exception $e) {
            return response()->json(['message' => 'Scheduling process failed: ' . $e->getMessage()], 400);
        }
    }


    /**
     * Stage a standalone Absolute Time Command (ATC) 
     */
    public function scheduleIndividualCommand(Request $request): JsonResponse
    {
        $request->validate([
            'command_id'           => 'required|integer|exists:commands,id',
            'dest_address'         => 'required|integer',
            'execute_at'           => 'required|date|after_or_equal:now',
            'data'                 => 'nullable|array',
            'data.pwrl_id'         => 'nullable|string',
            'data.image_id'        => 'nullable|integer',
            'data.timer_value'     => 'nullable|integer',
            'data.mode_id'         => 'nullable|string',
            'data.sequence_number' => 'nullable|integer',
            'data.window_size'     => 'nullable|integer',
            'data.tlm_frame_seq_no' => 'nullable|integer',
        ]);

        try {
            $command = $this->commandService->getCommandById($request->input('command_id'));
            $executeAt = Carbon::parse($request->input('execute_at'));

            // Filter out any null payload keys
            $payload = array_filter(
                $request->input('data', []),
                fn($value) => $value !== null
            );

            if (isset($payload['pwrl_id'])) {
                $enumCase = PowerLine::tryFrom($payload['pwrl_id']);
                if ($enumCase) {
                    $payload['pwrl_id'] = $enumCase; 
                }
            }

            // Run parameters validation using your core validation rules
            $this->commandService->validateCommandData($command->id, $payload);

            // Insert directly into our staging table scratchpad
            $scheduledTask = CommandSchedule::create([
                'batch_uuid'     => null,       
                'macro_name'     => 'MANUAL_ATC',
                'command_id'     => $command->id,
                'dest_address'   => $request->input('dest_address'),
                'data'           => $payload,
                'execute_at'     => $executeAt,
                'status'         => 'pending'
            ]);

            return response()->json([
                'status'       => 'success',
                'message'      => "Direct Absolute Time Command successfully scheduled for execution at {$executeAt->toDateTimeString()}.",
                'schedule_id'  => $scheduledTask->id
            ]);
        } catch (Exception $e) {
            return response()->json([
                'status'  => 'error',
                'message' => 'Failed to stage standalone scheduled command: ' . $e->getMessage()
            ], 400);
        }
    }
}
