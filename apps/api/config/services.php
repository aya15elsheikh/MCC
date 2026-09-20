<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Third Party Services
    |--------------------------------------------------------------------------
    |
    | This file is for storing the credentials for third party services such
    | as Mailgun, Postmark, AWS and more. This file provides the de facto
    | location for this type of information, allowing packages to have
    | a conventional file to locate the various service credentials.
    |
    */

    'postmark' => [
        'key' => env('POSTMARK_API_KEY'),
    ],

    'resend' => [
        'key' => env('RESEND_API_KEY'),
    ],

    'ses' => [
        'key' => env('AWS_ACCESS_KEY_ID'),
        'secret' => env('AWS_SECRET_ACCESS_KEY'),
        'region' => env('AWS_DEFAULT_REGION', 'us-east-1'),
    ],

    'slack' => [
        'notifications' => [
            'bot_user_oauth_token' => env('SLACK_BOT_USER_OAUTH_TOKEN'),
            'channel' => env('SLACK_BOT_USER_DEFAULT_CHANNEL'),
        ],
    ],

    'anomaly_api' => [
        'url'     => env('ANOMALY_API_URL', 'http://host.docker.internal:8000'),
        'key'     => env('ANOMALY_API_KEY', 'change-me-in-production'),
        'timeout' => env('ANOMALY_API_TIMEOUT', 15),
    ],

    'enhancement_api' => [
        'url'     => env('ENHANCEMENT_API_URL', 'http://host.docker.internal:8001'),
        'timeout' => env('ENHANCEMENT_API_TIMEOUT', 15),
    ],

    'object_detection_api' => [
        'url'     => env('OBJECT_DETECTION_API_URL', 'http://host.docker.internal:8002'),
        'timeout' => env('OBJECT_DETECTION_API_TIMEOUT', 15),
    ],

    'panorama_api' => [
        'url' => env('PANORAMA_API_URL', 'http://host.docker.internal:8003'), 
    ],

    'satellite' => [
        'url'     => env('SATELLITE_API_URL', 'http://host.docker.internal:8081'),
        'timeout' => env('SATELLITE_API_TIMEOUT', 15),
    ],

    'command' => [
        'url'     => env('COMMAND_API_URL', 'ws://host.docker.internal:8081/ws/radio'),
        'timeout' => env('COMMAND_API_TIMEOUT', 15),
    ],

    'space_keys' => [
        'enabled'      => env('SPACE_KEYS_IMAGE_ENABLED', false),
        'source'       => env('SPACE_KEYS_SOURCE_ADDRESS', 0x01),
        'destination'  => env('SPACE_KEYS_DESTINATION_ADDRESS', 0x07),
        'image_url'    => env('SPACE_KEYS_IMAGE_URL', 'http://192.168.4.1/image'),
        'http_timeout' => env('SPACE_KEYS_IMAGE_TIMEOUT', 15),
    ],

    'decoder' => [
        'url'     => env('DECODER_API_URL', 'http://host.docker.internal:8082'),
        'timeout' => env('DECODER_API_TIMEOUT', 15),
    ],
];
