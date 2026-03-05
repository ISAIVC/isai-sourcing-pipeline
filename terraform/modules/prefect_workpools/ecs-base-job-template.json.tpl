{
  "job_configuration": {
    "env": "{{ env }}",
    "name": "{{ name }}",
    "labels": "{{ labels }}",
    "vpc_id": "{{ vpc_id }}",
    "cluster": "{{ cluster }}",
    "command": "{{ command }}",
    "container_name": "{{ container_name }}",
    "aws_credentials": "{{ aws_credentials }}",
    "prefect_api_key_secret_arn": "{{ prefect_api_key_secret_arn }}",
    "task_definition": {
        "cpu": "{{ cpu }}",
        "family": "{{ family }}",
        "memory": "{{ memory }}",
        "executionRoleArn": "{{ execution_role_arn }}",
        "containerDefinitions": [
            {
                "name": "{{ container_name }}",
                "image": "{{ image }}",
                "secrets": [
                    {
                        "name": "GOOGLE_CREDENTIALS",
                        "valueFrom": "${secrets_arn}:google-credentials::"
                    },
                    {
                        "name": "DEALROOM_API_KEY",
                        "valueFrom": "${secrets_arn}:dealroom-api-key::"
                    },
                    {
                        "name": "SUPABASE_URL",
                        "valueFrom": "${secrets_arn}:supabase-url::"
                    },
                    {
                        "name": "SUPABASE_SERVICE_ROLE_KEY",
                        "valueFrom": "${secrets_arn}:supabase-service-role-key::"
                    },
                    {
                        "name": "ATTIO_BY_TOKEN",
                        "valueFrom": "${secrets_arn}:attio-by-token::"
                    },
                    {
                        "name": "ATTIO_CG_TOKEN",
                        "valueFrom": "${secrets_arn}:attio-cg-token::"
                    }
                ]
            }
        ]
    },
    "task_run_request": {
        "tags": "{{ labels }}",
        "cluster": "{{ cluster }}",
        "overrides": {
            "cpu": "{{ cpu }}",
            "memory": "{{ memory }}",
            "taskRoleArn": "{{ task_role_arn }}",
            "containerOverrides": [
                {
                    "cpu": "{{ cpu }}",
                    "name": "{{ container_name }}",
                    "memory": "{{ memory }}",
                    "command": "{{ command }}",
                    "environment": "{{ env }}"
                }
            ]
        },
        "launchType": "{{ launch_type }}",
        "taskDefinition": "{{ task_definition_arn }}"
    },
    "network_configuration": "{{ network_configuration }}",
    "cloudwatch_logs_options": "{{ cloudwatch_logs_options }}",
    "configure_cloudwatch_logs": "{{ configure_cloudwatch_logs }}",
    "task_start_timeout_seconds": "{{ task_start_timeout_seconds }}",
    "auto_deregister_task_definition": "{{ auto_deregister_task_definition }}"
},
  "variables": {
    "description": "Variables for templating an ECS job.",
    "type": "object",
    "required": [
      "aws_credentials"
    ],
    "properties": {
      "cpu": {
        "type": "integer",
        "title": "CPU",
        "description": "The amount of CPU to provide to the ECS task. Valid amounts are specified in the AWS documentation. If not provided, a default value of 1024 will be used unless present on the task definition.",
        "default": ${pool_cpu}
      },
      "env": {
        "type": "object",
        "title": "Environment Variables",
        "description": "Environment variables to provide to the task run. These variables are set on the Prefect container at task runtime. These will not be set on the task definition.",
        "additionalProperties": {
          "type": "string"
        },
        "default": ${envvars}
      },
      "name": {
        "type": "string",
        "title": "Name",
        "description": "Name given to created infrastructure."
      },
      "image": {
        "type": "string",
        "title": "Image",
        "description": "The image to use for the Prefect container in the task. If this value is not null, it will override the value in the task definition. This value defaults to a Prefect base image matching your local versions.",
        "default": "${image}"
      },
      "family": {
        "type": "string",
        "title": "Family",
        "description": "A family for the task definition. If not provided, it will be inferred from the task definition. If the task definition does not have a family, the name will be generated. When flow and deployment metadata is available, the generated name will include their names. Values for this field will be slugified to match AWS character requirements."
      },
      "labels": {
        "type": "object",
        "title": "Labels",
        "description": "Labels applied to created infrastructure.",
        "additionalProperties": {
          "type": "string"
        }
      },
      "memory": {
        "type": "integer",
        "title": "Memory",
        "description": "The amount of memory to provide to the ECS task. Valid amounts are specified in the AWS documentation. If not provided, a default value of 2048 will be used unless present on the task definition.",
        "default": ${pool_memory}
      },
      "vpc_id": {
        "type": "string",
        "title": "VPC ID",
        "default": "${vpc_id}",
        "description": "The AWS VPC to link the task run to. This is only applicable when using the 'awsvpc' network mode for your task. FARGATE tasks require this network  mode, but for EC2 tasks the default network mode is 'bridge'. If using the 'awsvpc' network mode and this field is null, your default VPC will be used. If no default VPC can be found, the task run will fail."
      },
      "cluster": {
        "type": "string",
        "title": "Cluster",
        "default": "${cluster}",
        "description": "The ECS cluster to run the task in. An ARN or name may be provided. If not provided, the default cluster will be used."
      },
      "command": {
        "type": "string",
        "title": "Command",
        "description": "The command to use when starting a flow run. In most cases, this should be left blank and the command will be automatically generated.",
        "default": "${command}"
      },
      "launch_type": {
        "enum": [
          "FARGATE",
          "EC2",
          "EXTERNAL",
          "FARGATE_SPOT"
        ],
        "type": "string",
        "title": "Launch Type",
        "default": "FARGATE",
        "description": "The type of ECS task run infrastructure that should be used. Note that 'FARGATE_SPOT' is not a formal ECS launch type, but we will configure the proper capacity provider strategy if set here."
      },
      "task_role_arn": {
        "type": "string",
        "title": "Task Role ARN",
        "default": "${task_role_arn}",
        "description": "A role to attach to the task run. This controls the permissions of the task while it is running."
      },
      "container_name": {
        "type": "string",
        "title": "Container Name",
        "description": "The name of the container flow run orchestration will occur in. If not specified, a default value of prefect will be used and if that is not found in the task definition the first container will be used.",
        "default": "${container_name}"
      },
      "aws_credentials": {
        "title": "AWS Credentials",
        "default": {
          "$ref": {
            "block_document_id": "${block_document_id}"
          }
        },
        "description": "The AWS credentials to use to connect to ECS. `aws_access_key_id`, `aws_secret_access_key` and `region_name` are required.",
        "allOf": [
          {
            "type": "object",
            "title": "AwsCredentials",
            "properties": {
              "region_name": {
                "type": "string",
                "title": "Region Name",
                "description": "The AWS Region where you want to create new connections."
              },
              "profile_name": {
                "type": "string",
                "title": "Profile Name",
                "description": "The profile to use when creating your session."
              },
              "aws_access_key_id": {
                "type": "string",
                "title": "AWS Access Key ID",
                "description": "A specific AWS access key ID."
              },
              "aws_session_token": {
                "type": "string",
                "title": "AWS Session Token",
                "description": "The session key for your AWS account. This is only needed when you are using temporary credentials."
              },
              "aws_client_parameters": {
                "title": "AWS Client Parameters",
                "description": "Extra parameters to initialize the Client.",
                "allOf": [
                  {
                    "type": "object",
                    "title": "AwsClientParameters",
                    "description": "Model used to manage extra parameters that you can pass when you initialize\nthe Client. If you want to find more information, see\n[boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)\nfor more info about the possible client configurations.\n\nAttributes:\n    api_version: The API version to use. By default, botocore will\n        use the latest API version when creating a client. You only need\n        to specify this parameter if you want to use a previous API version\n        of the client.\n    use_ssl: Whether or not to use SSL. By default, SSL is used.\n        Note that not all services support non-ssl connections.\n    verify: Whether or not to verify SSL certificates. By default\n        SSL certificates are verified. If False, SSL will still be used\n        (unless use_ssl is False), but SSL certificates\n        will not be verified. Passing a file path to this is deprecated.\n    verify_cert_path: A filename of the CA cert bundle to\n        use. You can specify this argument if you want to use a\n        different CA cert bundle than the one used by botocore.\n    endpoint_url: The complete URL to use for the constructed\n        client. Normally, botocore will automatically construct the\n        appropriate URL to use when communicating with a service. You\n        can specify a complete URL (including the \"http/https\" scheme)\n        to override this behavior. If this value is provided,\n        then ``use_ssl`` is ignored.\n    config: Advanced configuration for Botocore clients. See\n        [botocore docs](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html)\n        for more details.",
                    "properties": {
                      "config": {
                        "type": "object",
                        "title": "Botocore Config",
                        "description": "Advanced configuration for Botocore clients."
                      },
                      "verify": {
                        "title": "Verify",
                        "default": true,
                        "description": "Whether or not to verify SSL certificates.",
                        "anyOf": [
                          {
                            "type": "boolean"
                          },
                          {
                            "type": "string",
                            "format": "file-path"
                          }
                        ]
                      },
                      "use_ssl": {
                        "type": "boolean",
                        "title": "Use SSL",
                        "default": true,
                        "description": "Whether or not to use SSL."
                      },
                      "api_version": {
                        "type": "string",
                        "title": "API Version",
                        "description": "The API version to use."
                      },
                      "endpoint_url": {
                        "type": "string",
                        "title": "Endpoint URL",
                        "description": "The complete URL to use for the constructed client."
                      },
                      "verify_cert_path": {
                        "type": "string",
                        "title": "Certificate Authority Bundle File Path",
                        "format": "file-path",
                        "description": "Path to the CA cert bundle to use."
                      }
                    }
                  }
                ]
              },
              "aws_secret_access_key": {
                "type": "string",
                "title": "AWS Access Key Secret",
                "format": "password",
                "writeOnly": true,
                "description": "A specific AWS secret access key."
              }
            },
            "description": "Block used to manage authentication with AWS. AWS authentication is handled via the `boto3` module. For more information refer to the \n[boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).",
            "secret_fields": [
              "aws_secret_access_key"
            ],
            "block_type_slug": "aws-credentials",
            "block_schema_references": {}
          }
        ]
      },
      "execution_role_arn": {
        "type": "string",
        "title": "Execution Role ARN",
        "default": "${execution_role_arn}",
        "description": "An execution role to use for the task. This controls the permissions of the task when it is launching. If this value is not null, it will override the value in the task definition. An execution role must be provided to capture logs from the container."
      },
      "prefect_api_key_secret_arn": {
        "type": "string",
        "title": "Prefect API Key Secret ARN",
        "default": "${secrets_arn}:prefect-api-key::",
        "description": "The ARN of the AWS Secrets Manager secret containing the Prefect API key. The worker will automatically inject this as the PREFECT_API_KEY environment variable."
      },
      "task_definition_arn": {
        "type": "string",
        "title": "Task Definition Arn",
        "description": "An identifier for an existing task definition to use. If set, options that require changes to the task definition will be ignored. All contents of the task definition in the job configuration will be ignored."
      },
      "network_configuration": {
        "type": "object",
        "title": "Network Configuration",
        "default": {
          "subnets": ${jsonencode(subnets)},
          "assignPublicIp": "ENABLED",
          "securityGroups": ${jsonencode(security_groups)}
        },
        "description": "Apply a custom network configuration. See the [AWS documentation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-ecs-service-awsvpcconfiguration.html) for available options."
      },
      "cloudwatch_logs_options": {
        "type": "object",
        "title": "Cloudwatch Logs Options",
        "default": { "awslogs-group": "${log_group_name}" },
        "description": "When `configure_cloudwatch_logs` is enabled, this setting may be used to pass additional options to the CloudWatch logs configuration or override the default options. See the [AWS documentation](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/using_awslogs.html#create_awslogs_logdriver_options.) for available options. ",
        "additionalProperties": {
          "type": "string"
        }
      },
      "configure_cloudwatch_logs": {
        "type": "boolean",
        "title": "Configure Cloudwatch Logs",
        "default": true,
        "description": "If enabled, the Prefect container will be configured to send its output to the AWS CloudWatch logs service. This functionality requires an execution role with logs:CreateLogStream, logs:CreateLogGroup, and logs:PutLogEvents permissions. The default for this field is `False` unless `stream_output` is set."
      },
      "task_start_timeout_seconds": {
        "type": "integer",
        "title": "Task Start Timeout Seconds",
        "default": 300,
        "description": "The amount of time to watch for the start of the ECS task before marking it as failed. The task must enter a RUNNING state to be considered started."
      },
      "auto_deregister_task_definition": {
        "type": "boolean",
        "title": "Auto Deregister Task Definition",
        "default": false,
        "description": "If enabled, any task definitions that are created by this block will be deregistered. Existing task definitions linked by ARN will never be deregistered. Deregistering a task definition does not remove it from your AWS account, instead it will be marked as INACTIVE."
      }
    } ,
    "definitions": {
      "AwsClientParameters": {
        "title": "AwsClientParameters",
        "description": "Model used to manage extra parameters that you can pass when you initialize\nthe Client. If you want to find more information, see\n[boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html)\nfor more info about the possible client configurations.\n\nAttributes:\n    api_version: The API version to use. By default, botocore will\n        use the latest API version when creating a client. You only need\n        to specify this parameter if you want to use a previous API version\n        of the client.\n    use_ssl: Whether or not to use SSL. By default, SSL is used.\n        Note that not all services support non-ssl connections.\n    verify: Whether or not to verify SSL certificates. By default\n        SSL certificates are verified. If False, SSL will still be used\n        (unless use_ssl is False), but SSL certificates\n        will not be verified. Passing a file path to this is deprecated.\n    verify_cert_path: A filename of the CA cert bundle to\n        use. You can specify this argument if you want to use a\n        different CA cert bundle than the one used by botocore.\n    endpoint_url: The complete URL to use for the constructed\n        client. Normally, botocore will automatically construct the\n        appropriate URL to use when communicating with a service. You\n        can specify a complete URL (including the \"http/https\" scheme)\n        to override this behavior. If this value is provided,\n        then ``use_ssl`` is ignored.\n    config: Advanced configuration for Botocore clients. See\n        [botocore docs](https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html)\n        for more details.",
        "type": "object",
        "properties": {
          "api_version": {
            "title": "API Version",
            "description": "The API version to use.",
            "type": "string"
          },
          "use_ssl": {
            "title": "Use SSL",
            "description": "Whether or not to use SSL.",
            "default": true,
            "type": "boolean"
          },
          "verify": {
            "title": "Verify",
            "description": "Whether or not to verify SSL certificates.",
            "default": true,
            "anyOf": [
              {
                "type": "boolean"
              },
              {
                "type": "string",
                "format": "file-path"
              }
            ]
          },
          "verify_cert_path": {
            "title": "Certificate Authority Bundle File Path",
            "description": "Path to the CA cert bundle to use.",
            "format": "file-path",
            "type": "string"
          },
          "endpoint_url": {
            "title": "Endpoint URL",
            "description": "The complete URL to use for the constructed client.",
            "type": "string"
          },
          "config": {
            "title": "Botocore Config",
            "description": "Advanced configuration for Botocore clients.",
            "type": "object"
          }
        }
      },
      "AwsCredentials": {
        "title": "AwsCredentials",
        "description": "Block used to manage authentication with AWS. AWS authentication is handled via the `boto3` module. For more information refer to the \n[boto3 docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).",
        "type": "object",
        "properties": {
          "aws_access_key_id": {
            "title": "AWS Access Key ID",
            "description": "A specific AWS access key ID.",
            "type": "string"
          },
          "aws_secret_access_key": {
            "title": "AWS Access Key Secret",
            "description": "A specific AWS secret access key.",
            "type": "string",
            "writeOnly": true,
            "format": "password"
          },
          "aws_session_token": {
            "title": "AWS Session Token",
            "description": "The session key for your AWS account. This is only needed when you are using temporary credentials.",
            "type": "string"
          },
          "profile_name": {
            "title": "Profile Name",
            "description": "The profile to use when creating your session.",
            "type": "string"
          },
          "region_name": {
            "title": "Region Name",
            "description": "The AWS Region where you want to create new connections.",
            "type": "string"
          },
          "aws_client_parameters": {
            "title": "AWS Client Parameters",
            "description": "Extra parameters to initialize the Client.",
            "allOf": [
              {
                "$ref": "#/definitions/AwsClientParameters"
              }
            ]
          }
        },
        "block_type_slug": "aws-credentials",
        "secret_fields": [
          "aws_secret_access_key"
        ],
        "block_schema_references": {}
      }
    }
  }
}
