from enum import StrEnum, auto


class AppEnv(StrEnum):
    PROD = auto()
    QA = auto()
    DEV = auto()


class AppInfraPlatform(StrEnum):
    ON_PREM_VM = auto()
    AWS_EC2 = auto()
    AWS_ECS_EC2 = auto()
    AWS_ECS_FARGATE = auto()


class AppHostPattern(StrEnum):
    ON_PREM_VM_NATIVE = auto()
    AWS_EC2_NATIVE = auto()
    AWS_EC2_CONTAINER = auto()
    AWS_ECS_CONTAINER = auto()


class AppHostPatternEnvFile(StrEnum):
    ON_PREM_VM_NATIVE = "on_prem_vm_native.env"
    AWS_EC2_NATIVE = "aws_ec2_native.env"
    AWS_EC2_CONTAINER = "aws_ec2_container.env"
    AWS_ECS_CONTAINER = "aws_ecs_container.env"


class StoragePlatform(StrEnum):
    NAS_STORAGE = auto()
    AWS_S3_STORAGE = auto()
    NAS_AWS_S3_STORAGE = auto()


class LogHandler(StrEnum):
    TIMED_ROTATING_FILE_HANDLER = auto()
    STREAM_HANDLER_STDOUT = auto()
    STREAM_HANDLER_STDERR = auto()
