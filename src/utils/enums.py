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


# class AppHostPatternEnvFile(StrEnum):
#     ON_PREM_VM_NATIVE = "app_env.on_prem_vm_native.dev.env"
#     AWS_EC2_NATIVE = "app_env.aws_ec2_native.dev.env"
#     AWS_EC2_CONTAINER = "app_env.aws_ec2_container.dev.env"
#     AWS_ECS_CONTAINER = "app_env.aws_ecs_container.dev.env"

# class AppHostPatternDefaultEnvFile(StrEnum):
#     ON_PREM_VM_NATIVE = ".env.default"
#     AWS_EC2_NATIVE = ".env.default"
#     AWS_EC2_CONTAINER = "global_env.aws_ec2_container.dev.env"
#     AWS_ECS_CONTAINER = ".env.default.aws_ecs_container.env"


class StoragePlatform(StrEnum):
    NAS_STORAGE = auto()
    AWS_S3_STORAGE = auto()
    NAS_AWS_S3_STORAGE = auto()


class LogHandler(StrEnum):
    TIMED_ROTATING_FILE_HANDLER = auto()
    STREAM_HANDLER_STDOUT = auto()
    STREAM_HANDLER_STDERR = auto()


class SparkHostPattern(StrEnum):
    ON_PREM_VM_NATIVE = auto()
    AWS_EC2_NATIVE = auto()
    AWS_EC2_CONTAINER = auto()
    AWS_EMR_CLUSTER = auto()


class SparkClusterManager(StrEnum):
    LOCAL = auto()
    STANDALONE = auto()
    YARN = auto()
    KUBERNETES = auto()


class SparkDeployMode(StrEnum):
    LOCAL = auto()
    CLIENT = auto()
    CLUSTER = auto()
