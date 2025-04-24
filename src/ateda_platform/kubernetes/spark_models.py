# src/ateda_platform/kubernetes/spark_models.py

from typing import Dict, Optional, List
from pydantic import BaseModel, Field as PydanticField

# Define the SparkApplication group and version here as they are used in defaults
SPARK_OP_GROUP = "sparkoperator.k8s.io"
SPARK_OP_VERSION = "v1beta2"
SPARK_OP_PLURAL = "sparkapplications"

# --- Pydantic Models for SparkApplication CRD ---
# NOTE: Using PydanticField for Pydantic's Field to avoid conflict with Dagster's Field
# (Dagster Field isn't needed here anyway)


class ObjectMeta(BaseModel):
    name: str
    namespace: str
    labels: Optional[Dict[str, str]] = None


class EmptyDirVolumeSource(BaseModel):
    pass  # Represents {}


class HostPathVolumeSource(BaseModel):
    path: str
    type: Optional[str] = None


class PersistentVolumeClaimSource(BaseModel):
    claimName: str
    readOnly: Optional[bool] = PydanticField(default=None)


class Volume(BaseModel):
    name: str
    emptyDir: Optional[EmptyDirVolumeSource] = PydanticField(default=None)
    hostPath: Optional[HostPathVolumeSource] = PydanticField(default=None)
    persistentVolumeClaim: Optional[PersistentVolumeClaimSource] = PydanticField(
        default=None
    )


class VolumeMount(BaseModel):
    name: str
    mountPath: str


class PodSpecBase(BaseModel):  # Base for Driver/Executor
    cores: Optional[int] = None
    memory: Optional[str] = None
    labels: Optional[Dict[str, str]] = None
    serviceAccount: Optional[str] = PydanticField(default=None)
    # Spark Operator supports both env (list of dicts) and envVars (dict)
    # Using envVars dict for simplicity here as it was used before.
    envVars: Optional[Dict[str, str]] = PydanticField(default=None)
    volumeMounts: Optional[List[VolumeMount]] = PydanticField(default=None)

    model_config = {
        "populate_by_name": True
    }  # Allow using field names without alias for init


class DriverSpec(PodSpecBase):
    pass


class ExecutorSpec(PodSpecBase):
    instances: Optional[int] = None


class DynamicAllocation(BaseModel):
    enabled: bool
    minExecutors: Optional[int] = PydanticField(default=None)
    maxExecutors: Optional[int] = PydanticField(default=None)

    model_config = {"populate_by_name": True}


class RestartPolicy(BaseModel):
    type: str  # e.g., "Never"


class SparkApplicationSpec(BaseModel):
    type: str
    pythonVersion: Optional[str] = PydanticField(default=None)
    mode: str
    image: str
    imagePullPolicy: Optional[str] = PydanticField(default=None)
    mainApplicationFile: str
    arguments: Optional[List[str]] = None
    sparkVersion: str
    restartPolicy: RestartPolicy
    volumes: Optional[List[Volume]] = None
    driver: DriverSpec
    executor: ExecutorSpec
    dynamicAllocation: Optional[DynamicAllocation] = PydanticField(default=None)
    sparkConf: Optional[Dict[str, str]] = PydanticField(default=None)

    model_config = {"populate_by_name": True}


class SparkApplication(BaseModel):
    # Use PydanticField with alias for fields clashing with Python keywords or needing specific K8s naming
    apiVersion: str = PydanticField(default=f"{SPARK_OP_GROUP}/{SPARK_OP_VERSION}")
    kind: str = PydanticField(default="SparkApplication")
    metadata: ObjectMeta
    spec: SparkApplicationSpec

    # Pydantic V2 config
    model_config = {
        "exclude_none": True,  # Don't include None values in the output dict
        "populate_by_name": True,  # Allow using Python names for aliased fields during instantiation
        "extra": "ignore",  # Ignore extra fields during instantiation if needed, 'forbid' is stricter
        "by_alias": False,  # Default serialization uses field names, use True for aliases if needed in output
    }

    # Helper to get dict compatible with K8s client (uses aliases)
    def to_dict(self) -> Dict:
        # Use model_dump with by_alias=True for the final K8s submission
        return self.model_dump(mode="python", by_alias=True, exclude_none=True)
