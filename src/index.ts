export { default } from "./Cadenza";
import CadenzaService, {
  NetworkMode,
  SecurityProfile,
  type ServerOptions,
} from "./Cadenza";
import {
  Actor,
  GlobalDefinition,
  HelperDefinition,
  type ActorDefinition,
  type ActorSpec,
  type ActorStateDefinition,
  type ActorKeyDefinition,
  type ActorTaskBindingDefinition,
  type ActorFactoryOptions,
  type ActorLoadPolicy,
  type ActorWriteContract,
  type ActorRuntimeReadGuard,
  type ActorTaskMode,
  type ActorKind,
  type ActorConsistencyProfileName,
  type ActorInvocationOptions,
  type ActorStateStore,
  type ActorTaskBindingOptions,
  type ActorTaskContext,
  type ActorTaskHandler,
  type ActorStateReducer,
  type RetryPolicy,
  type IdempotencyPolicy,
  type SessionPolicy,
  type RuntimeValidationPolicy,
  type RuntimeValidationScope,
  type SignalDefinitionInput,
  type SignalDeliveryMode,
  type SignalMetadata,
  type SignalReceiverFilter,
  DebounceTask,
  EphemeralTask,
  GraphRoutine,
  Task,
} from "@cadenza.io/core";
import type {
  AnyObject,
  DebounceOptions,
  EphemeralTaskOptions,
  HelperFunction,
  RuntimeTools,
  TaskFunction,
  TaskOptions,
  ThrottleTagGetter,
} from "@cadenza.io/core";
import GraphMetadataController from "./graph/controllers/GraphMetadataController";
import DatabaseTask from "./graph/definition/DatabaseTask";
import DeputyTask from "./graph/definition/DeputyTask";
import SignalTransmissionTask from "./graph/definition/SignalTransmissionTask";
import RestController from "@service-rest-controller";
import SocketController from "./network/SocketController";
import ServiceRegistry, {
  DeputyDescriptor,
} from "./registry/ServiceRegistry";
import SignalController from "./signals/SignalController";
import {
  RUNTIME_VALIDATION_INTENTS,
  RUNTIME_VALIDATION_SIGNALS,
} from "./runtime/RuntimeValidationController";
import DatabaseController from "@service-database-controller";
import {
  buildExecutionPersistenceDependency,
  buildExecutionPersistenceEnsureEvent,
  buildExecutionPersistenceUpdateEvent,
  createExecutionPersistenceBundle,
  EXECUTION_PERSISTENCE_BUNDLE_SIGNAL,
  type ExecutionPersistenceBundle,
  type ExecutionPersistenceEnsureEntityType,
  type ExecutionPersistenceEnsureEvent,
  type ExecutionPersistenceEntityType,
  type ExecutionPersistenceEvent,
  type ExecutionPersistenceUpdateEntityType,
  type ExecutionPersistenceUpdateEvent,
} from "./execution/ExecutionPersistenceCoordinator";
import {
  BootstrapOptions,
  HydrationOptions,
  ResolvedBootstrapEndpoint,
} from "./utils/bootstrap";
import {
  createSSRInquiryBridge,
  SSRInquiryBridge,
  SSRInquiryBridgeOptions,
} from "./ssr/createSSRInquiryBridge";
import type {
  BrowserRuntimeActorHandle,
  BrowserRuntimeActorOptions,
  BrowserRuntimeActorRuntimeState,
  BrowserRuntimeProjectionBinding,
  BrowserRuntimeServiceOptions,
} from "./frontend/createBrowserRuntimeActor";
import {
  AggregateDefinition,
  AggregateFunction,
  DbOperationType,
  DbOperationPayload,
  JoinDefinition,
  OpEffect,
  QueryMode,
  SortDirection,
  SubOperation,
  SubOperationType,
  ValueOrSubOp,
} from "./types/queryData";
import type {
  DistributedInquiryMeta,
  DistributedInquiryOptions,
  InquiryResponderDescriptor,
  InquiryResponderStatus,
} from "./types/inquiry";
import type { ServiceInstanceDescriptor } from "./types/serviceRegistry";
import type {
  ServiceTransportConfig,
  ServiceTransportDescriptor,
  ServiceTransportProtocol,
  ServiceTransportRole,
  ServiceTransportSecurityProfile,
} from "./types/transport";
import type {
  DatabaseMigrationConstraintDefinition,
  DatabaseMigrationDefinition,
  DatabaseMigrationPolicy,
  DatabaseMigrationStep,
  DatabaseSchemaDefinition,
  FieldDefinition,
  TableDefinition,
} from "./types/database";
import {
  AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
  RUNTIME_STATUS_AUTHORITY_SYNC_REQUESTED_SIGNAL,
  buildAuthorityRuntimeStatusSignature,
  normalizeAuthorityRuntimeStatusReport,
  type AuthorityRuntimeStatusReport,
  type RuntimeMetricsHealthDetail,
} from "./registry/runtimeStatusContract";
import {
  AUTHORITY_BOOTSTRAP_FULL_SYNC_INTENT,
  AUTHORITY_BOOTSTRAP_SIGNAL_NAMES,
  AUTHORITY_SERVICE_INSTANCE_REGISTER_INTENT,
  AUTHORITY_SERVICE_INSTANCE_REGISTER_TASK_NAME,
  AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_INTENT,
  AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_TASK_NAME,
  getAuthorityBootstrapInsertIntentSpecForTable,
  isAuthorityBootstrapIntent,
  isAuthorityBootstrapSignal,
} from "./registry/authorityBootstrapControlPlane";
import {
  AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
  AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
  normalizeServiceManifestSnapshot,
  selectLatestServiceManifestSnapshots,
} from "./registry/serviceManifestContract";
import {
  buildServiceManifestSnapshot,
  explodeServiceManifestSnapshots,
} from "./registry/serviceManifest";
import type {
  ServiceManifestActorDefinition,
  ServiceManifestActorTaskMap,
  ServiceManifestDirectionalTaskMap,
  ServiceManifestIntentDefinition,
  ServiceManifestIntentTaskMap,
  ServiceManifestRoutineDefinition,
  ServiceManifestSignalDefinition,
  ServiceManifestSignalTaskMap,
  ServiceManifestSnapshot,
  ServiceManifestTaskDefinition,
  ServiceManifestTaskRoutineMap,
} from "./types/serviceManifest";

const emit = CadenzaService.emit.bind(CadenzaService) as typeof CadenzaService.emit;
const log = CadenzaService.log.bind(CadenzaService) as typeof CadenzaService.log;
const createTask = CadenzaService.createTask.bind(
  CadenzaService,
) as typeof CadenzaService.createTask;
const createMetaTask = CadenzaService.createMetaTask.bind(
  CadenzaService,
) as typeof CadenzaService.createMetaTask;
const createHelper = CadenzaService.createHelper.bind(
  CadenzaService,
) as typeof CadenzaService.createHelper;
const createMetaHelper = CadenzaService.createMetaHelper.bind(
  CadenzaService,
) as typeof CadenzaService.createMetaHelper;
const createGlobal = CadenzaService.createGlobal.bind(
  CadenzaService,
) as typeof CadenzaService.createGlobal;
const createMetaGlobal = CadenzaService.createMetaGlobal.bind(
  CadenzaService,
) as typeof CadenzaService.createMetaGlobal;
const createActor = CadenzaService.createActor.bind(
  CadenzaService,
) as typeof CadenzaService.createActor;
const createCadenzaService = CadenzaService.createCadenzaService.bind(
  CadenzaService,
) as typeof CadenzaService.createCadenzaService;
const createDatabaseService = CadenzaService.createDatabaseService.bind(
  CadenzaService,
) as typeof CadenzaService.createDatabaseService;
const createBrowserRuntimeActor = CadenzaService.createBrowserRuntimeActor.bind(
  CadenzaService,
) as typeof CadenzaService.createBrowserRuntimeActor;
const getServiceReadySignalName = CadenzaService.getServiceReadySignalName.bind(
  CadenzaService,
) as typeof CadenzaService.getServiceReadySignalName;
const inquiryBroker = CadenzaService.inquiryBroker;
const serviceRegistry = CadenzaService.serviceRegistry;

export type {
  BootstrapOptions,
  HydrationOptions,
  ResolvedBootstrapEndpoint,
  SSRInquiryBridge,
  SSRInquiryBridgeOptions,
  BrowserRuntimeActorHandle,
  BrowserRuntimeActorOptions,
  BrowserRuntimeActorRuntimeState,
  BrowserRuntimeProjectionBinding,
  BrowserRuntimeServiceOptions,
  ActorDefinition,
  ActorSpec,
  ActorStateDefinition,
  ActorKeyDefinition,
  ActorTaskBindingDefinition,
  ActorFactoryOptions,
  ActorLoadPolicy,
  ActorWriteContract,
  ActorRuntimeReadGuard,
  ActorTaskMode,
  ActorKind,
  ActorConsistencyProfileName,
  ActorInvocationOptions,
  ActorStateStore,
  ActorTaskBindingOptions,
  ActorTaskContext,
  ActorTaskHandler,
  ActorStateReducer,
  RetryPolicy,
  IdempotencyPolicy,
  SessionPolicy,
  ServiceInstanceDescriptor,
  ServiceTransportConfig,
  ServiceTransportDescriptor,
  ServiceTransportProtocol,
  ServiceTransportRole,
  ServiceTransportSecurityProfile,
  DeputyDescriptor,
  DbOperationType,
  QueryMode,
  AggregateFunction,
  AggregateDefinition,
  SortDirection,
  JoinDefinition,
  SubOperationType,
  SubOperation,
  OpEffect,
  ValueOrSubOp,
  DbOperationPayload,
  SecurityProfile,
  NetworkMode,
  ServerOptions,
  DistributedInquiryOptions,
  DistributedInquiryMeta,
  InquiryResponderDescriptor,
  InquiryResponderStatus,
  AnyObject,
  TaskOptions,
  ThrottleTagGetter,
  TaskFunction,
  HelperFunction,
  DebounceOptions,
  EphemeralTaskOptions,
  RuntimeTools,
  RuntimeValidationPolicy,
  RuntimeValidationScope,
  SignalDefinitionInput,
  SignalDeliveryMode,
  SignalMetadata,
  SignalReceiverFilter,
  DatabaseMigrationConstraintDefinition,
  DatabaseMigrationDefinition,
  DatabaseMigrationPolicy,
  DatabaseMigrationStep,
  DatabaseSchemaDefinition,
  ExecutionPersistenceBundle,
  ExecutionPersistenceEnsureEntityType,
  ExecutionPersistenceEnsureEvent,
  ExecutionPersistenceEntityType,
  ExecutionPersistenceEvent,
  ExecutionPersistenceUpdateEntityType,
  ExecutionPersistenceUpdateEvent,
  FieldDefinition,
  TableDefinition,
  AuthorityRuntimeStatusReport,
  RuntimeMetricsHealthDetail,
  ServiceManifestActorDefinition,
  ServiceManifestActorTaskMap,
  ServiceManifestDirectionalTaskMap,
  ServiceManifestIntentDefinition,
  ServiceManifestIntentTaskMap,
  ServiceManifestRoutineDefinition,
  ServiceManifestSignalDefinition,
  ServiceManifestSignalTaskMap,
  ServiceManifestSnapshot,
  ServiceManifestTaskDefinition,
  ServiceManifestTaskRoutineMap,
};
export {
  Actor,
  HelperDefinition,
  GlobalDefinition,
  GraphMetadataController,
  DeputyTask,
  DatabaseController,
  DatabaseTask,
  SignalTransmissionTask,
  RestController,
  SocketController,
  ServiceRegistry,
  SignalController,
  createSSRInquiryBridge,
  Task,
  DebounceTask,
  EphemeralTask,
  GraphRoutine,
  emit,
  log,
  createTask,
  createMetaTask,
  createHelper,
  createMetaHelper,
  createGlobal,
  createMetaGlobal,
  createActor,
  createCadenzaService,
  createDatabaseService,
  createBrowserRuntimeActor,
  buildExecutionPersistenceDependency,
  buildExecutionPersistenceEnsureEvent,
  buildExecutionPersistenceUpdateEvent,
  createExecutionPersistenceBundle,
  EXECUTION_PERSISTENCE_BUNDLE_SIGNAL,
  AUTHORITY_BOOTSTRAP_FULL_SYNC_INTENT,
  AUTHORITY_RUNTIME_STATUS_REPORT_INTENT,
  RUNTIME_STATUS_AUTHORITY_SYNC_REQUESTED_SIGNAL,
  AUTHORITY_SERVICE_MANIFEST_REPORT_INTENT,
  AUTHORITY_SERVICE_MANIFEST_UPDATED_SIGNAL,
  AUTHORITY_BOOTSTRAP_SIGNAL_NAMES,
  AUTHORITY_SERVICE_INSTANCE_REGISTER_INTENT,
  AUTHORITY_SERVICE_INSTANCE_REGISTER_TASK_NAME,
  AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_INTENT,
  AUTHORITY_SERVICE_INSTANCE_TRANSPORT_REGISTER_TASK_NAME,
  getAuthorityBootstrapInsertIntentSpecForTable,
  isAuthorityBootstrapIntent,
  isAuthorityBootstrapSignal,
  normalizeAuthorityRuntimeStatusReport,
  buildAuthorityRuntimeStatusSignature,
  normalizeServiceManifestSnapshot,
  selectLatestServiceManifestSnapshots,
  buildServiceManifestSnapshot,
  explodeServiceManifestSnapshots,
  getServiceReadySignalName,
  inquiryBroker,
  serviceRegistry,
  RUNTIME_VALIDATION_INTENTS,
  RUNTIME_VALIDATION_SIGNALS,
};
