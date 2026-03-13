export type ServiceTransportRole = "internal" | "public";
export type ServiceTransportProtocol = "rest" | "socket";
export type ServiceTransportSecurityProfile = "low" | "medium" | "high";

export interface ServiceTransportConfig {
  role: ServiceTransportRole;
  origin: string;
  protocols?: ServiceTransportProtocol[];
  securityProfile?: ServiceTransportSecurityProfile | null;
  authStrategy?: string | null;
}

export interface ServiceTransportDescriptor {
  uuid: string;
  serviceInstanceId: string;
  role: ServiceTransportRole;
  origin: string;
  protocols: ServiceTransportProtocol[];
  securityProfile: ServiceTransportSecurityProfile | null;
  authStrategy: string | null;
  deleted?: boolean;
  clientCreated?: boolean;
}
