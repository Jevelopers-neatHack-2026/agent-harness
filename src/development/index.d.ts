export interface DevelopmentSourceManifest {
  application: string;
  agent_harness: string;
  creative_engine: string;
  event_intelligence?: string;
  finance?: string;
}

export declare function invokeAgentCreativeDevelopment<Request, Response>(options: {
  host: {
    environment: 'development' | 'test';
    source_manifest: DevelopmentSourceManifest;
    invoke(request: Request): Promise<Response>;
  };
  request: Request;
  source_manifest: DevelopmentSourceManifest;
}): Promise<Readonly<{
  source_manifest: Readonly<DevelopmentSourceManifest>;
  evidence_mode: 'development_only';
  response: Response;
}>>;
