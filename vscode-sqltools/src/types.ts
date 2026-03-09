/**
 * Credentials provided by the user in the SQLTools connection form.
 * branchId and workspaceId are optional -- auto-detected when omitted.
 */
export interface KeboolaCredentials {
  keboolaStack: string;
  customConnectionUrl?: string;
  token: string;
  branchId?: string;
  workspaceId?: string;
}

/** Response from POST /api/v1/branches/{b}/workspaces/{w}/queries */
export interface QueryJobResponse {
  queryJobId: string;
  sessionId?: string;
}

/** Response from GET /api/v1/queries/{queryJobId} (job status polling) */
export interface JobStatusResponse {
  queryJobId: string;
  status: string;
  statements: StatementStatusResponse[];
}

/** Status of an individual statement within a query job */
export interface StatementStatusResponse {
  id: string;
  status: string;
  error?: { message: string };
  numberOfRows?: number;
}

/** Response from GET /api/v1/statements/{statementId}/result (paginated results) */
export interface QueryResultResponse {
  columns: { name: string; type?: string }[];
  data: any[][];
  numberOfRows?: number;
}

/** A development branch from GET /v2/storage/dev-branches */
export interface BranchResponse {
  id: number;
  name: string;
  isDefault: boolean;
}

/** A workspace from GET /v2/storage/workspaces */
export interface WorkspaceResponse {
  id: number;
  name: string;
  type: string;
}

/** A storage bucket from GET /v2/storage/buckets */
export interface BucketInfo {
  id: string;
  name: string;
  stage: string;
  description?: string;
}

/** A table within a bucket from GET /v2/storage/buckets/{id}/tables */
export interface TableInfo {
  id: string;
  name: string;
  primaryKey?: string[];
}
