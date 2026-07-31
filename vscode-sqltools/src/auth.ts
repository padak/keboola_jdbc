import {
  AUTHORIZATION_HEADER,
  BEARER_SCHEME,
  HTTP_TIMEOUT_MS,
  PAT_DISCOVERY_PATH,
  PAT_TOKEN_PREFIX,
  PROJECT_ID_HEADER,
  STORAGE_API_TOKEN_HEADER,
} from './constants';
import { PatListResponse, PatProject } from './types';

/** Credential kind carried by the single token field of the connection form */
export type AuthMode = 'storageApiToken' | 'pat';

/** Credential subset needed to authenticate an API request */
export interface AuthContext {
  token: string;
  projectId?: string;
}

/** Shown when the stack does not have the programmatic-auth feature enabled */
export const PAT_NOT_ENABLED_MESSAGE =
  'PAT support is not enabled on this Keboola stack. Use a Storage API token instead.';

/** Shown when the stack rejects the bearer credential */
export const PAT_INVALID_MESSAGE =
  'The Personal Access Token is invalid, expired or revoked.';

/** Shown when the token authenticates but reaches no project */
export const PAT_NO_PROJECTS_MESSAGE =
  'The Personal Access Token grants access to no project. ' +
  'Ask a Keboola project administrator for access, or use a Storage API token.';

/** Shown where a project cannot be picked interactively */
export const PAT_AMBIGUOUS_PROJECT_MESSAGE =
  'The Personal Access Token can access several projects. ' +
  'Set Project ID in the connection settings to choose one.';

/**
 * Derives the credential kind from the token itself: the connection form has a
 * single token field, so the prefix is what tells the two kinds apart.
 */
export function detectAuthMode(token: string | null | undefined): AuthMode {
  if (token && token.trim().startsWith(PAT_TOKEN_PREFIX)) {
    return 'pat';
  }
  return 'storageApiToken';
}

/**
 * Builds the authentication headers for a Storage API or Query Service request.
 * A Personal Access Token needs a project because it is not scoped to one.
 */
export function buildAuthHeaders(auth: AuthContext): Record<string, string> {
  const token = (auth.token || '').trim();
  if (detectAuthMode(token) === 'storageApiToken') {
    return { [STORAGE_API_TOKEN_HEADER]: token };
  }

  const projectId = (auth.projectId || '').trim();
  if (!projectId) {
    throw new Error(
      'Project ID is required when connecting with a Personal Access Token.'
    );
  }
  return {
    ...buildBearerHeaders(token),
    [PROJECT_ID_HEADER]: projectId,
  };
}

/** Bearer-only headers, for the /v1/auth/* routes that take no project */
export function buildBearerHeaders(token: string): Record<string, string> {
  return { [AUTHORIZATION_HEADER]: `${BEARER_SCHEME} ${(token || '').trim()}` };
}

/**
 * Collects the projects a Personal Access Token can reach.
 *
 * The listing covers the calling token and its descendants, and a descendant can
 * never exceed its parent, so the union over all items is exactly what the caller
 * reaches. Ids stay strings because they can exceed the safe integer range.
 */
export function parseAccessibleProjects(payload: PatListResponse | null | undefined): PatProject[] {
  const byId = new Map<string, PatProject>();
  for (const item of payload?.items || []) {
    for (const project of item?.projects || []) {
      if (project?.id === undefined || project?.id === null) {
        continue;
      }
      const id = String(project.id);
      if (!byId.has(id)) {
        byId.set(id, { id, name: project.name || `Project ${id}` });
      }
    }
  }
  return [...byId.values()];
}

/** Maps a failed discovery response to a message a user can act on */
export function mapDiscoveryError(status: number, body: string): Error {
  if (status === 404) {
    return new Error(PAT_NOT_ENABLED_MESSAGE);
  }
  if (status === 401) {
    return new Error(PAT_INVALID_MESSAGE);
  }
  const detail = extractErrorDetail(body);
  return new Error(
    `Failed to list projects for the Personal Access Token (HTTP ${status}${detail ? ': ' + detail : ''}).`
  );
}

/**
 * Lists the projects the given Personal Access Token can reach.
 * Shared by the extension host and the language server so both report the
 * same diagnosis for the same stack response.
 */
export async function fetchAccessibleProjects(
  connectionUrl: string,
  token: string
): Promise<PatProject[]> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);
  try {
    const response = await fetch(`https://${connectionUrl}${PAT_DISCOVERY_PATH}`, {
      headers: buildBearerHeaders(token),
      signal: controller.signal,
    });
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      throw mapDiscoveryError(response.status, body);
    }
    const payload = (await response.json()) as PatListResponse;
    return parseAccessibleProjects(payload);
  } finally {
    clearTimeout(timeoutId);
  }
}

/** Pulls the human-readable part out of a Keboola API error body */
function extractErrorDetail(body: string): string {
  if (!body) {
    return '';
  }
  try {
    const parsed = JSON.parse(body);
    return parsed.message || parsed.error || parsed.exception || '';
  } catch {
    return body.substring(0, 200);
  }
}
