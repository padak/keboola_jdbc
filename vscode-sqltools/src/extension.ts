import * as vscode from 'vscode';
import { IExtension, IExtensionPlugin, IDriverExtensionApi, IConnection } from '@sqltools/types';
import { DRIVER_ALIASES, getConnectionUrl, HTTP_TIMEOUT_MS } from './constants';
import {
  AuthContext,
  buildAuthHeaders,
  detectAuthMode,
  fetchAccessibleProjects,
  PAT_NO_PROJECTS_MESSAGE,
} from './auth';

const { publisher, name, displayName } = require('../package.json');

/**
 * Makes an authenticated request to the Keboola Storage API.
 * Used in the extension context (not the LS driver) for connection setup.
 */
async function storageApiGet<T = any>(connectionUrl: string, auth: AuthContext, path: string): Promise<T> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), HTTP_TIMEOUT_MS);
  try {
    const response = await fetch(`https://${connectionUrl}${path}`, {
      headers: buildAuthHeaders(auth),
      signal: controller.signal,
    });
    if (!response.ok) {
      const body = await response.text().catch(() => '');
      let errorDetail = '';
      try {
        const parsed = JSON.parse(body);
        errorDetail = parsed.message || parsed.error || parsed.exception || '';
      } catch {
        errorDetail = body.substring(0, 200);
      }
      throw new Error(
        `HTTP ${response.status}${errorDetail ? ': ' + errorDetail : ''} (${path})`
      );
    }
    return response.json() as Promise<T>;
  } finally {
    clearTimeout(timeoutId);
  }
}

/**
 * Shows QuickPick for branch selection if branchId is empty.
 * Returns the selected branchId or undefined if resolution failed.
 */
async function resolveBranchId(connectionUrl: string, auth: AuthContext): Promise<string | undefined> {
  const branches = await storageApiGet<any[]>(connectionUrl, auth, '/v2/storage/dev-branches');
  if (branches.length === 1) {
    return String(branches[0].id);
  }
  if (branches.length > 1) {
    const defaultBranch = branches.find((b: any) => b.isDefault);
    const items = branches.map((b: any) => ({
      label: b.name,
      description: `ID: ${b.id}${b.isDefault ? ' (default)' : ''}`,
      branchId: String(b.id),
    }));
    const picked = await vscode.window.showQuickPick(items, {
      title: 'Select Keboola Branch',
      placeHolder: defaultBranch ? `Default: ${defaultBranch.name}` : 'Select a branch',
    });
    return picked ? picked.branchId : String(defaultBranch?.id || branches[0].id);
  }
  return undefined;
}

/**
 * Shows QuickPick for workspace selection if workspaceId is empty.
 * Returns the selected workspaceId or undefined if resolution failed.
 */
async function resolveWorkspaceId(connectionUrl: string, auth: AuthContext): Promise<string | undefined> {
  const workspaces = await storageApiGet<any[]>(connectionUrl, auth, '/v2/storage/workspaces');
  if (workspaces.length === 0) {
    throw new Error('No workspaces found. Create a workspace in Keboola first.');
  }
  if (workspaces.length === 1) {
    return String(workspaces[0].id);
  }
  const items = workspaces.map((w: any) => ({
    label: w.name || `Workspace ${w.id}`,
    description: `ID: ${w.id} | Type: ${w.type || 'snowflake'}`,
    workspaceId: String(w.id),
  }));
  const picked = await vscode.window.showQuickPick(items, {
    title: 'Select Keboola Workspace',
    placeHolder: 'Select a workspace to connect to',
  });
  return picked ? picked.workspaceId : String(workspaces[workspaces.length - 1].id);
}

/**
 * Shows QuickPick for project selection when a Personal Access Token reaches
 * more than one project. Returns the selected projectId, or undefined when the
 * user dismissed the picker.
 */
async function resolveProjectId(connectionUrl: string, token: string): Promise<string | undefined> {
  const projects = await fetchAccessibleProjects(connectionUrl, token);
  if (projects.length === 0) {
    throw new Error(PAT_NO_PROJECTS_MESSAGE);
  }
  if (projects.length === 1) {
    return projects[0].id;
  }
  const items = projects.map((p) => ({
    label: p.name,
    description: `ID: ${p.id}`,
    projectId: p.id,
  }));
  const picked = await vscode.window.showQuickPick(items, {
    title: 'Select Keboola Project',
    placeHolder: 'Select the project to connect to',
  });
  return picked ? picked.projectId : undefined;
}

/**
 * Fills in projectId, branchId and workspaceId via QuickPick if they are empty.
 * Used by both parseBeforeSaveConnection and resolveConnection.
 */
async function fillConnectionFields(connInfo: any): Promise<void> {
  const connectionUrl = getConnectionUrl(connInfo.keboolaStack, connInfo.customConnectionUrl);
  const isPat = detectAuthMode(connInfo.token) === 'pat';

  if (isPat && !connInfo.projectId) {
    try {
      const projectId = await resolveProjectId(connectionUrl, connInfo.token);
      if (projectId) {
        connInfo.projectId = projectId;
      }
    } catch (err: any) {
      vscode.window.showErrorMessage(`Keboola: ${err.message}`);
    }
  }

  // A Personal Access Token spans projects, so no Storage API call can be made
  // before one is chosen.
  if (isPat && !connInfo.projectId) {
    return;
  }

  const auth: AuthContext = { token: connInfo.token, projectId: connInfo.projectId };

  if (!connInfo.branchId) {
    try {
      const branchId = await resolveBranchId(connectionUrl, auth);
      if (branchId) {
        connInfo.branchId = branchId;
      }
    } catch (err: any) {
      vscode.window.showWarningMessage(
        `Keboola: Failed to list branches: ${err.message}`
      );
    }
  }

  if (!connInfo.workspaceId) {
    try {
      const workspaceId = await resolveWorkspaceId(connectionUrl, auth);
      if (workspaceId) {
        connInfo.workspaceId = workspaceId;
      }
    } catch (err: any) {
      vscode.window.showWarningMessage(
        `Keboola: Failed to list workspaces: ${err.message}`
      );
    }
  }
}

export async function activate(extContext: vscode.ExtensionContext): Promise<IDriverExtensionApi> {
  const sqltools = vscode.extensions.getExtension<IExtension>('mtxr.sqltools');
  if (!sqltools) {
    throw new Error('SQLTools extension is not installed. Please install mtxr.sqltools first.');
  }
  await sqltools.activate();

  const api = sqltools.exports;
  const extensionId = `${publisher}.${name}`;

  const plugin: IExtensionPlugin = {
    extensionId,
    name: `${displayName} Plugin`,
    type: 'driver',
    async register(extension) {
      extension.resourcesMap().set(`driver/${DRIVER_ALIASES[0].value}/icons`, {
        active: extContext.asAbsolutePath('icons/active.png'),
        default: extContext.asAbsolutePath('icons/default.png'),
        inactive: extContext.asAbsolutePath('icons/inactive.png'),
      });

      DRIVER_ALIASES.forEach(({ value }) => {
        extension.resourcesMap().set(`driver/${value}/extension-id`, extensionId);
        extension
          .resourcesMap()
          .set(`driver/${value}/connection-schema`, extContext.asAbsolutePath('connection.schema.json'));
        extension
          .resourcesMap()
          .set(`driver/${value}/ui-schema`, extContext.asAbsolutePath('ui.schema.json'));
      });

      await extension.client.sendRequest('ls/RegisterPlugin', {
        path: extContext.asAbsolutePath('out/ls/plugin.js'),
      });
    },
  };

  api.registerPlugin(plugin);

  // Register cancel query command
  const cancelCmd = vscode.commands.registerCommand('keboola.cancelQuery', async () => {
    try {
      const result: any = await api.client.sendRequest('keboola/cancelQuery');
      if (result?.cancelled?.length > 0) {
        vscode.window.showInformationMessage(
          `Cancelled ${result.cancelled.length} running query job(s).`
        );
      } else {
        vscode.window.showInformationMessage('No running queries to cancel.');
      }
    } catch (err: any) {
      vscode.window.showErrorMessage(`Failed to cancel query: ${err.message}`);
    }
  });
  extContext.subscriptions.push(cancelCmd);

  return {
    driverName: displayName,
    parseBeforeEditConnection: ({ connInfo }) => connInfo,

    // Called when user clicks "Save Connection" — returned value is persisted to settings.
    // This is the right place for QuickPick: values get saved so the user is not asked again.
    parseBeforeSaveConnection: async ({ connInfo }) => {
      await fillConnectionFields(connInfo as any);
      return connInfo;
    },

    // Called on every connect/test. If values are already saved, this is a no-op.
    // If not (e.g. Test Connection before Save), fills in-memory values for this session.
    resolveConnection: async ({ connInfo }) => {
      await fillConnectionFields(connInfo as any);
      return connInfo;
    },

    driverAliases: DRIVER_ALIASES,
  };
}

export function deactivate() {}
