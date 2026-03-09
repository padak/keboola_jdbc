import * as vscode from 'vscode';
import { IExtension, IExtensionPlugin, IDriverExtensionApi } from '@sqltools/types';
import { DRIVER_ALIASES } from './constants';

const { publisher, name, displayName } = require('../package.json');

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
      // Register driver icons
      extension.resourcesMap().set(`driver/${DRIVER_ALIASES[0].value}/icons`, {
        active: extContext.asAbsolutePath('icons/active.png'),
        default: extContext.asAbsolutePath('icons/default.png'),
        inactive: extContext.asAbsolutePath('icons/inactive.png'),
      });

      // Register connection schema, UI schema, and extension ID for each alias
      DRIVER_ALIASES.forEach(({ value }) => {
        extension.resourcesMap().set(`driver/${value}/extension-id`, extensionId);
        extension
          .resourcesMap()
          .set(`driver/${value}/connection-schema`, extContext.asAbsolutePath('connection.schema.json'));
        extension
          .resourcesMap()
          .set(`driver/${value}/ui-schema`, extContext.asAbsolutePath('ui.schema.json'));
      });

      // Register the language server plugin (driver implementation)
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
    parseBeforeSaveConnection: ({ connInfo }) => connInfo,
    parseBeforeEditConnection: ({ connInfo }) => connInfo,
    driverAliases: DRIVER_ALIASES,
  };
}

export function deactivate() {}
