import { ILanguageServerPlugin } from '@sqltools/types';
import { DRIVER_ALIASES } from '../constants';
import KeboolaDriver from './driver';

/**
 * Language server plugin for Keboola.
 *
 * Registers the KeboolaDriver class with SQLTools for each configured alias
 * and sets up the custom query cancellation handler.
 */
const KeboolaPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      server.getContext().drivers.set(value, KeboolaDriver as any);
    });

    // Register custom request handler for query cancellation
    server.onRequest('keboola/cancelQuery', () => {
      return KeboolaDriver.cancelAllActiveQueries();
    });
  },
};

export default KeboolaPlugin;
