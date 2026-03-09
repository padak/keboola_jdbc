import { ILanguageServerPlugin } from '@sqltools/types';
import { DRIVER_ALIASES } from '../constants';

/**
 * Language server plugin for Keboola.
 *
 * In Phase 1 (scaffolding), the driver class is not yet implemented.
 * The plugin registers a placeholder and the cancel query handler.
 * Phase 2 will add the actual KeboolaDriver import and registration.
 */
const KeboolaPlugin: ILanguageServerPlugin = {
  register(server) {
    DRIVER_ALIASES.forEach(({ value }) => {
      // Driver class will be registered here in Phase 2
      // server.getContext().drivers.set(value, KeboolaDriver as any);
    });

    // Register custom request handler for query cancellation
    server.onRequest('keboola/cancelQuery', () => {
      // Will delegate to KeboolaDriver.cancelAllActiveQueries() in Phase 2
      return { cancelled: [] };
    });
  },
};

export default KeboolaPlugin;
