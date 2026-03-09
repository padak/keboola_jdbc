import * as assert from 'assert';
import { getConnectionUrl, getQueryUrl, KEBOOLA_STACKS } from '../../constants';

suite('Constants - getConnectionUrl', () => {
  test('returns stack value for predefined stacks', () => {
    assert.strictEqual(
      getConnectionUrl('connection.keboola.com'),
      'connection.keboola.com'
    );
  });

  test('returns stack value for each predefined stack', () => {
    for (const stack of Object.keys(KEBOOLA_STACKS)) {
      assert.strictEqual(getConnectionUrl(stack), stack);
    }
  });

  test('returns customConnectionUrl when stack is "custom"', () => {
    assert.strictEqual(
      getConnectionUrl('custom', 'connection.mycompany.keboola.com'),
      'connection.mycompany.keboola.com'
    );
  });

  test('throws when stack is "custom" but no customConnectionUrl provided', () => {
    assert.throws(
      () => getConnectionUrl('custom'),
      /Custom Connection URL is required/
    );
  });

  test('throws when stack is "custom" and customConnectionUrl is empty string', () => {
    assert.throws(
      () => getConnectionUrl('custom', ''),
      /Custom Connection URL is required/
    );
  });
});

suite('Constants - getQueryUrl', () => {
  test('maps all predefined stacks correctly', () => {
    for (const [connectionUrl, queryUrl] of Object.entries(KEBOOLA_STACKS)) {
      assert.strictEqual(getQueryUrl(connectionUrl), queryUrl);
    }
  });

  test('converts custom connection.X to query.X', () => {
    assert.strictEqual(
      getQueryUrl('connection.custom.keboola.com'),
      'query.custom.keboola.com'
    );
  });

  test('throws for URLs not starting with "connection."', () => {
    assert.throws(
      () => getQueryUrl('invalid.keboola.com'),
      /Invalid connection URL/
    );
  });

  test('throws for empty string', () => {
    assert.throws(
      () => getQueryUrl(''),
      /Invalid connection URL/
    );
  });
});
