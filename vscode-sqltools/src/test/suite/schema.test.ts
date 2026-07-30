import * as assert from 'assert';
import * as path from 'path';
import * as fs from 'fs';
import Ajv from 'ajv';

suite('Connection Schema Validation', () => {
  let ajv: InstanceType<typeof Ajv>;
  let validate: ReturnType<InstanceType<typeof Ajv>['compile']>;

  suiteSetup(() => {
    const schemaPath = path.resolve(__dirname, '../../../connection.schema.json');
    const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf-8'));
    ajv = new Ajv({ allErrors: true, strict: false });
    validate = ajv.compile(schema);
  });

  // Regression test: predefined stack with empty customConnectionUrl must pass.
  // The bug: a pattern constraint on customConnectionUrl blocked the Save Connection
  // button when the field was left empty for predefined stacks.
  test('REGRESSION: predefined stack with empty customConnectionUrl should pass validation', () => {
    const data = {
      name: 'My Keboola',
      keboolaStack: 'connection.keboola.com',
      customConnectionUrl: '',
      token: 'my-token-123',
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema validation failed: ${JSON.stringify(validate.errors)}`);
  });

  test('predefined stack without customConnectionUrl field should pass', () => {
    const data = {
      name: 'My Keboola',
      keboolaStack: 'connection.keboola.com',
      token: 'my-token-123',
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema validation failed: ${JSON.stringify(validate.errors)}`);
  });

  test('valid payload with optional branchId and workspaceId should pass', () => {
    const data = {
      name: 'Full Connection',
      keboolaStack: 'connection.keboola.com',
      token: 'my-token-123',
      branchId: '12345',
      workspaceId: '67890',
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema validation failed: ${JSON.stringify(validate.errors)}`);
  });

  test('valid payload with optional fields omitted should pass', () => {
    const data = {
      name: 'Minimal Connection',
      keboolaStack: 'connection.keboola.com',
      token: 'my-token-123',
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema validation failed: ${JSON.stringify(validate.errors)}`);
  });

  test('custom stack with valid customConnectionUrl should pass', () => {
    const data = {
      name: 'Custom Keboola',
      keboolaStack: 'custom',
      customConnectionUrl: 'connection.mycompany.keboola.com',
      token: 'my-token-123',
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema validation failed: ${JSON.stringify(validate.errors)}`);
  });

  test('all predefined stacks should pass validation', () => {
    const stacks = [
      'connection.keboola.com',
      'connection.us-east4.gcp.keboola.com',
      'connection.eu-central-1.keboola.com',
      'connection.north-europe.azure.keboola.com',
      'connection.europe-west3.gcp.keboola.com',
    ];
    for (const stack of stacks) {
      const data = {
        name: 'Test',
        keboolaStack: stack,
        customConnectionUrl: '',
        token: 'token',
      };
      const valid = validate(data);
      assert.strictEqual(valid, true, `Stack ${stack} failed: ${JSON.stringify(validate.errors)}`);
    }
  });

  // Note: "name" is NOT in our schema's required array because SQLTools'
  // prepareSchema() adds it automatically. Having it in both causes duplicate
  // required items, which makes ajv throw "schema is invalid" and breaks
  // form validation (the Save Connection button silently fails).
  test('missing name should pass raw schema (SQLTools adds name to required via prepareSchema)', () => {
    const data = {
      keboolaStack: 'connection.keboola.com',
      token: 'token',
    };
    assert.strictEqual(validate(data), true);
  });

  test('REGRESSION: prepared schema (with SQLTools fields) should not have duplicate required items', () => {
    const schemaPath = path.resolve(__dirname, '../../../connection.schema.json');
    const schema = JSON.parse(fs.readFileSync(schemaPath, 'utf-8'));

    // Simulate SQLTools prepareSchema (from SQLTools module 7311)
    const preparedRequired = ['name', 'driver', ...(schema.required || [])];
    const uniqueRequired = [...new Set(preparedRequired)];

    assert.deepStrictEqual(
      preparedRequired,
      uniqueRequired,
      `Duplicate items in required array after prepareSchema: ${JSON.stringify(preparedRequired)}. ` +
      'Remove duplicates from connection.schema.json required array -- SQLTools adds "name" and "driver" automatically.'
    );
  });

  test('missing required field "token" should fail', () => {
    const data = {
      name: 'Test',
      keboolaStack: 'connection.keboola.com',
    };
    assert.strictEqual(validate(data), false);
  });

  test('missing required field "keboolaStack" should fail', () => {
    const data = {
      name: 'Test',
      token: 'token',
    };
    assert.strictEqual(validate(data), false);
  });

  test('invalid keboolaStack value should fail', () => {
    const data = {
      name: 'Test',
      keboolaStack: 'invalid-stack-value',
      token: 'token',
    };
    assert.strictEqual(validate(data), false);
  });

  test('empty name should fail (minLength: 1)', () => {
    const data = {
      name: '',
      keboolaStack: 'connection.keboola.com',
      token: 'token',
    };
    assert.strictEqual(validate(data), false);
  });

  test('branchId and workspaceId are NOT required', () => {
    const data = {
      name: 'Test',
      keboolaStack: 'connection.keboola.com',
      token: 'token',
      // branchId and workspaceId intentionally omitted
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema should not require branchId/workspaceId: ${JSON.stringify(validate.errors)}`);
  });

  test('Personal Access Token with projectId should pass', () => {
    const data = {
      name: 'PAT Connection',
      keboolaStack: 'connection.keboola.com',
      token: 'kbc_pat_placeholder-personal-access-token',
      projectId: '1234',
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema validation failed: ${JSON.stringify(validate.errors)}`);
  });

  test('projectId is NOT required', () => {
    const data = {
      name: 'Test',
      keboolaStack: 'connection.keboola.com',
      token: 'kbc_pat_placeholder-personal-access-token',
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema should not require projectId: ${JSON.stringify(validate.errors)}`);
  });

  // Project ids can exceed the 32-bit range, so they are carried as strings.
  test('numeric projectId should fail (must be a string)', () => {
    const data = {
      name: 'Test',
      keboolaStack: 'connection.keboola.com',
      token: 'token',
      projectId: 1234,
    };
    assert.strictEqual(validate(data), false);
  });

  test('projectId beyond the 32-bit range should pass as a string', () => {
    const data = {
      name: 'Test',
      keboolaStack: 'connection.keboola.com',
      token: 'kbc_pat_placeholder-personal-access-token',
      projectId: '9007199254740993',
    };
    const valid = validate(data);
    assert.strictEqual(valid, true, `Schema validation failed: ${JSON.stringify(validate.errors)}`);
  });
});

suite('UI Schema Consistency', () => {
  let connectionSchema: any;
  let uiSchema: any;

  suiteSetup(() => {
    const base = path.resolve(__dirname, '../../..');
    connectionSchema = JSON.parse(fs.readFileSync(path.join(base, 'connection.schema.json'), 'utf-8'));
    uiSchema = JSON.parse(fs.readFileSync(path.join(base, 'ui.schema.json'), 'utf-8'));
  });

  test('ui:order lists every connection schema property', () => {
    const properties = Object.keys(connectionSchema.properties);
    for (const property of properties) {
      assert.ok(
        uiSchema['ui:order'].includes(property),
        `Property "${property}" is missing from ui:order in ui.schema.json`
      );
    }
  });

  test('ui:order contains no unknown properties', () => {
    const properties = Object.keys(connectionSchema.properties);
    for (const ordered of uiSchema['ui:order']) {
      assert.ok(
        properties.includes(ordered),
        `ui:order references unknown property "${ordered}"`
      );
    }
  });

  test('token field stays masked', () => {
    assert.strictEqual(uiSchema.token['ui:widget'], 'password');
  });
});
