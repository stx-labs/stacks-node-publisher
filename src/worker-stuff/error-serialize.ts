/* eslint-disable @typescript-eslint/no-unsafe-member-access */
/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/no-unsafe-assignment */

const errorConstructors = new Map(
  [
    // Native ES errors https://262.ecma-international.org/12.0/#sec-well-known-intrinsic-objects
    Error,
    EvalError,
    RangeError,
    ReferenceError,
    SyntaxError,
    TypeError,
    URIError,
    AggregateError,

    // Built-in errors
    globalThis.DOMException,

    // Node-specific errors https://nodejs.org/api/errors.html
    (globalThis as any).AssertionError as Error,
    (globalThis as any).SystemError as Error,
  ]
    // Non-native Errors are used with `globalThis` because they might be missing. This filter drops them when undefined.
    .filter(Boolean)
    .map(constructor => [constructor.name, constructor as ErrorConstructor] as const)
);

export function addKnownErrorConstructor(
  constructor: new (message?: string, ..._arguments: unknown[]) => Error
) {
  try {
    new constructor();
  } catch (error) {
    throw new Error(`The error constructor "${constructor.name}" is not compatible`, {
      cause: error,
    });
  }

  errorConstructors.set(constructor.name, constructor as ErrorConstructor);
}

const commonProperties: {
  name: string;
  descriptor: Partial<PropertyDescriptor>;
  deserialize?: (_: any) => any;
  serialize?: (_: any) => any;
}[] = [
  {
    name: 'message',
    descriptor: {
      enumerable: false,
      configurable: true,
      writable: true,
    },
  },
  {
    name: 'stack',
    descriptor: {
      enumerable: false,
      configurable: true,
      writable: true,
    },
  },
  {
    name: 'code',
    descriptor: {
      enumerable: true,
      configurable: true,
      writable: true,
    },
  },
  {
    name: 'cause',
    descriptor: {
      enumerable: false,
      configurable: true,
      writable: true,
    },
  },
  {
    name: 'errors',
    descriptor: {
      enumerable: false,
      configurable: true,
      writable: true,
    },
    deserialize: (errors: SerializedError[]) => errors.map(error => deserializeError(error)),
    serialize: (errors: Error[]) => errors.map(error => serializeError(error)),
  },
];

export type SerializedError = {
  name: string;
  message: string;
  stack: string;
  [key: string]: any;
};

export function isErrorLike(value: unknown): value is Error & { stack: string } {
  return (
    typeof value === 'object' &&
    value !== null &&
    'name' in value &&
    'message' in value &&
    'stack' in value &&
    typeof (value as Error).name === 'string' &&
    typeof (value as Error).message === 'string' &&
    typeof (value as Error).stack === 'string'
  );
}

export function serializeError(subject: Error): SerializedError {
  if (!isErrorLike(subject)) {
    // If the subject is not an error, for example `throw "boom", then we throw.
    // This function should only be passed error objects, callers can use `isErrorLike`.
    throw new TypeError('Failed to serialize error, expected an error object');
  }

  const data: Record<string, any> = {
    name: 'Error',
    message: '',
    stack: '',
  };

  for (const prop of commonProperties) {
    if (!(prop.name in subject)) {
      continue;
    }
    let value = (subject as any)[prop.name];
    // TODO: if value instanceof Error then recursively serializeError
    if (prop.serialize) {
      value = prop.serialize(value);
    }
    data[prop.name] = value;
  }

  // Include any other enumerable own properties
  for (const key of Object.keys(subject)) {
    if (!(key in data)) {
      data[key] = (subject as any)[key];
    }
  }

  if (globalThis.DOMException && subject instanceof globalThis.DOMException) {
    data.name = 'DOMException';
  } else {
    data.name = subject.constructor.name;
  }
  return data as SerializedError;
}

export function deserializeError(subject: SerializedError): Error {
  if (!isErrorLike(subject)) {
    // If the subject is not an error, for example `throw "boom", then we throw.
    // This function should only be passed error objects, callers can use `isErrorLike`.
    throw new TypeError('Failed to desserialize error, expected an error object');
  }

  const con = errorConstructors.get(subject.name) ?? Error;
  const output = Object.create(con.prototype) as Error;

  for (const prop of commonProperties) {
    if (!(prop.name in subject)) continue;

    let value = (subject as any)[prop.name];
    // TODO: if value instanceof Error then recursively deserializeError
    if (prop.deserialize) value = prop.deserialize(value);

    Object.defineProperty(output, prop.name, {
      ...prop.descriptor,
      value: value,
    });
  }

  // Add any other properties (custom props not in commonProperties)
  for (const key of Object.keys(subject)) {
    if (!commonProperties.some(p => p.name === key)) {
      (output as any)[key] = (subject as any)[key];
      Object.assign(output, { [key]: (subject as any)[key] });
    }
  }

  return output;
}
