import Fastify from 'fastify';
import { Registry } from 'prom-client';
import { PINO_LOGGER_CONFIG } from '@stacks/api-toolkit';

export async function buildPromServer(args: { registry: Registry }) {
  const promServer = Fastify({
    trustProxy: true,
    logger: PINO_LOGGER_CONFIG,
  });

  promServer.route({
    url: '/metrics',
    method: 'GET',
    logLevel: 'info',
    handler: async (_, reply) => {
      const metrics = await args.registry.metrics();
      await reply.type(args.registry.contentType).send(metrics);
    },
  });

  return promServer;
}
