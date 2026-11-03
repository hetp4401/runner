import {readFileSync} from 'node:fs';
import {join} from 'node:path';

export const ROUTER_NAME = 'global-router';
export const ROUTER_REPLICAS = 3;

// Required platform workload, loaded from the same release as the controller.
// No external repository, app deployment or user routing setting is involved.
export function loadSystemRouter(root) {
  const files = Object.fromEntries(['Dockerfile','connector.mjs','router.mjs','server.mjs','streams.mjs']
    .map(name => [name,readFileSync(join(root,'gateway',name),'utf8')]));
  const compose = readFileSync(join(root,'gateway/compose.yaml'),'utf8');
  return {name:ROUTER_NAME, replicas:ROUTER_REPLICAS, port:18278, files, compose};
}
