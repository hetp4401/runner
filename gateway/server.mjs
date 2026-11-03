import {createRouter} from './router.mjs';
const server = createRouter();
server.listen(Number(process.env.PORT)||18278,'0.0.0.0',()=>console.log('Global app router ready'));
for (const signal of ['SIGTERM','SIGINT']) process.on(signal,()=>server.close(()=>process.exit(0)));
