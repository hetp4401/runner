import {readFile,writeFile,unlink,mkdir} from 'node:fs/promises';
const reference=new URL('../../control/src/.rust-spec-reference.mjs',import.meta.url);
const source=await readFile(new URL('../../control/src/index.js',import.meta.url),'utf8');
await writeFile(reference,source+'\nexport {buildSpec,claimsOf,policyOf,remapCompose,fixedPorts,dupProblem};\n',{flag:'wx'});
try {
  const original=await import(reference.href);
  const {agentPlan}=await import(new URL('../../control/src/agent-plan.mjs',import.meta.url));
  const bases=[
    {compose:'services:\n  app:\n    image: nginx\n    ports: ["8080:80"]\n',port:8080},
    {dockerfile:'FROM busybox\nCMD ["sleep", "infinity"]\n',port:8081,replicas:3},
    {compose:'services:\n  app:\n    build: ./web\n',files:{'web/Dockerfile':'FROM busybox'}},
    {compose:'x-runner:\n  ready: /healthz\n  regions: [us-west, home-lab]\n  spread-tags: true\nservices:\n  app:\n    image: nginx\n'},
    {compose:'services:\n  app:\n    image: busybox\n    volumes: ["./:/app:ro", "./data:/data", "./config:/config:ro"]\n',files:{'data/value':'a','config':'b','A.txt':'UP','a.txt':'lo'}},
    {compose:'services:\n  app:\n    image: busybox\n    depends_on:\n      init:\n        condition: service_completed_successfully\n  init:\n    image: busybox\n  optional:\n    image: busybox\n    profiles: [optional]\n'},
  ];
  const input=[...bases,...[{},null,{files:[]},{compose:'services: {}'},{compose:[]},...['privileged: true','network_mode: host','volumes: ["/etc:/etc"]','ports: ["19080:80"]','env_file: ../private','extra_hosts: ["host:host-gateway"]'].map(extra=>({compose:`services:\n  app:\n    image: busybox\n    ${extra}\n`}))]];
  const fixtures=[];
  for(const body of input)for(const trusted of [false,true]){
    try{const spec=original.buildSpec(body,{trusted});const plan=agentPlan({app:'example',replica:2,v:3,...spec});fixtures.push({body,trusted,ok:true,spec,plan,claims:original.claimsOf(spec.compose),policy:original.policyOf(spec.compose),fixed:original.fixedPorts(spec.compose),nodup:original.dupProblem(spec.compose)});}
    catch(error){fixtures.push({body,trusted,ok:false,status:error.status,error:error.message});}
  }
  const path=new URL('../runner-control/tests/fixtures/spec.json',import.meta.url);await mkdir(new URL('.',path),{recursive:true});await writeFile(path,JSON.stringify(fixtures,null,2)+'\n');console.log(`${fixtures.length} original-controller specification fixtures`);
}finally{await unlink(reference);}
