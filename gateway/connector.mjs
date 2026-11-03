import {spawn} from 'node:child_process';
let child, stopping=false, failures=0;
const stop=()=>{stopping=true;child?.kill('SIGTERM');};
process.on('SIGTERM',stop);process.on('SIGINT',stop);
while(!stopping){
  let healthy=false;
  try{const r=await fetch('http://app:18278/healthz',{signal:AbortSignal.timeout(2000)});healthy=r.ok;}catch{}
  failures=healthy?0:failures+1;
  if(healthy&&!child){const current=spawn('/usr/local/bin/cloudflared',['tunnel','--no-autoupdate','--metrics','127.0.0.1:20241','run'],{stdio:'inherit'});child=current;current.once('error',()=>{if(child===current)child=null;});current.once('exit',()=>{if(child===current)child=null;});}
  if(failures>=2&&child)child.kill('SIGTERM');
  await new Promise(r=>setTimeout(r,3000));
}
